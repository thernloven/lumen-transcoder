import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";
import { query, queryOne, pool } from "../db";
import { downloadFromR2, uploadFileToR2, deleteFromR2, fileExistsInR2 } from "../services/r2";


const POLL_INTERVAL = 5000;
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "2", 10);
const IDLE_TIMEOUT = parseInt(process.env.IDLE_TIMEOUT || "120000", 10); // 2 min default
const IS_DROPLET = process.env.TRANSCODER_MODE === "droplet";
const WORK_DIR = path.join(process.env.TRANSCODE_DIR || os.tmpdir(), "aperture-transcode");

if (!fs.existsSync(WORK_DIR)) {
  fs.mkdirSync(WORK_DIR, { recursive: true });
}

interface TranscodeJob {
  id: string;
  type: "movie" | "episode";
  content_id: string;
  title: string;
  original_key: string;
  season_number?: number;
  episode_number?: number;
}

interface SubtitleTrack {
  index: number;
  language: string;
}

async function findNextJob(activeJobIds: Set<string>): Promise<TranscodeJob | null> {
  // Build exclusion list so concurrent workers don't grab the same job
  const excludeIds = Array.from(activeJobIds);

  const movie = await queryOne<{ id: string; title: string }>(
    `SELECT id, title FROM content
     WHERE status = 'transcoding' AND type != 'series'
     ${excludeIds.length > 0 ? `AND id != ALL($1)` : ""}
     ORDER BY created_at ASC LIMIT 1`,
    excludeIds.length > 0 ? [excludeIds] : []
  );

  if (movie) {
    const extensions = ["mkv", "mp4", "avi", "mov", "webm"];
    for (const ext of extensions) {
      const key = `content/${movie.id}/original.${ext}`;
      if (await fileExistsInR2(key)) {
        return { id: movie.id, type: "movie", content_id: movie.id, title: movie.title, original_key: key };
      }
    }
    await query(`UPDATE content SET status = 'error' WHERE id = $1`, [movie.id]);
    console.error(`No original file found for "${movie.title}"`);
    return null;
  }

  const episode = await queryOne<{
    id: string; content_id: string; season_number: number;
    episode_number: number; title: string;
  }>(
    `SELECT se.id, se.content_id, se.season_number, se.episode_number, se.title
     FROM series_episodes se WHERE se.status = 'transcoding'
     ${excludeIds.length > 0 ? `AND se.id != ALL($1)` : ""}
     ORDER BY se.created_at ASC LIMIT 1`,
    excludeIds.length > 0 ? [excludeIds] : []
  );

  if (episode) {
    const s = String(episode.season_number).padStart(2, "0");
    const e = String(episode.episode_number).padStart(2, "0");
    const extensions = ["mkv", "mp4", "avi", "mov", "webm"];
    for (const ext of extensions) {
      const key = `content/${episode.content_id}/s${s}e${e}/original.${ext}`;
      if (await fileExistsInR2(key)) {
        return {
          id: episode.id, type: "episode", content_id: episode.content_id,
          title: `S${s}E${e} - ${episode.title}`, original_key: key,
          season_number: episode.season_number, episode_number: episode.episode_number,
        };
      }
    }
    await query(`UPDATE series_episodes SET status = 'error' WHERE id = $1`, [episode.id]);
    return null;
  }

  return null;
}

function runFFmpeg(args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", args, { stdio: ["ignore", "inherit", "pipe"] });
    let stderr = "";
    proc.stderr?.on("data", (data) => { stderr += data.toString(); });
    proc.on("close", (code, signal) => {
      if (code === 0) return resolve();
      const lastLines = stderr.split("\n").filter(l => l.trim()).slice(-10).join("\n");
      reject(new Error(`FFmpeg failed (code=${code}, signal=${signal}):\n${lastLines}`));
    });
    proc.on("error", reject);
  });
}

// Probe file for subtitle tracks using ffprobe
function probeSubtitles(inputUrl: string): Promise<SubtitleTrack[]> {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffprobe", [
      "-v", "quiet",
      "-print_format", "json",
      "-show_streams",
      "-select_streams", "s",
      inputUrl,
    ]);

    let output = "";
    proc.stdout.on("data", (data) => { output += data.toString(); });
    proc.stderr.on("data", () => {}); // suppress

    proc.on("close", (code) => {
      if (code !== 0) {
        resolve([]); // no subs is fine
        return;
      }

      try {
        const data = JSON.parse(output);
        const tracks: SubtitleTrack[] = (data.streams || []).map((s: any, i: number) => ({
          index: i,
          language: s.tags?.language || `und${i}`,
        }));
        resolve(tracks);
      } catch {
        resolve([]);
      }
    });

    proc.on("error", () => resolve([]));
  });
}

function hasAudioStream(inputPath: string): Promise<boolean> {
  return new Promise((resolve) => {
    const proc = spawn("ffprobe", [
      "-v", "quiet",
      "-select_streams", "a",
      "-show_entries", "stream=codec_name",
      "-print_format", "json",
      inputPath,
    ]);

    let output = "";
    proc.stdout.on("data", (data) => { output += data.toString(); });
    proc.stderr.on("data", () => {});

    proc.on("close", () => {
      try {
        const data = JSON.parse(output);
        resolve((data.streams || []).length > 0);
      } catch {
        resolve(false);
      }
    });

    proc.on("error", () => resolve(false));
  });
}

// Probe video codec, pixel format, and color metadata
function probeVideoCodec(inputPath: string): Promise<{
  codec: string | null; pixFmt: string | null;
  colorSpace: string | null; colorPrimaries: string | null;
  colorTrc: string | null; colorRange: string | null;
}> {
  return new Promise((resolve) => {
    const proc = spawn("ffprobe", [
      "-v", "quiet",
      "-select_streams", "v:0",
      "-show_entries", "stream=codec_name,pix_fmt,color_space,color_primaries,color_transfer,color_range",
      "-print_format", "json",
      inputPath,
    ]);

    let output = "";
    proc.stdout.on("data", (data) => { output += data.toString(); });
    proc.stderr.on("data", () => {});

    proc.on("close", () => {
      try {
        const data = JSON.parse(output);
        const stream = data.streams?.[0];
        resolve({
          codec: stream?.codec_name || null,
          pixFmt: stream?.pix_fmt || null,
          colorSpace: stream?.color_space || null,
          colorPrimaries: stream?.color_primaries || null,
          colorTrc: stream?.color_transfer || null,
          colorRange: stream?.color_range || null,
        });
      } catch {
        resolve({ codec: null, pixFmt: null, colorSpace: null, colorPrimaries: null, colorTrc: null, colorRange: null });
      }
    });

    proc.on("error", () => resolve({ codec: null, pixFmt: null, colorSpace: null, colorPrimaries: null, colorTrc: null, colorRange: null }));
  });
}

function srtToVtt(srt: string): string {
  let vtt = "WEBVTT\n\n";
  vtt += srt
    .replace(/\r\n/g, "\n")
    .replace(/(\d{2}:\d{2}:\d{2}),(\d{3})/g, "$1.$2")
    .trim();
  return vtt;
}

async function extractSubtitles(inputUrl: string, jobDir: string, basePath: string, contentId: string) {
  const tracks = await probeSubtitles(inputUrl);

  if (tracks.length === 0) {
    console.log(`[SUBS] No embedded subtitles found`);
    return;
  }

  console.log(`[SUBS] Found ${tracks.length} subtitle tracks: ${tracks.map(t => t.language).join(", ")}`);

  for (const track of tracks) {
    try {
      const srtPath = path.join(jobDir, `${track.language}.srt`);

      // Extract subtitle track as SRT
      await runFFmpeg([
        "-i", inputUrl,
        "-map", `0:s:${track.index}`,
        "-c:s", "srt",
        "-y",
        srtPath,
      ]);

      // Convert SRT to VTT
      const srtContent = fs.readFileSync(srtPath, "utf-8");
      const vttContent = srtToVtt(srtContent);
      const vttPath = path.join(jobDir, `${track.language}.vtt`);
      fs.writeFileSync(vttPath, vttContent, "utf-8");

      // Upload to R2
      const r2Key = `${basePath}/subtitles/${track.language}.vtt`;
      await uploadFileToR2(vttPath, r2Key, "text/vtt");

      // Save to DB
      await query(
        `INSERT INTO subtitles (content_id, language, vtt_key) VALUES ($1, $2, $3)
         ON CONFLICT (content_id, language, vtt_key) DO NOTHING`,
        [contentId, track.language, r2Key]
      );

      console.log(`[SUBS] Extracted: ${track.language}`);
    } catch (err) {
      console.error(`[SUBS] Failed to extract ${track.language}:`, err);
    }
  }
}

async function processJob(job: TranscodeJob) {
  const jobDir = path.join(WORK_DIR, job.id);
  fs.mkdirSync(jobDir, { recursive: true });

  const basePath = job.original_key.replace(/\/original\.[^.]+$/, "");
  const ext = job.original_key.split(".").pop() || "mkv";
  const inputPath = path.join(jobDir, `original.${ext}`);

  try {
    // Download from R2 to local disk first (avoids FFmpeg SIGSEGV on HTTPS URLs)
    console.log(`[DOWNLOAD] Downloading: "${job.title}"`);
    await downloadFromR2(job.original_key, inputPath);
    console.log(`[DOWNLOAD] Done (${(fs.statSync(inputPath).size / 1024 / 1024).toFixed(0)} MB)`);

    const mp4Key = `${basePath}/stream.mp4`;
    const mp4Path = path.join(jobDir, "stream.mp4");

    // Check if video can be stream-copied (H.264 only)
    const { codec: videoCodec, pixFmt, colorSpace, colorPrimaries, colorTrc, colorRange } = await probeVideoCodec(inputPath);
    const hasAudio = await hasAudioStream(inputPath);
    const canCopyVideo = videoCodec === "h264";

    if (canCopyVideo) {
      console.log(`[REMUX] Copying video (${videoCodec}, ${pixFmt})${hasAudio ? ", re-encoding audio" : ", no audio"}: "${job.title}"`);
    } else {
      console.log(`[TRANSCODE] Re-encoding video (${videoCodec || "unknown"}, ${pixFmt} → H.264, color: ${colorSpace}/${colorRange})${hasAudio ? "" : ", no audio"}: "${job.title}"`);
    }

    const colorArgs: string[] = [];
    if (!canCopyVideo) {
      colorArgs.push("-colorspace", colorSpace || "bt709");
      colorArgs.push("-color_primaries", colorPrimaries || "bt709");
      colorArgs.push("-color_trc", colorTrc || "bt709");
      colorArgs.push("-color_range", colorRange || "tv");
    }

    const videoArgs = canCopyVideo
      ? ["-c:v", "copy"]
      : ["-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-profile:v", "high", "-level", "4.1", "-pix_fmt", "yuv420p",
        ...colorArgs];

    const audioArgs = hasAudio
      ? ["-map", "0:a:0", "-c:a", "aac", "-ac", "2", "-b:a", "192k"]
      : [];

    await runFFmpeg([
      "-i", inputPath,
      "-map", "0:v:0",
      ...audioArgs,
      ...videoArgs,
      "-movflags", "+faststart",
      "-y",
      mp4Path,
    ]);

    // Extract embedded subtitles
    await extractSubtitles(inputPath, jobDir, basePath, job.content_id);

    // Upload processed MP4
    console.log(`[UPLOAD] Uploading MP4: "${job.title}"`);
    await uploadFileToR2(mp4Path, mp4Key, "video/mp4");

    // Delete original from R2
    console.log(`[CLEANUP] Deleting original: ${job.original_key}`);
    await deleteFromR2(job.original_key).catch(() => {});

    // Update status to ready
    if (job.type === "movie") {
      await query(`UPDATE content SET status = 'ready', hls_key = $1 WHERE id = $2`, [mp4Key, job.content_id]);
    } else {
      await query(
        `UPDATE series_episodes SET status = 'ready', hls_key = $1
         WHERE content_id = $2 AND season_number = $3 AND episode_number = $4`,
        [mp4Key, job.content_id, job.season_number, job.episode_number]
      );
    }

    console.log(`[DONE] "${job.title}" — ready to watch`);
  } finally {
    fs.rmSync(jobDir, { recursive: true, force: true });
  }
}

async function selfDestruct() {
  console.log("[DO] Queue empty, self-destructing droplet...");
  try {
    // Get our own droplet ID from metadata
    const metaRes = await fetch("http://169.254.169.254/metadata/v1/id");
    const dropletId = (await metaRes.text()).trim();
    console.log(`[DO] Droplet ID: ${dropletId}`);

    const delRes = await fetch(`https://api.digitalocean.com/v2/droplets?tag_name=aperture-transcoder`, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${process.env.DO_API_TOKEN}` },
    });

    if (delRes.status === 204) {
      console.log("[DO] Self-destruct successful");
    } else {
      const body = await delRes.text();
      console.error(`[DO] Self-destruct failed (${delRes.status}): ${body}`);
    }
  } catch (err) {
    console.error("[DO] Self-destruct failed:", err);
  }
  process.exit(0);
}

async function main() {
  const mode = IS_DROPLET ? "droplet (self-destruct enabled)" : "persistent";
  console.log(`Aperture Transcoder running... (mode: ${mode}, concurrency: ${MAX_CONCURRENT})`);

  // Clean up any leftover temp files on startup
  try { fs.rmSync(WORK_DIR, { recursive: true, force: true }); fs.mkdirSync(WORK_DIR, { recursive: true }); } catch {}

  const activeJobs = new Map<string, { done: boolean }>();
  let idleSince: number | null = null;

  while (true) {
    try {
      // Clean up completed jobs
      for (const [id, state] of activeJobs) {
        if (state.done) activeJobs.delete(id);
      }

      // Fill up to MAX_CONCURRENT slots
      while (activeJobs.size < MAX_CONCURRENT) {
        const activeIds = new Set(activeJobs.keys());
        const job = await findNextJob(activeIds);

        if (!job) break;

        idleSince = null;
        console.log(`\nProcessing: "${job.title}" (${job.type}) [${activeJobs.size + 1}/${MAX_CONCURRENT}]`);

        const state = { done: false };
        processJob(job).catch(async (jobErr) => {
          console.error(`[ERROR] Job failed for "${job.title}":`, jobErr);
          if (job.type === "movie") {
            await query(`UPDATE content SET status = 'error' WHERE id = $1`, [job.content_id]).catch(() => {});
          } else {
            await query(
              `UPDATE series_episodes SET status = 'error' WHERE content_id = $1 AND season_number = $2 AND episode_number = $3`,
              [job.content_id, job.season_number, job.episode_number]
            ).catch(() => {});
          }
        }).finally(() => { state.done = true; });

        activeJobs.set(job.id, state);
      }

      // If no active jobs, track idle time
      if (activeJobs.size === 0) {
        if (idleSince === null) {
          idleSince = Date.now();
        } else if (IS_DROPLET && Date.now() - idleSince >= IDLE_TIMEOUT) {
          await pool.end();
          await selfDestruct();
        }
      } else {
        idleSince = null;
      }

      await new Promise((r) => setTimeout(r, POLL_INTERVAL));
    } catch (err) {
      console.error("Transcoder error:", err);
      await new Promise((r) => setTimeout(r, POLL_INTERVAL));
    }
  }
}

main().catch(async (err) => {
  console.error("Fatal transcoder error:", err);
  if (IS_DROPLET) await selfDestruct();
  process.exit(1);
});
