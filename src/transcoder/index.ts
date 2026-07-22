import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import os from "os";
import { query, queryOne, pool } from "../db";
import { downloadFromR2, uploadFileToR2, deleteFromR2, fileExistsInR2 } from "../services/r2";
import { computePhash, probeResolutionHeight, compareHashes, FrameHash } from "./phash";


const POLL_INTERVAL = 5000;
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "2", 10);
const IDLE_TIMEOUT = parseInt(process.env.IDLE_TIMEOUT || "900000", 10); // 15 min default
const IS_DROPLET = process.env.TRANSCODER_MODE === "droplet";
const TRANSCODE_POOL = process.env.TRANSCODE_POOL || ""; // 'free' or 'paid'
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
  const excludeIds = Array.from(activeJobIds);

  // Build pool filter — if TRANSCODE_POOL is set, only pick jobs for this pool
  const poolFilter = TRANSCODE_POOL ? `AND transcode_pool = '${TRANSCODE_POOL}'` : "";

  // Atomically claim jobs by setting status to 'processing' via UPDATE ... RETURNING
  // This prevents multiple droplets from grabbing the same job
  // Keep 'upgrading' status so content stays playable during re-encode
  const movie = await queryOne<{ id: string; title: string }>(
    `UPDATE content SET status = CASE WHEN status = 'upgrading' THEN 'upgrading' ELSE 'processing' END, status_updated_at = NOW()
     WHERE id = (
       SELECT id FROM content
       WHERE status IN ('transcoding', 'upgrading') AND type != 'series'
       ${poolFilter}
       ${excludeIds.length > 0 ? `AND id != ALL($1)` : ""}
       ORDER BY transcode_priority DESC, status_updated_at ASC LIMIT 1
       FOR UPDATE SKIP LOCKED
     ) RETURNING id, title`,
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
    `UPDATE series_episodes SET status = CASE WHEN status = 'upgrading' THEN 'upgrading' ELSE 'processing' END, status_updated_at = NOW()
     WHERE id = (
       SELECT se.id FROM series_episodes se
       WHERE se.status IN ('transcoding', 'upgrading')
       ${poolFilter}
       ${excludeIds.length > 0 ? `AND se.id != ALL($1)` : ""}
       ORDER BY se.transcode_priority DESC, se.status_updated_at ASC LIMIT 1
       FOR UPDATE SKIP LOCKED
     ) RETURNING id, content_id, season_number, episode_number, title`,
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

    // Quality gate. When replacing an existing version, only overwrite it if the new
    // file is the SAME content (pHash pixel match) AND higher resolution — mirrors the
    // desktop-upload gate (upload.ts) so torrent packs can't blindly redo/downgrade an
    // episode you already have, and a mis-matched torrent can't clobber the wrong slot.
    const newResolution = await probeResolutionHeight(inputPath);
    const existing = job.type === "movie"
      ? await queryOne<{ resolution_height: number | null; phashes: any; phash_verified: boolean }>(
          `SELECT resolution_height, phashes, phash_verified FROM content WHERE id = $1`, [job.content_id])
      : await queryOne<{ resolution_height: number | null; phashes: any; phash_verified: boolean }>(
          `SELECT resolution_height, phashes, phash_verified FROM series_episodes WHERE content_id = $1 AND season_number = $2 AND episode_number = $3`,
          [job.content_id, job.season_number, job.episode_number]
        );

    // Same as upload.ts: <32 avg hamming = same content, higher res = worth replacing.
    const SAME_CONTENT_MAX_DISTANCE = 32;

    const keepExisting = async (reason: string) => {
      console.log(`[QUALITY] "${job.title}" — keeping existing (${reason})`);
      await deleteFromR2(job.original_key).catch(() => {});
      if (job.type === "movie") {
        await query(`UPDATE content SET status = 'ready', status_updated_at = NOW() WHERE id = $1`, [job.content_id]);
      } else {
        await query(
          `UPDATE series_episodes SET status = 'ready', status_updated_at = NOW() WHERE content_id = $1 AND season_number = $2 AND episode_number = $3`,
          [job.content_id, job.season_number, job.episode_number]
        );
      }
    };

    if (existing?.phash_verified && existing.phashes && existing.resolution_height) {
      // There's an existing verified version to protect — run the pixel + resolution gate.
      const storedHashes: FrameHash[] = typeof existing.phashes === "string" ? JSON.parse(existing.phashes) : existing.phashes;
      const newPhash = await computePhash(inputPath, jobDir).catch((err) => {
        console.error(`[QUALITY] pHash failed for "${job.title}", falling back to resolution-only:`, err);
        return null;
      });

      if (newPhash) {
        const distance = compareHashes(newPhash.hashes, storedHashes);
        const sameContent = distance < SAME_CONTENT_MAX_DISTANCE;
        const higherRes = !!newResolution && newResolution > existing.resolution_height;
        console.log(`[QUALITY] "${job.title}" — pHash distance ${distance.toFixed(1)}, new ${newResolution}p vs existing ${existing.resolution_height}p`);

        if (!sameContent) { await keepExisting(`different content, pHash distance ${distance.toFixed(1)} ≥ ${SAME_CONTENT_MAX_DISTANCE}`); return; }
        if (!higherRes)   { await keepExisting(`same content but not higher resolution (${newResolution}p ≤ ${existing.resolution_height}p)`); return; }
        console.log(`[QUALITY] "${job.title}" — same content, higher quality (${newResolution}p > ${existing.resolution_height}p) — replacing`);
      } else if (newResolution && newResolution < existing.resolution_height) {
        // pHash unavailable — fall back to resolution-only
        await keepExisting(`new ${newResolution}p < existing ${existing.resolution_height}p (pHash unavailable)`); return;
      }
    } else if (existing?.resolution_height && newResolution && newResolution < existing.resolution_height) {
      // Older content without a verified pHash — resolution-only gate (prior behavior).
      await keepExisting(`new ${newResolution}p < existing ${existing.resolution_height}p`); return;
    }

    console.log(`[QUALITY] "${job.title}" — ${newResolution}p${existing?.resolution_height ? ` (existing: ${existing.resolution_height}p)` : ""} — proceeding`);

    const mp4Key = `${basePath}/stream.mp4`;
    const mp4Path = path.join(jobDir, "stream.mp4");

    // Probe video bitrate
    async function probeVideoBitrate(filePath: string): Promise<number | null> {
      // MKV sources frequently omit the per-stream bit_rate tag that MP4/remuxed files
      // carry (Matroska just doesn't always declare it) — querying stream=bit_rate alone
      // silently came back null for Dunkirk's source, which fed straight into the
      // copy-eligibility check's "unknown → assume safe" fallback and skipped the bitrate
      // gate entirely. Fall back to the container-level format.bit_rate, and if that's
      // ALSO missing, estimate from file size / duration (includes audio, so this slightly
      // overestimates video bitrate — the safe direction, since it only makes a file MORE
      // likely to get re-encoded, never less).
      return new Promise((resolve) => {
        const proc = spawn("ffprobe", [
          "-v", "quiet",
          "-select_streams", "v:0",
          "-show_entries", "stream=bit_rate",
          "-show_entries", "format=bit_rate,duration,size",
          "-print_format", "json",
          filePath,
        ]);
        let output = "";
        proc.stdout.on("data", (data) => { output += data.toString(); });
        proc.stderr.on("data", () => {});
        proc.on("close", () => {
          try {
            const data = JSON.parse(output);
            const streamBr = parseInt(data.streams?.[0]?.bit_rate);
            if (!isNaN(streamBr)) { resolve(Math.round(streamBr / 1_000_000 * 10) / 10); return; }

            const formatBr = parseInt(data.format?.bit_rate);
            if (!isNaN(formatBr)) { resolve(Math.round(formatBr / 1_000_000 * 10) / 10); return; }

            const size = parseInt(data.format?.size);
            const duration = parseFloat(data.format?.duration);
            if (!isNaN(size) && !isNaN(duration) && duration > 0) {
              resolve(Math.round((size * 8 / duration) / 1_000_000 * 10) / 10);
              return;
            }
            resolve(null);
          } catch { resolve(null); }
        });
        proc.on("error", () => resolve(null));
      });
    }

    // Check if video can be stream-copied (H.264 + comfortably under the bitrate cap).
    // COPY_ELIGIBLE_MAX_MBPS is deliberately well below MAX_BITRATE_MBPS: a copy preserves
    // the source's bitrate as-is with NO cap on local/peak bitrate, only the file's
    // declared AVERAGE gets checked here. A VBR encode can look safe on average while still
    // spiking hard in busy scenes (explosions, fast motion) — exactly what caused Dunkirk to
    // lag despite averaging 10.2 Mbps: it slipped under the old 17 Mbps copy gate and its
    // scene-level peaks went out uncapped. Anything above this lower gate now re-encodes,
    // which DOES enforce a hard peak cap via -maxrate/-bufsize below.
    // Bufsize == maxrate (a 1s VBV window, was 2x/2s) so the encoder can't sustain a high
    // rate for long — measured on Dunkirk: with the old 17M/34M (2s window) it held 10-39
    // CONSECUTIVE seconds at 13-17+ Mbps (peaking 27.92 Mbps in a single second) during its
    // long action sequences, while comparison titles (Backrooms, Obsession) only ever spiked
    // for 1-4s, which a 2s buffer smooths out fine. A 10-40s sustained near-ceiling demand is
    // what a plain <video> tag with no adaptive bitrate can't reliably sustain over a real
    // connection — a tighter window forces the encoder to average down much sooner instead of
    // "borrowing" against the buffer for that long. Ceiling also lowered (17 → 12) since 17
    // Mbps sustained for any real duration is already a lot to ask without ABR fallback.
    const MAX_BITRATE_MBPS = 12;
    const COPY_ELIGIBLE_MAX_MBPS = 8;
    const { codec: videoCodec, pixFmt, colorSpace, colorPrimaries, colorTrc, colorRange } = await probeVideoCodec(inputPath);
    const hasAudio = await hasAudioStream(inputPath);
    const bitrateMbps = await probeVideoBitrate(inputPath);
    // Unknown bitrate must fail SAFE (re-encode), not fail open (copy unchecked). This is
    // exactly how Dunkirk got through a second time even after lowering the copy gate: its
    // MKV source had no stream-level bit_rate tag, probeVideoBitrate returned null, and
    // "null → treat as safe to copy" skipped the gate entirely regardless of the threshold.
    const canCopyVideo = videoCodec === "h264" && bitrateMbps !== null && bitrateMbps <= COPY_ELIGIBLE_MAX_MBPS;

    if (canCopyVideo) {
      console.log(`[REMUX] Copying video (${videoCodec}, ${pixFmt}, ${bitrateMbps ? bitrateMbps + " Mbps" : "unknown bitrate"})${hasAudio ? ", re-encoding audio" : ", no audio"}: "${job.title}"`);
    } else {
      console.log(`[TRANSCODE] Re-encoding video (${videoCodec || "unknown"}, ${pixFmt}, ${bitrateMbps ? bitrateMbps + " Mbps" : "unknown bitrate"} → H.264 capped at ${MAX_BITRATE_MBPS} Mbps, color: ${colorSpace}/${colorRange})${hasAudio ? "" : ", no audio"}: "${job.title}"`);
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
      : ["-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-maxrate", `${MAX_BITRATE_MBPS}M`, "-bufsize", `${MAX_BITRATE_MBPS}M`,
        "-profile:v", "high", "-level", "4.1", "-pix_fmt", "yuv420p",
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

    // Compute pHash and resolution on the transcoded file
    console.log(`[PHASH] Computing pHash: "${job.title}"`);
    const [phashResult, resHeight] = await Promise.all([
      computePhash(mp4Path, jobDir).catch((err) => {
        console.error(`[PHASH] Failed for "${job.title}":`, err);
        return null;
      }),
      probeResolutionHeight(mp4Path),
    ]);

    if (phashResult) {
      console.log(`[PHASH] "${job.title}" — ${phashResult.hashes.length} hashes, ${resHeight}p`);
    }

    // Upload processed MP4
    console.log(`[UPLOAD] Uploading MP4: "${job.title}"`);
    await uploadFileToR2(mp4Path, mp4Key, "video/mp4");

    // Delete original from R2
    console.log(`[CLEANUP] Deleting original: ${job.original_key}`);
    await deleteFromR2(job.original_key).catch(() => {});

    // Update status to ready with pHash data
    const phashJson = phashResult ? JSON.stringify(phashResult.hashes) : null;

    if (job.type === "movie") {
      await query(
        `UPDATE content SET status = 'ready', hls_key = $1, phashes = $2, resolution_height = $3, phash_verified = TRUE WHERE id = $4`,
        [mp4Key, phashJson, resHeight, job.content_id]
      );
    } else {
      await query(
        `UPDATE series_episodes SET status = 'ready', hls_key = $1, phashes = $2, resolution_height = $3, phash_verified = TRUE
         WHERE content_id = $4 AND season_number = $5 AND episode_number = $6`,
        [mp4Key, phashJson, resHeight, job.content_id, job.season_number, job.episode_number]
      );
    }

    console.log(`[DONE] "${job.title}" — ready to watch`);
  } finally {
    fs.rmSync(jobDir, { recursive: true, force: true });
  }
}

async function selfDestruct() {
  console.log("[DO] Idle timeout reached, self-destructing droplet...");
  try {
    // Get our own droplet ID from DO metadata service
    const metaRes = await fetch("http://169.254.169.254/metadata/v1/id");
    const dropletId = (await metaRes.text()).trim();
    console.log(`[DO] Droplet ID: ${dropletId}`);

    // Delete only this specific droplet (not all pool members)
    const delRes = await fetch(`https://api.digitalocean.com/v2/droplets/${dropletId}`, {
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
  const poolLabel = TRANSCODE_POOL || "all";
  console.log(`Aperture Transcoder running... (mode: ${mode}, pool: ${poolLabel}, concurrency: ${MAX_CONCURRENT}, idle timeout: ${IDLE_TIMEOUT / 1000}s)`);

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
