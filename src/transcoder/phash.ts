import { spawn } from "child_process";
import fs from "fs";
import path from "path";

export interface FrameHash {
  position: number;
  hash: string;
}

export interface PhashResult {
  hashes: FrameHash[];
  duration_secs: number;
}

function getDuration(inputPath: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffprobe", [
      "-v", "error", "-print_format", "json", "-show_format", inputPath,
    ], { stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    let stderr = "";
    proc.stdout.on("data", (d) => (output += d.toString()));
    proc.stderr.on("data", (d) => (stderr += d.toString()));
    proc.on("close", (code) => {
      if (code !== 0) return reject(new Error(`ffprobe failed: ${stderr || "no output"}`));
      try {
        const data = JSON.parse(output);
        const dur = parseFloat(data.format?.duration || "0");
        if (dur <= 0) return reject(new Error("Could not read duration"));
        resolve(dur);
      } catch (e) { reject(e); }
    });
    proc.on("error", reject);
  });
}

function extractFrame(inputPath: string, timestamp: number, outputPath: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", [
      "-ss", timestamp.toFixed(3), "-i", inputPath,
      "-vframes", "1", "-vf", "scale=32:32", "-pix_fmt", "gray",
      "-f", "rawvideo", "-y", outputPath,
    ], { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    proc.stderr.on("data", (d) => (stderr += d.toString()));
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`Frame extraction failed: ${stderr.slice(-200)}`));
    });
    proc.on("error", reject);
  });
}

function computeDctHash(rawPath: string): string {
  const pixels = fs.readFileSync(rawPath);
  if (pixels.length !== 1024) {
    throw new Error(`Expected 1024 bytes (32x32), got ${pixels.length}`);
  }

  const matrix: number[][] = [];
  for (let y = 0; y < 32; y++) {
    const row: number[] = [];
    for (let x = 0; x < 32; x++) {
      row.push(pixels[y * 32 + x]);
    }
    matrix.push(row);
  }

  const rowDct: number[][] = Array.from({ length: 32 }, () => new Array(32).fill(0));
  for (let y = 0; y < 32; y++) {
    for (let u = 0; u < 32; u++) {
      let sum = 0;
      for (let x = 0; x < 32; x++) {
        sum += matrix[y][x] * Math.cos(((2 * x + 1) * u * Math.PI) / 64);
      }
      const cu = u === 0 ? 1 / Math.SQRT2 : 1;
      rowDct[y][u] = cu * sum * Math.sqrt(2 / 32);
    }
  }

  const dct: number[][] = Array.from({ length: 32 }, () => new Array(32).fill(0));
  for (let u = 0; u < 32; u++) {
    for (let v = 0; v < 32; v++) {
      let sum = 0;
      for (let y = 0; y < 32; y++) {
        sum += rowDct[y][u] * Math.cos(((2 * y + 1) * v * Math.PI) / 64);
      }
      const cv = v === 0 ? 1 / Math.SQRT2 : 1;
      dct[v][u] = cv * sum * Math.sqrt(2 / 32);
    }
  }

  const values: number[] = [];
  for (let y = 0; y < 8; y++) {
    for (let x = 0; x < 8; x++) {
      if (y === 0 && x === 0) continue;
      values.push(dct[y][x]);
    }
  }

  const sorted = [...values].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];

  let hash = 0n;
  for (let y = 0; y < 8; y++) {
    for (let x = 0; x < 8; x++) {
      const bitPos = y * 8 + x;
      if (dct[y][x] > median) {
        hash |= 1n << BigInt(bitPos);
      }
    }
  }

  return hash.toString(16).padStart(16, "0");
}

export async function computePhash(inputPath: string, workDir: string): Promise<PhashResult> {
  const duration = await getDuration(inputPath);
  if (duration < 10) {
    throw new Error("Video too short for pHash verification");
  }

  const numFrames = 10;
  const hashes: FrameHash[] = [];

  for (let i = 1; i <= numFrames; i++) {
    const position = i / (numFrames + 1);
    const timestamp = duration * position;
    const framePath = path.join(workDir, `phash_frame_${i}.raw`);

    await extractFrame(inputPath, timestamp, framePath);
    const hash = computeDctHash(framePath);
    hashes.push({ position, hash });
    fs.unlinkSync(framePath);
  }

  return { hashes, duration_secs: duration };
}

export function probeResolutionHeight(inputPath: string): Promise<number | null> {
  return new Promise((resolve) => {
    const proc = spawn("ffprobe", [
      "-v", "quiet", "-select_streams", "v:0",
      "-show_entries", "stream=height",
      "-print_format", "json", inputPath,
    ], { stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    proc.stdout.on("data", (d) => (output += d.toString()));
    proc.on("close", () => {
      try {
        const data = JSON.parse(output);
        resolve(data.streams?.[0]?.height || null);
      } catch { resolve(null); }
    });
    proc.on("error", () => resolve(null));
  });
}
