import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import fs from "fs";
import { env } from "../config/env";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
  },
});

const MULTIPART_THRESHOLD = 2 * 1024 * 1024 * 1024; // 2 GB

export async function uploadFileToR2(localPath: string, key: string, contentType: string): Promise<void> {
  const fileSize = fs.statSync(localPath).size;
  const body = fs.createReadStream(localPath);

  if (fileSize >= MULTIPART_THRESHOLD) {
    const upload = new Upload({
      client: s3,
      params: {
        Bucket: env.R2_BUCKET_NAME,
        Key: key,
        Body: body,
        ContentType: contentType,
      },
      partSize: 100 * 1024 * 1024, // 100 MB parts
      leavePartsOnError: false,
    });
    await upload.done();
  } else {
    await s3.send(
      new PutObjectCommand({
        Bucket: env.R2_BUCKET_NAME,
        Key: key,
        Body: body,
        ContentType: contentType,
      })
    );
  }
}

export async function downloadFromR2(key: string, localPath: string): Promise<void> {
  const response = await s3.send(new GetObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: key }));
  const body = response.Body as NodeJS.ReadableStream;
  const ws = fs.createWriteStream(localPath);
  await new Promise<void>((resolve, reject) => {
    body.pipe(ws);
    ws.on("finish", resolve);
    ws.on("error", reject);
  });
}

export async function fileExistsInR2(key: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: env.R2_BUCKET_NAME, Key: key }));
    return true;
  } catch {
    return false;
  }
}

export async function deleteFromR2(key: string): Promise<void> {
  await s3.send(
    new DeleteObjectCommand({
      Bucket: env.R2_BUCKET_NAME,
      Key: key,
    })
  );
}
