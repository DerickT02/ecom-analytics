import { createConsumer } from "@analytics/clients";
import { S3Client, PutObjectCommand, CreateBucketCommand, HeadBucketCommand } from "@aws-sdk/client-s3";

const TOPIC = "events_raw";
const GROUP_ID = "archiver";

const FLUSH_EVERY_MS = Number(process.env.FLUSH_EVERY_MS ?? 5000);
const MAX_BUFFER = Number(process.env.MAX_BUFFER ?? 500);



const s3 = new S3Client({
  region: "us-east-1",
  endpoint: process.env.S3_ENDPOINT ?? "http://localhost:9002",
  forcePathStyle: true,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY ?? "minio",
    secretAccessKey: process.env.S3_SECRET_KEY ?? "minio_password",
  },
});

const BUCKET = process.env.S3_BUCKET ?? "analytics-archive";


async function ensureBucket(bucket: string) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: bucket }));
  } catch {
    await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    console.log(`[archiver] created bucket ${bucket}`);
  }
}

await ensureBucket(BUCKET);

const consumer = createConsumer(GROUP_ID);

type Buffered = { tenantId: string; value: string; ts: string };

let buffer: Buffered[] = [];
let lastFlush = Date.now();

function keyFor(tenantId: string, isoTime: string) {
  // isoTime like 2025-12-20T12:34:56.789Z
  const date = isoTime.slice(0, 10);
  const hour = isoTime.slice(11, 13);
  const batch = Date.now();
  return `tenant_id=${tenantId}/date=${date}/hour=${hour}/batch_${batch}.jsonl`;
}

async function flush() {
  if (buffer.length === 0) return;

  // naive grouping: first event decides partition
  const first = buffer[0];
  const objectKey = keyFor(first.tenantId, first.ts);

  const body = buffer.map((b) => b.value).join("\n") + "\n";

  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: objectKey,
      Body: body,
      ContentType: "application/x-ndjson",
    })
  );

  console.log(`[archiver] wrote ${buffer.length} events -> s3://${BUCKET}/${objectKey}`);

  buffer = [];
  lastFlush = Date.now();
}

await consumer.connect();
await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

console.log("[archiver] started");


// periodic flush
setInterval(async () => {
  try {
    if (Date.now() - lastFlush >= FLUSH_EVERY_MS) {
      await flush();
    }
  } catch (e) {
    console.error("[archiver] flush error", e);
  }
}, 1000);

await consumer.run({
  eachMessage: async ({ message }) => {
    const raw = message.value?.toString();
    if (!raw) return;

    const tenantId = message.key?.toString() ?? "unknown";

    // best-effort extract time for partitioning
    let ts = new Date().toISOString();
    try {
      const obj = JSON.parse(raw);
      if (typeof obj?.time === "string") ts = obj.time;
    } catch {}

    buffer.push({ tenantId, value: raw, ts });
    
    //flush if buffer exceeds max size
    if (buffer.length >= MAX_BUFFER) {
      await flush();
    }
  },
});

process.on("SIGINT", async () => {
  console.log("[archiver] shutting down...");
  await flush();
  await consumer.disconnect();
  process.exit(0);
});