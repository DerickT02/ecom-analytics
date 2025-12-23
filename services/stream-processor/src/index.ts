import { createConsumer } from "@analytics/clients";
import { createClient } from "@clickhouse/client";
import { EventSchema } from "@analytics/schemas";

const ch = createClient({
  host: "http://localhost:8123",
  username: "analytics",
  password: "analytics_password",
  database: "analytics", // ✅ ensure inserts target the right DB
});

const consumer = createConsumer("stream-processor");

await consumer.connect();
await consumer.subscribe({ topic: "events_raw", fromBeginning: false });

console.log("[stream-processor] started");

await consumer.run({
  eachMessage: async ({ message }) => {
    const raw = message.value?.toString();
    if (!raw) return;

    let json: unknown;
    try {
      json = JSON.parse(raw);
    } catch {
      console.error("Invalid JSON payload");
      return;
    }

    const parsed = EventSchema.safeParse(json);
    if (!parsed.success) {
      console.error("Invalid event schema", parsed.error.flatten());
      return;
    }

    const event = parsed.data;
    const tenantId = message.key?.toString() ?? "unknown";

    console.log("Processed event", event.name);

    const chTime = event.time.replace("T", " ").replace("Z", "");

await ch.insert({
  table: "analytics.events_raw",
  values: [{
    tenant_id: tenantId,
    event_id: event.eventId,
    name: event.name,
    time: chTime, // ✅ "2025-12-20 00:00:00.000"
    source: event.source,
    anonymous_id: event.anonymousId,
    session_id: event.sessionId,
    properties: JSON.stringify(event.properties ?? {}),
    schema_version: event.schemaVersion,
  }],
  format: "JSONEachRow",
});
  },
});

process.on("SIGINT", async () => {
  console.log("[stream-processor] shutting down");
  await consumer.disconnect();
  process.exit(0);
});