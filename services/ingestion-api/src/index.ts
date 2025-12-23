import express from "express";
import { createProducer } from '@analytics/clients'
const app = express();
app.use(express.json({limit: "1mb"}));
const producer = createProducer();
await producer.connect();


app.post("/v1/events", async (req, res) => {
  try {
    const tenantId = req.body?.tenantId ?? "unknown";
    const events = Array.isArray(req.body?.events) ? req.body.events : [];

    console.log("received", events.length, "events for tenant", tenantId);

    if (events.length === 0) {
      return res.status(400).json({ ok: false, error: "events must be a non-empty array" });
    }

 await producer.send({
  topic: "events_raw",
  messages: [{
    key: tenantId,
    value: JSON.stringify({
      tenantId,
      events
    })
  }]
});

    console.log("published", events.length, "events to Kafka");
    res.json({ ok: true, count: events.length });
  } catch (err) {
    console.error("FAILED TO PUBLISH TO KAFKA:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.listen(3000, () => console.log("ingestion-api on http://localhost:3000"));