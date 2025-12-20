import express from "express";
import { createProducer } from '@analytics/clients'
const app = express();
app.use(express.json({limit: "1mb"}));
const producer = createProducer();
await producer.connect();


app.post("/v1/events", async (req, res) => {
  const { tenantId, events } = req.body;

  await producer.send({
    topic: "events_raw",
    messages: events.map((event: any) => ({
      key: tenantId,
      value: JSON.stringify(event)
    }))
  });

  res.json({ ok: true, count: events.length });

});

app.listen(3000, () => console.log("ingestion-api on http://localhost:3000"));