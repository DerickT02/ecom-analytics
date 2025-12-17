import express from "express";

const app = express();
app.use(express.json());

app.post("/v1/events", (req, res) => {
  res.json({ ok: true, received: req.body });
});

app.listen(3000, () => console.log("ingestion-api on http://localhost:3000"));