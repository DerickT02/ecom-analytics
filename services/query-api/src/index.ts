import express from "express";
import { createClient } from "@clickhouse/client";

const ch = createClient({
  host: "http://localhost:8123",
  username: "analytics",
  password: "analytics_password",
  database: "analytics", // ✅ ensure inserts target the right DB
});

const app = express();
app.use(express.json());

app.get("/all-events", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw",
    format: "JSONEachRow"
  });
  const rows = await result.json();
  res.json(rows);
});

app.listen(4000, () => console.log("query-api on http://localhost:4000"));