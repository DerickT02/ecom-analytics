import express from "express";
import { createClient } from "@clickhouse/client";

const ch = createClient({
  host: "http://localhost:8123",
  username: "analytics",
  password: "analytics_password",
  database: "analytics", // ✅ ensure inserts target the right DB
});

type ActivitySummaryRow = {
  tenant_id: string;
  day: string; // ClickHouse Date comes back as string
  summary_md: string;
  highlights_json: string;
  created_at: string;
};

const app = express();
app.use(express.json());

app.get("/all-events", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw ORDER BY time",
    format: "JSONEachRow"
  });
  const rows = await result.json();
  res.json(rows);
});

app.get("/page-events", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw WHERE tenant_id = 'tenant_derick' and startsWith(name, 'page.') ORDER BY time",
    format: "JSONEachRow"
  });
  const rows = await result.json();
  res.json(rows);
});

app.get("/click-events", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw WHERE tenant_id = 'tenant_derick' and startsWith(name, 'button.') ORDER BY time",
    format: "JSONEachRow"
  });
  const rows = await result.json();
  res.json(rows);
});

app.get("/get-mobile", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw WHERE tenant_id = 'tenant_derick' and source = 'mobile' ORDER BY time",
    format: "JSONEachRow" 
  });    
  const rows = await result.json();
  res.json(rows);
});

app.get("/get-orders-completed", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw WHERE tenant_id = 'tenant_derick' and name = 'order.completed' ORDER BY time",
    format: "JSONEachRow" 
  });    
  const rows = await result.json();
  res.json(rows);
});

app.get("/checkout-events", async (req, res) => {
  const result = await ch.query({
    query: "SELECT * FROM events_raw WHERE tenant_id = 'tenant_derick' and startsWith(name, 'checkout.') ORDER BY time",
    format: "JSONEachRow"
  });
  const rows = await result.json();
  res.json(rows);
});

app.get("/daily/:tenantId", async (req, res) => {
  const tenantId = req.params.tenantId;          // ✅ path param
  const day = (req.query.day as string) ?? null; // ✅ optional query param

  if (!tenantId) {
    return res.status(400).json({ error: "tenantId is required" });
  }

  const query = `
    SELECT
      tenant_id,
      day,
      summary_md,
      highlights_json,
      created_at
    FROM activity_summaries
    WHERE tenant_id = {tenantId:String}
    ${day ? "AND day = toDate({day:String})" : ""}
    ORDER BY created_at DESC
    LIMIT 1
  `;

  const result = await ch.query({
    query,
    query_params: { tenantId, day },
    format: "JSONEachRow",
  });

  const rows = (await result.json()) as ActivitySummaryRow[]; // ✅ typed

  if (!rows.length) {
    return res.status(404).json({ message: "No summary found" });
  }

  const row = rows[0];

  // ✅ return a nicer API shape + parse highlights JSON
  return res.json({
    tenantId: row.tenant_id,
    day: row.day,
    summary: row.summary_md,
    highlights: JSON.parse(row.highlights_json),
    createdAt: row.created_at,
  });
});



app.listen(4000, () => console.log("query-api on http://localhost:4000"));