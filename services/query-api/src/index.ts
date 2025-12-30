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



app.listen(4000, () => console.log("query-api on http://localhost:4000"));