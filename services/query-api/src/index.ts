import express from "express";

const app = express();
app.use(express.json());

app.post("/dau/query", (req, res) => {
  res.json({ ok: true, received: req.body });
});

app.listen(4000, () => console.log("query-api on http://localhost:4000"));