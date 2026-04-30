const express = require("express");
const pool = require("./db/pgdb");
const app = express();

app.use(express.json());
const { upsertSubscriber, findSubscribersForEvent,
  getSubscriberById } = require("./models/subscribers");
const {
  insertEvent,
  createDeliveries,
} = require("./models/events");

app.post("/subscribe", async (req, res) => {
  try {
    const { url, event_types, secret } = req.body;
    if (!url || !event_types || !Array.isArray(event_types)) {
        return res.status(400).json({ error: "url and event_types (array) are required" })
  }

    const subscriber = await upsertSubscriber({ url, event_types, secret });

    res.json({ success: true, subscriber });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Something went wrong" });
  }
});

app.post("/event", async (req, res) => {
  try {
    const { type, payload, sequence_id } = req.body;

    if (!type || !payload) {
        return res.status(400).json({ error: "type and payload are required" })
  }

    const event = await insertEvent({ type, payload, sequence_id });

    const subscribers = await findSubscribersForEvent(type);

    await createDeliveries(event.id, subscribers, sequence_id);

    res.status(202).json({ accepted: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Something went wrong" });
  }
});

app.get("/deliveries", async (req, res) => {
  try {
    const { subscriber, status, page = 1 } = req.query;

    const limit = 10;
    const offset = (page - 1) * limit;

    let query = `
      SELECT * FROM deliveries
      WHERE 1=1
    `;

    const values = [];

    if (subscriber) {
      values.push(subscriber);
      query += ` AND subscriber_id = $${values.length}`;
    }

    if (status) {
      values.push(status);
      query += ` AND status = $${values.length}`;
    }

    values.push(limit);
    values.push(offset);

    query += `
      ORDER BY created_at DESC
      LIMIT $${values.length - 1}
      OFFSET $${values.length}
    `;

    const result = await pool.query(query, values);

    res.json({
      page: Number(page),
      count: result.rows.length,
      deliveries: result.rows
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch deliveries" });
  }
});

app.post("/replay/:delivery_id", async (req, res) => {
  try {
    const { delivery_id } = req.params;

    // 🔹 get original delivery
    const result = await pool.query(
      `SELECT * FROM deliveries WHERE id = $1`,
      [delivery_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Delivery not found" });
    }

    const oldDelivery = result.rows[0];

    // 🔹 only allow replay if dead (optional but good)
    if (oldDelivery.status !== "dead") {
      return res.status(400).json({
        error: "Only dead deliveries can be replayed"
      });
    }

    // 🔹 create NEW delivery (not updating old one)
    const newDelivery = await pool.query(
      `INSERT INTO deliveries (
        event_id,
        subscriber_id,
        status,
        attempt_count,
        next_retry_at,
        sequence_id
      )
      VALUES ($1, $2, 'pending', 0, NOW(), $3)
      RETURNING *`,
      [
        oldDelivery.event_id,
        oldDelivery.subscriber_id,
        oldDelivery.sequence_id
      ]
    );

    res.json({
      message: "Replay created",
      new_delivery: newDelivery.rows[0]
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Replay failed" });
  }
});

app.get("/health", (req, res) => {
  res.send("OK");
});

app.listen(3000, () => {
  console.log("Server running on port 3000");
});