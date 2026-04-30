const express = require("express");
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

app.get("/health", (req, res) => {
  res.send("OK");
});

app.listen(3000, () => {
  console.log("Server running on port 3000");
});