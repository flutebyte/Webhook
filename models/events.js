const pool = require("../db/pgdb");

async function insertEvent({ type, payload, sequence_id }) {
  const res = await pool.query(
    `INSERT INTO events (type, payload, sequence_id)
     VALUES ($1, $2, $3)
     RETURNING *`,
    [type, payload, sequence_id]
  );

  return res.rows[0];
}


async function createDeliveries(eventId, subscribers, sequence_id) {
  for (let sub of subscribers) {
    await pool.query(
      `INSERT INTO deliveries (event_id, subscriber_id, sequence_id)
       VALUES ($1, $2, $3)
       ON CONFLICT DO NOTHING`,
      [eventId, sub.id, sequence_id]
    );
  }
}

module.exports = {
  insertEvent,
  createDeliveries,
};