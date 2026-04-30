const pool = require("../db/pgdb");

async function upsertSubscriber({ url, event_types, secret }) {
  const query = `
    INSERT INTO subscribers (url, event_types, secret)
    VALUES ($1, $2, $3)
    ON CONFLICT (url)
    DO UPDATE SET event_types = EXCLUDED.event_types,
                  secret = EXCLUDED.secret
    RETURNING *;
  `;

  const values = [url, event_types, secret];

  const res = await pool.query(query, values);
  return res.rows[0];
}
async function findSubscribersForEvent(eventType) {
  const result = await pool.query(
    `SELECT * FROM subscribers
     WHERE is_active = true
       AND event_types @> ARRAY[$1]::text[]`,
    [eventType]
  )
  return result.rows
}

async function getSubscriberById(id) {
  const result = await pool.query(
    `SELECT * FROM subscribers WHERE id = $1`,
    [id]
  )
  return result.rows[0] || null
}

module.exports = { upsertSubscriber, findSubscribersForEvent,
  getSubscriberById };