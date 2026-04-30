const pool = require("./db/pgdb");
const axios = require("axios");

const retryDelays = [10000, 30000, 120000, 600000, 3600000];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDeliveries() {
  while (true) {
    let client;

    try {
      client = await pool.connect();
      await client.query("BEGIN");

      const res = await client.query(`
        SELECT * FROM deliveries
        WHERE (
          (status = 'pending' AND next_retry_at <= NOW())
          OR
          (status = 'processing' AND locked_at < NOW() - INTERVAL '10 minutes')
        )
        AND status != 'dead'
        AND attempt_count < 6
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `);

      if (res.rows.length === 0) {
        await client.query("COMMIT");
        await sleep(1500);
        continue;
      }

      const delivery = res.rows[0];

      // mark processing
      await client.query(`
        UPDATE deliveries 
        SET status = 'processing',
            locked_at = NOW()
        WHERE id = $1
      `, [delivery.id]);

      await client.query("COMMIT");

      console.log(`🚀 Processing: ${delivery.id}`);

      // ordering check
      if (delivery.sequence_id) {
        const blockCheck = await pool.query(`
          SELECT 1 FROM deliveries
          WHERE subscriber_id = $1
            AND sequence_id = $2
            AND created_at < $3
            AND status NOT IN ('success', 'dead')
          LIMIT 1
        `, [delivery.subscriber_id, delivery.sequence_id, delivery.created_at]);

        if (blockCheck.rows.length > 0) {
          console.log(`⛔ Blocked: ${delivery.id}`);

          await pool.query(`
            UPDATE deliveries
            SET status = 'pending',
                locked_at = NULL,
                next_retry_at = NOW() + INTERVAL '10 seconds'
            WHERE id = $1
          `, [delivery.id]);

          continue;
        }
      }

      // fetch data
      const [subRes, eventRes] = await Promise.all([
        pool.query(`SELECT * FROM subscribers WHERE id = $1`, [delivery.subscriber_id]),
        pool.query(`SELECT * FROM events WHERE id = $1`, [delivery.event_id])
      ]);

      const subscriber = subRes.rows[0];
      const event = eventRes.rows[0];

      if (!subscriber?.url || !event) {
        console.log(`⚠️ Missing data: ${delivery.id}`);
        await pool.query(`UPDATE deliveries SET status = 'dead' WHERE id = $1`, [delivery.id]);
        continue;
      }

      const start = Date.now();

      try {
        const crypto = require("crypto");

// ensure payload is string for HMAC
const payloadString = JSON.stringify(event.payload || {});

let signature = null;

if (subscriber.secret) {
  const hmac = crypto
    .createHmac("sha256", subscriber.secret)
    .update(payloadString)
    .digest("hex");

  signature = `sha256=${hmac}`;
}

const headers = {
  "Content-Type": "application/json",
  "X-Webhook-Id": delivery.id
};

if (signature) {
  headers["X-Webhook-Signature"] = signature;
}

const response = await axios.post(
  subscriber.url,
  JSON.parse(payloadString),
  {
    timeout: 10000,
    headers
  }
);

        const latency = Date.now() - start;

        // success
        const updateRes = await pool.query(`
          UPDATE deliveries
          SET status = 'success',
              attempt_count = attempt_count + 1,
              locked_at = NULL
          WHERE id = $1
          RETURNING attempt_count
        `, [delivery.id]);

        const newAttempt = updateRes.rows[0].attempt_count;

        await pool.query(`
          INSERT INTO delivery_attempts
          (delivery_id, attempt_number, http_status, latency_ms)
          VALUES ($1, $2, $3, $4)
        `, [delivery.id, newAttempt, response.status, latency]);

        console.log(`✅ Success: ${delivery.id}`);

      } catch (err) {
        const latency = Date.now() - start;

        let errorBody = err.message;
        if (err.response?.data) {
          try {
            errorBody = JSON.stringify(err.response.data).substring(0, 500);
          } catch {}
        }

        const newAttempt = delivery.attempt_count + 1;

        if (newAttempt >= retryDelays.length) {
          await pool.query(`
            UPDATE deliveries
            SET status = 'dead',
                attempt_count = $2,
                locked_at = NULL
            WHERE id = $1
          `, [delivery.id, newAttempt]);

          console.log(`💀 Dead: ${delivery.id}`);
        } else {
          const delay = retryDelays[newAttempt - 1];

          await pool.query(`
            UPDATE deliveries
            SET status = 'pending',
                attempt_count = $2,
                next_retry_at = NOW() + ($3 * INTERVAL '1 millisecond'),
                locked_at = NULL
            WHERE id = $1
          `, [delivery.id, newAttempt, delay]);

          console.log(`🔁 Retry: ${delivery.id} attempt ${newAttempt}`);
        }

        await pool.query(`
          INSERT INTO delivery_attempts
          (delivery_id, attempt_number, http_status, latency_ms, error)
          VALUES ($1, $2, $3, $4, $5)
        `, [
          delivery.id,
          newAttempt,
          err.response?.status || 0,
          latency,
          errorBody
        ]);
      }

    } catch (err) {
      console.error("Worker critical error:", err);

      if (client) {
        try { await client.query("ROLLBACK"); } catch {}
      }

      await sleep(2000);

    } finally {
      if (client) client.release();
    }
  }
}

processDeliveries();