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
          (
            status = 'pending'
            AND next_retry_at <= NOW()
          )
          OR
          (
            status = 'processing'
            AND locked_at < NOW() - INTERVAL '5 minutes'
          )
        )
        AND status != 'dead'
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `);

      // 💤 nothing to process
      if (res.rows.length === 0) {
        await client.query("COMMIT");
        client.release();
        client = null;
        await sleep(2000);
        continue;
      }

      const delivery = res.rows[0];

      // 🔥 mark processing
      await client.query(
        `UPDATE deliveries
         SET status = 'processing',
             locked_at = NOW()
         WHERE id = $1`,
        [delivery.id]
      );

      await client.query("COMMIT");
      client.release();
      client = null;

      console.log("🚀 Processing:", delivery.id);

      // 🔒 ORDERING CHECK
      if (delivery.sequence_id) {
        const blockCheck = await pool.query(
          `SELECT 1 FROM deliveries d2
           WHERE d2.subscriber_id = $1
             AND d2.sequence_id = $2
             AND d2.created_at < $3
             AND d2.status NOT IN ('success', 'dead')
           LIMIT 1`,
          [delivery.subscriber_id, delivery.sequence_id, delivery.created_at]
        );

        if (blockCheck.rows.length > 0) {
          console.log("⛔ Blocked:", delivery.id);

          await pool.query(
            `UPDATE deliveries
             SET status = 'pending',
                 locked_at = NULL,
                 next_retry_at = NOW() + INTERVAL '5 seconds'
             WHERE id = $1`,
            [delivery.id]
          );

          continue;
        }
      }

      // 📦 fetch data
      const subRes = await pool.query(
        `SELECT * FROM subscribers WHERE id = $1`,
        [delivery.subscriber_id]
      );

      const eventRes = await pool.query(
        `SELECT * FROM events WHERE id = $1`,
        [delivery.event_id]
      );

      const subscriber = subRes.rows[0];
      const event = eventRes.rows[0];

      if (!subscriber || !event) {
        console.log("⚠️ Missing data, skipping:", delivery.id);
        continue;
      }

      const start = Date.now();

      try {
        const response = await axios.post(
          subscriber.url,
          event.payload,
          {
            timeout: 10000,
            headers: {
              "X-Webhook-Id": delivery.id
            }
          }
        );

        const latency = Date.now() - start;

        // ✅ SUCCESS (increment attempt_count)
        const updateRes = await pool.query(
          `UPDATE deliveries
           SET attempt_count = attempt_count + 1,
               status = 'success'
           WHERE id = $1
           RETURNING attempt_count`,
          [delivery.id]
        );

        const newAttempt = updateRes.rows[0].attempt_count;

        await pool.query(
          `INSERT INTO delivery_attempts
           (delivery_id, attempt_number, http_status, latency_ms)
           VALUES ($1, $2, $3, $4)`,
          [
            delivery.id,
            newAttempt,
            response.status,
            latency
          ]
        );

        console.log("✅ Success:", delivery.id);

      } catch (err) {
        const latency = Date.now() - start;

        let errorBody = err.message;
        if (err.response?.data) {
          try {
            errorBody = JSON.stringify(err.response.data).substring(0, 500);
          } catch { }
        }

        // 🔁 increment attempt_count
        const updateRes = await pool.query(
          `UPDATE deliveries
           SET attempt_count = attempt_count + 1
           WHERE id = $1
           RETURNING attempt_count`,
          [delivery.id]
        );

        const newAttempt = updateRes.rows[0].attempt_count;

        if (newAttempt >= retryDelays.length) {
          // 💀 DEAD
          await pool.query(
            `UPDATE deliveries
             SET status = 'dead'
             WHERE id = $1`,
            [delivery.id]
          );

          console.log("💀 Dead:", delivery.id);

        } else {
          const delay = retryDelays[newAttempt - 1];

          await pool.query(
            `UPDATE deliveries
             SET status = 'pending',
                 next_retry_at = NOW() + ($2 * INTERVAL '1 millisecond')
             WHERE id = $1`,
            [delivery.id, delay]
          );

          console.log("🔁 Retry:", delivery.id, "| attempt:", newAttempt);
        }

        await pool.query(
          `INSERT INTO delivery_attempts
           (delivery_id, attempt_number, http_status, latency_ms, error)
           VALUES ($1, $2, $3, $4, $5)`,
          [
            delivery.id,
            newAttempt,
            err.response?.status || 0,
            latency,
            errorBody
          ]
        );
      }

    } catch (err) {
      console.error("Worker error:", err);
      if (client) await client.query("ROLLBACK");
    } finally {
      if (client) client.release();
    }
  }
}

processDeliveries();