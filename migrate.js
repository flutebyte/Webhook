const pool = require("./db/pgdb");

async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS subscribers (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      url TEXT UNIQUE,
      event_types TEXT[],
      secret TEXT,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      type TEXT,
      payload JSONB,
      sequence_id TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS deliveries (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_id UUID REFERENCES events(id),
        subscriber_id UUID REFERENCES subscribers(id),

        status TEXT DEFAULT 'pending'
            CHECK (status IN ('pending','processing','success','dead')),

        attempt_count INT DEFAULT 0,
        next_retry_at TIMESTAMPTZ DEFAULT NOW(),

        sequence_id TEXT,
        locked_at TIMESTAMPTZ,

        created_at TIMESTAMPTZ DEFAULT NOW(),

        UNIQUE(event_id, subscriber_id)
        );

    CREATE TABLE IF NOT EXISTS delivery_attempts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      delivery_id UUID REFERENCES deliveries(id),
      attempt_number INT,
      http_status INT,
      response_body TEXT,
      latency_ms INT,
      error TEXT,
      attempted_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  console.log("Tables created");
  process.exit();
}

migrate();