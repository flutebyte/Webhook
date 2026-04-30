const { Pool }= require("pg");

const pool = new Pool({
    user: "postgres",
    host: "localhost",
    database: "webhook",
    password: "lvhi2006",
    port: 5432,
});

module.exports = pool;