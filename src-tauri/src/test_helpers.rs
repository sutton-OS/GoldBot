use rusqlite::{params, Connection};

pub(crate) fn init_in_memory_db() -> Connection {
    let conn = Connection::open(":memory:").expect("failed to open in-memory sqlite database");
    conn.pragma_update(None, "foreign_keys", "ON")
        .expect("failed to enable foreign_keys pragma");
    conn.execute_batch(include_str!("../migrations/001_init.sql"))
        .expect("failed to apply schema");

    let always_open = r#"{"mon":[["00:00","23:59"]],"tue":[["00:00","23:59"]],"wed":[["00:00","23:59"]],"thu":[["00:00","23:59"]],"fri":[["00:00","23:59"]],"sat":[["00:00","23:59"]],"sun":[["00:00","23:59"]]}"#;
    conn.execute(
        "INSERT INTO locations (gym_name, timezone, business_hours_json) VALUES (?1, ?2, ?3)",
        params!["Test Gym", "America/New_York", always_open],
    )
    .expect("failed to seed default location");

    conn
}
