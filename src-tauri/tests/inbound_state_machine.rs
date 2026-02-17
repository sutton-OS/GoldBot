#[path = "../src/main.rs"]
mod app;

use rusqlite::{params, Connection};
use serde_json::Value;

const FIXED_TS: &str = "2026-01-10T15:00:00Z";
const ALWAYS_OPEN_BUSINESS_HOURS: &str = r#"{
  "mon":[["00:00","23:59"]],
  "tue":[["00:00","23:59"]],
  "wed":[["00:00","23:59"]],
  "thu":[["00:00","23:59"]],
  "fri":[["00:00","23:59"]],
  "sat":[["00:00","23:59"]],
  "sun":[["00:00","23:59"]]
}"#;

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory DB");
    conn.execute_batch(include_str!("../migrations/001_init.sql"))
        .expect("apply schema");

    conn.execute(
        "INSERT INTO locations (id, gym_name, timezone, business_hours_json) VALUES (?, ?, ?, ?)",
        params![1_i64, "Integration Test Gym", "America/New_York", ALWAYS_OPEN_BUSINESS_HOURS],
    )
    .expect("seed location");

    conn.execute(
        "INSERT INTO leads (
            id, phone_e164, first_name, last_name, consent, consent_at, consent_source,
            status, opted_out, needs_staff_attention, last_contact_at, next_action_at, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            1_i64,
            "+15550001111",
            "Pat",
            "Member",
            1_i64,
            FIXED_TS,
            "web_form",
            "awaiting_yes",
            0_i64,
            0_i64,
            Option::<String>::None,
            Option::<String>::None,
            FIXED_TS
        ],
    )
    .expect("seed lead");

    conn.execute(
        "INSERT INTO conversations (
            id, lead_id, state, state_json, last_inbound_at, last_outbound_at, repair_attempts
         ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            1_i64,
            1_i64,
            "awaiting_yes",
            r#"{"offered_slots":[]}"#,
            Option::<String>::None,
            Option::<String>::None,
            0_i64
        ],
    )
    .expect("seed conversation");

    conn
}

#[test]
fn inbound_state_machine_end_to_end() {
    let conn = setup_db();
    let lead_id = 1_i64;
    let conversation_id = 1_i64;

    app::test_execute_initial_follow_up(&conn, lead_id).expect("initial follow-up executes");
    let outbound_after_follow_up: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND direction='OUTBOUND'",
            params![conversation_id],
            |row| row.get(0),
        )
        .expect("count outbound");
    assert_eq!(outbound_after_follow_up, 1);

    app::test_process_inbound_state_machine(&conn, lead_id, "YES").expect("YES flow executes");
    let (state_after_yes, state_json_after_yes): (String, String) = conn
        .query_row(
            "SELECT state, state_json FROM conversations WHERE lead_id=?",
            params![lead_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("load conversation after YES");
    assert_eq!(state_after_yes, "awaiting_time_choice");
    let state_value: Value = serde_json::from_str(&state_json_after_yes).expect("parse state_json");
    let offered_slots = state_value
        .get("offered_slots")
        .and_then(Value::as_array)
        .expect("offered_slots array");
    assert_eq!(offered_slots.len(), 2);

    app::test_process_inbound_state_machine(&conn, lead_id, "1").expect("choice flow executes");
    let appointment_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM appointments WHERE lead_id=? AND status='booked'",
            params![lead_id],
            |row| row.get(0),
        )
        .expect("count appointments");
    assert_eq!(appointment_count, 1);
    let state_after_choice: String = conn
        .query_row(
            "SELECT state FROM conversations WHERE lead_id=?",
            params![lead_id],
            |row| row.get(0),
        )
        .expect("state after choice");
    assert_eq!(state_after_choice, "booked");

    app::test_process_inbound_state_machine(&conn, lead_id, "STOP").expect("STOP flow executes");
    let opted_out: i64 = conn
        .query_row("SELECT opted_out FROM leads WHERE id=?", params![lead_id], |row| {
            row.get(0)
        })
        .expect("load opted_out");
    assert_eq!(opted_out, 1);

    let stop_confirmation_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages
             WHERE conversation_id=?
               AND direction='OUTBOUND'
               AND body='You are unsubscribed and will receive no more automated messages.'",
            params![conversation_id],
            |row| row.get(0),
        )
        .expect("count stop confirmations");
    assert_eq!(stop_confirmation_count, 1);

    let outbound_before_blocked_attempt: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND direction='OUTBOUND'",
            params![conversation_id],
            |row| row.get(0),
        )
        .expect("count outbound before blocked attempt");

    let blocked = app::test_execute_initial_follow_up(&conn, lead_id)
        .expect_err("opted-out lead should block future outbound");
    assert!(
        blocked.contains("lead is opted out; outbound blocked"),
        "unexpected error: {blocked}"
    );

    let outbound_after_blocked_attempt: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE conversation_id=? AND direction='OUTBOUND'",
            params![conversation_id],
            |row| row.get(0),
        )
        .expect("count outbound after blocked attempt");
    assert_eq!(
        outbound_after_blocked_attempt,
        outbound_before_blocked_attempt
    );
}
