#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Timelike, Utc, Weekday,
};
use chrono_tz::Tz;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration as StdDuration;
use tauri::{AppHandle, Manager, State};
use thiserror::Error;

#[cfg(test)]
mod test_helpers;

#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("time parse error: {0}")]
    Chrono(#[from] chrono::ParseError),
    #[error("{0}")]
    Validation(String),
}

type AppResult<T> = Result<T, AppError>;

impl AppError {
    fn is_busy_or_locked(&self) -> bool {
        match self {
            AppError::Db(rusqlite::Error::SqliteFailure(err, _)) => {
                err.code == rusqlite::ErrorCode::DatabaseBusy
                    || err.code == rusqlite::ErrorCode::DatabaseLocked
            }
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LeadCreateInput {
    first_name: String,
    last_name: String,
    phone_e164: String,
    consent: bool,
    consent_at: Option<String>,
    source: String,
}

#[derive(Debug, Serialize)]
struct LeadCreateResult {
    created: bool,
    lead_id: i64,
    duplicate_of: Option<i64>,
    note: Option<String>,
}

#[derive(Debug, Serialize)]
struct LeadSummary {
    id: i64,
    phone_e164: String,
    first_name: Option<String>,
    last_name: Option<String>,
    status: String,
    consent: bool,
    opted_out: bool,
    needs_staff_attention: bool,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct LeadDetailLead {
    id: i64,
    phone_e164: String,
    first_name: Option<String>,
    last_name: Option<String>,
    status: String,
    consent: bool,
    consent_at: Option<String>,
    consent_source: Option<String>,
    opted_out: bool,
    needs_staff_attention: bool,
    last_contact_at: Option<String>,
    next_action_at: Option<String>,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct ConversationView {
    id: i64,
    state: String,
    state_json: String,
    last_inbound_at: Option<String>,
    last_outbound_at: Option<String>,
    repair_attempts: i64,
}

#[derive(Debug, Serialize)]
struct MessageView {
    id: i64,
    direction: String,
    body: String,
    status: String,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct AppointmentView {
    id: i64,
    start_at: String,
    end_at: String,
    status: String,
}

#[derive(Debug, Serialize)]
struct LeadDetail {
    lead: LeadDetailLead,
    conversation: ConversationView,
    messages: Vec<MessageView>,
    appointments: Vec<AppointmentView>,
}

#[derive(Debug, Serialize)]
struct TodayReport {
    leads_created: i64,
    contacted: i64,
    booked: i64,
    opt_outs: i64,
    needs_attention: i64,
}

#[derive(Debug, Serialize)]
struct RunJobsResult {
    processed: i64,
    skipped: i64,
    errors: i64,
}

#[derive(Debug)]
struct Location {
    id: i64,
    gym_name: String,
    timezone: String,
    business_hours_json: String,
}

#[derive(Debug)]
struct LeadRow {
    id: i64,
    phone_e164: String,
    first_name: Option<String>,
    last_name: Option<String>,
    consent: bool,
    opted_out: bool,
    status: String,
    needs_staff_attention: bool,
}

#[derive(Debug)]
struct ConversationRow {
    id: i64,
    lead_id: i64,
    state: String,
    state_json: String,
    last_inbound_at: Option<String>,
    last_outbound_at: Option<String>,
    repair_attempts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SlotChoice {
    start_at: String,
    end_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ConversationState {
    offered_slots: Vec<SlotChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutboundRequest {
    lead_id: i64,
    conversation_id: i64,
    body: String,
    automated: bool,
    allow_without_consent: bool,
    allow_opted_out_once: bool,
    allow_after_reply: bool,
    ignore_business_hours: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ScheduleJobRequest {
    job_type: String,
    target_id: Option<i64>,
    execute_at: String,
    payload_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppointmentRequest {
    lead_id: i64,
    start_at: String,
    end_at: String,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OptOutRequest {
    lead_id: i64,
    reason: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AgentActionType {
    SendOutbound,
    BookAppointment,
    SetOptOut,
    ScheduleJob,
}

impl AgentActionType {
    fn as_str(self) -> &'static str {
        match self {
            AgentActionType::SendOutbound => "send_outbound",
            AgentActionType::BookAppointment => "book_appointment",
            AgentActionType::SetOptOut => "set_opt_out",
            AgentActionType::ScheduleJob => "schedule_job",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action_type", rename_all = "snake_case")]
enum AgentAction {
    SendOutbound {
        lead_id: i64,
        conversation_id: i64,
        body: String,
        automated: bool,
        allow_without_consent: bool,
        allow_opted_out_once: bool,
        allow_after_reply: bool,
        ignore_business_hours: bool,
    },
    BookAppointment {
        lead_id: i64,
        start_at: String,
        end_at: String,
        status: String,
    },
    SetOptOut {
        lead_id: i64,
        reason: String,
    },
    ScheduleJob {
        job_type: String,
        target_id: Option<i64>,
        execute_at: String,
        payload_json: String,
    },
}

impl AgentAction {
    fn action_type(&self) -> AgentActionType {
        match self {
            AgentAction::SendOutbound { .. } => AgentActionType::SendOutbound,
            AgentAction::BookAppointment { .. } => AgentActionType::BookAppointment,
            AgentAction::SetOptOut { .. } => AgentActionType::SetOptOut,
            AgentAction::ScheduleJob { .. } => AgentActionType::ScheduleJob,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentDryRunRequest {
    action: AgentAction,
}

#[derive(Debug, Serialize)]
struct AgentDryRunResult {
    allowed: bool,
    blocked_reason: Option<String>,
    warnings: Vec<String>,
    normalized: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentExecuteRequest {
    action: AgentAction,
}

#[derive(Debug, Serialize)]
struct AgentExecuteResult {
    success: bool,
    result_json: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InitialFollowUpPayload {
    lead_id: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReminderPayload {
    lead_id: i64,
    appointment_id: i64,
    start_at: String,
}

struct ActionGateway<'a> {
    conn: &'a Connection,
    location: &'a Location,
}

impl<'a> ActionGateway<'a> {
    fn new(conn: &'a Connection, location: &'a Location) -> Self {
        Self { conn, location }
    }

    fn validate_outbound(&self, req: &OutboundRequest) -> AppResult<()> {
        if req.automated && is_kill_switch_enabled(self.conn)? {
            return Err(AppError::Validation(
                "kill switch is enabled; automated outbound blocked".to_string(),
            ));
        }

        let lead = get_lead(self.conn, req.lead_id)?;
        let convo = get_conversation_by_lead_id(self.conn, req.lead_id)?;
        if convo.id != req.conversation_id {
            return Err(AppError::Validation(
                "conversation_id does not match lead".to_string(),
            ));
        }

        if !lead.consent && !req.allow_without_consent {
            return Err(AppError::Validation(
                "consent required before outbound".to_string(),
            ));
        }

        if lead.opted_out && !req.allow_opted_out_once {
            return Err(AppError::Validation(
                "lead is opted out; outbound blocked".to_string(),
            ));
        }

        if !req.ignore_business_hours && !is_business_open(self.location, Utc::now())? {
            return Err(AppError::Validation(
                "outside business hours; outbound blocked".to_string(),
            ));
        }

        self.check_rate_limits(req.lead_id, &convo, req.allow_after_reply)?;
        Ok(())
    }

    fn validate_agent_outbound(&self, req: &OutboundRequest) -> AppResult<()> {
        if req.allow_without_consent {
            return Err(AppError::Validation(
                "agent outbound cannot bypass consent".to_string(),
            ));
        }
        if req.allow_opted_out_once {
            return Err(AppError::Validation(
                "agent outbound cannot bypass opt-out suppression".to_string(),
            ));
        }
        if req.ignore_business_hours {
            return Err(AppError::Validation(
                "agent outbound cannot ignore business hours".to_string(),
            ));
        }

        self.validate_outbound(req)?;

        // messages.created_at is stored as RFC3339 TEXT (from now_iso), so normalize ISO "T"/"Z"
        // before datetime comparison for robust parsing.
        let duplicate_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM messages
             WHERE conversation_id = ?
               AND direction = 'OUTBOUND'
               AND body = ?
               AND datetime(replace(replace(created_at, 'T', ' '), 'Z', '')) >= datetime('now', '-10 minutes')",
            params![req.conversation_id, req.body],
            |row| row.get(0),
        )?;
        if duplicate_count > 0 {
            return Err(AppError::Validation(
                "idempotency block: duplicate outbound body for conversation within 10 minutes"
                    .to_string(),
            ));
        }

        Ok(())
    }

    fn validate_appointment(&self, req: &AppointmentRequest) -> AppResult<()> {
        let lead = get_lead(self.conn, req.lead_id)?;
        if lead.opted_out {
            return Err(AppError::Validation(
                "cannot book appointment for opted-out lead".to_string(),
            ));
        }

        let start = parse_ts(&req.start_at)?;
        let end = parse_ts(&req.end_at)?;
        if end <= start {
            return Err(AppError::Validation(
                "appointment end must be after start".to_string(),
            ));
        }

        let overlap_count: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM appointments
             WHERE status = 'booked'
               AND datetime(start_at) < datetime(?, '+10 minutes')
               AND datetime(end_at, '+10 minutes') > datetime(?)",
            params![req.end_at, req.start_at],
            |row| row.get(0),
        )?;

        if overlap_count > 0 {
            return Err(AppError::Validation(
                "selected appointment slot is no longer available".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_schedule_job(&self, _req: &ScheduleJobRequest) -> AppResult<()> {
        if is_kill_switch_enabled(self.conn)? {
            return Err(AppError::Validation(
                "kill switch is enabled; job scheduling blocked".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_opt_out(&self, req: &OptOutRequest) -> AppResult<()> {
        let _ = get_lead(self.conn, req.lead_id)?;
        Ok(())
    }

    fn create_outbound_message(&self, req: OutboundRequest) -> AppResult<i64> {
        let request_json = serde_json::to_value(&req)?;
        let action = "create_outbound_message";
        let target_type = "conversation";
        let target_id = Some(req.conversation_id.to_string());

        let result = (|| -> AppResult<i64> {
            self.validate_outbound(&req)?;

            let now = now_iso();
            self.conn.execute(
                "INSERT INTO messages (conversation_id, direction, body, status, created_at) VALUES (?, 'OUTBOUND', ?, 'sent', ?)",
                params![req.conversation_id, req.body, now],
            )?;
            let message_id = self.conn.last_insert_rowid();

            self.conn.execute(
                "UPDATE conversations SET last_outbound_at = ? WHERE id = ?",
                params![now, req.conversation_id],
            )?;

            self.conn.execute(
                "UPDATE leads SET last_contact_at = ?, status = COALESCE(status, 'awaiting_yes') WHERE id = ?",
                params![now, req.lead_id],
            )?;

            Ok(message_id)
        })();

        match result {
            Ok(message_id) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    target_type,
                    target_id,
                    request_json,
                    Some(json!({ "message_id": message_id })),
                    true,
                    None,
                );
                Ok(message_id)
            }
            Err(err) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    target_type,
                    target_id,
                    request_json,
                    None,
                    false,
                    Some(err.to_string()),
                );
                Err(err)
            }
        }
    }

    fn create_outbound_message_for_agent(&self, req: OutboundRequest) -> AppResult<i64> {
        self.validate_agent_outbound(&req)?;
        self.create_outbound_message(req)
    }

    fn create_appointment(&self, req: AppointmentRequest) -> AppResult<i64> {
        let request_json = serde_json::to_value(&req)?;
        let action = "create_appointment";
        let target_type = "lead";
        let target_id = Some(req.lead_id.to_string());

        let result = (|| -> AppResult<i64> {
            self.validate_appointment(&req)?;

            self.conn.execute(
                "INSERT INTO appointments (lead_id, start_at, end_at, status, created_at) VALUES (?, ?, ?, ?, ?)",
                params![req.lead_id, req.start_at, req.end_at, req.status, now_iso()],
            )?;

            let appointment_id = self.conn.last_insert_rowid();
            self.conn.execute(
                "UPDATE leads SET status='booked', next_action_at=NULL WHERE id=?",
                params![req.lead_id],
            )?;

            Ok(appointment_id)
        })();

        match result {
            Ok(appointment_id) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    target_type,
                    target_id,
                    request_json,
                    Some(json!({ "appointment_id": appointment_id })),
                    true,
                    None,
                );
                Ok(appointment_id)
            }
            Err(err) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    target_type,
                    target_id,
                    request_json,
                    None,
                    false,
                    Some(err.to_string()),
                );
                Err(err)
            }
        }
    }

    fn set_opt_out(&self, req: OptOutRequest) -> AppResult<()> {
        let request_json = serde_json::to_value(&req)?;
        let action = "set_opt_out";

        let result = (|| -> AppResult<()> {
            self.validate_opt_out(&req)?;
            self.conn.execute(
                "UPDATE leads SET opted_out=1, status='opted_out', next_action_at=NULL WHERE id=?",
                params![req.lead_id],
            )?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "lead",
                    Some(req.lead_id.to_string()),
                    request_json,
                    Some(json!({ "result": "opted_out" })),
                    true,
                    None,
                );
                Ok(())
            }
            Err(err) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "lead",
                    Some(req.lead_id.to_string()),
                    request_json,
                    None,
                    false,
                    Some(err.to_string()),
                );
                Err(err)
            }
        }
    }

    fn schedule_job(&self, req: ScheduleJobRequest) -> AppResult<i64> {
        let request_json = serde_json::to_value(&req)?;
        let action = "schedule_job";

        let result = (|| -> AppResult<i64> {
            self.validate_schedule_job(&req)?;
            self.conn.execute(
                "INSERT INTO scheduled_jobs (job_type, target_id, execute_at, status, payload_json, created_at)
                 VALUES (?, ?, ?, 'pending', ?, ?)",
                params![req.job_type, req.target_id, req.execute_at, req.payload_json, now_iso()],
            )?;
            Ok(self.conn.last_insert_rowid())
        })();

        match result {
            Ok(job_id) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "scheduled_job",
                    Some(job_id.to_string()),
                    request_json,
                    Some(json!({ "job_id": job_id })),
                    true,
                    None,
                );
                Ok(job_id)
            }
            Err(err) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "scheduled_job",
                    None,
                    request_json,
                    None,
                    false,
                    Some(err.to_string()),
                );
                Err(err)
            }
        }
    }

    fn cancel_jobs_on_kill_switch(&self) -> AppResult<usize> {
        let action = "cancel_jobs_on_kill_switch";
        let request = json!({ "scope": "all_pending" });

        let result = (|| -> AppResult<usize> {
            let changed = self.conn.execute(
                "UPDATE scheduled_jobs SET status='cancelled' WHERE status='pending'",
                params![],
            )?;
            Ok(changed)
        })();

        match result {
            Ok(changed) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "scheduled_job",
                    None,
                    request,
                    Some(json!({ "cancelled": changed })),
                    true,
                    None,
                );
                Ok(changed)
            }
            Err(err) => {
                let _ = insert_audit(
                    self.conn,
                    action,
                    "scheduled_job",
                    None,
                    request,
                    None,
                    false,
                    Some(err.to_string()),
                );
                Err(err)
            }
        }
    }

    fn check_rate_limits(
        &self,
        lead_id: i64,
        convo: &ConversationRow,
        allow_after_reply: bool,
    ) -> AppResult<()> {
        let per_lead_today: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM messages m
             JOIN conversations c ON c.id = m.conversation_id
             WHERE c.lead_id = ?
               AND m.direction = 'OUTBOUND'
               AND date(m.created_at, 'localtime') = date('now', 'localtime')",
            params![lead_id],
            |row| row.get(0),
        )?;
        if per_lead_today >= 4 {
            return Err(AppError::Validation(
                "rate limit: max 4 outbound per lead/day".to_string(),
            ));
        }

        let per_location_hour: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM messages
             WHERE direction = 'OUTBOUND'
               AND datetime(created_at) >= datetime('now', '-1 hour')",
            params![],
            |row| row.get(0),
        )?;
        if per_location_hour >= 100 {
            return Err(AppError::Validation(
                "rate limit: max 100 outbound per location/hour".to_string(),
            ));
        }

        if let Some(last_outbound_at) = &convo.last_outbound_at {
            let last_outbound = parse_ts(last_outbound_at)?;
            let since_last_outbound = Utc::now().signed_duration_since(last_outbound);
            if since_last_outbound < Duration::hours(2) {
                let replied_since_last = match (&convo.last_inbound_at, allow_after_reply) {
                    (Some(last_inbound_at), true) => parse_ts(last_inbound_at)
                        .map(|inbound| inbound > last_outbound)
                        .unwrap_or(false),
                    _ => false,
                };

                if !replied_since_last {
                    return Err(AppError::Validation(
                        "rate limit: minimum 2 hours between outbound unless lead just replied"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

#[tauri::command]
fn create_lead(
    state: State<AppState>,
    app: AppHandle,
    input: LeadCreateInput,
) -> Result<LeadCreateResult, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;
        let now = now_iso();

        let phone = input.phone_e164.trim().to_string();
        if phone.is_empty() || !phone.starts_with('+') {
            return Err(AppError::Validation(
                "phone_e164 must be non-empty and start with '+'".to_string(),
            ));
        }

        let duplicate_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM leads
                 WHERE phone_e164 = ?
                   AND datetime(created_at) >= datetime('now', '-30 days')
                 ORDER BY created_at DESC
                 LIMIT 1",
                params![phone],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(existing) = duplicate_id {
            let note = "Duplicate lead in last 30 days; automation not restarted. Note added to audit log.";
            let _ = insert_audit(
                &conn,
                "duplicate_lead_detected",
                "lead",
                Some(existing.to_string()),
                json!({
                    "phone_e164": phone,
                    "source": input.source,
                    "attempted_at": now
                }),
                Some(json!({ "note": note })),
                true,
                None,
            );

            return Ok(LeadCreateResult {
                created: false,
                lead_id: existing,
                duplicate_of: Some(existing),
                note: Some(note.to_string()),
            });
        }

        conn.execute(
            "INSERT INTO leads (
                phone_e164, first_name, last_name, consent, consent_at, consent_source,
                status, opted_out, needs_staff_attention, created_at
             ) VALUES (?, ?, ?, ?, ?, ?, 'awaiting_yes', 0, 0, ?)",
            params![
                phone,
                null_if_empty(&input.first_name),
                null_if_empty(&input.last_name),
                bool_to_i64(input.consent),
                input.consent_at,
                null_if_empty(&input.source),
                now
            ],
        )?;

        let lead_id = conn.last_insert_rowid();

        conn.execute(
            "INSERT INTO conversations (lead_id, state, state_json, repair_attempts) VALUES (?, 'awaiting_yes', ?, 0)",
            params![lead_id, serde_json::to_string(&ConversationState::default())?],
        )?;

        let mut note: Option<String> = None;
        if input.consent {
            let gateway = ActionGateway::new(&conn, &location);
            let execute_at_utc = if is_business_open(&location, Utc::now())? {
                Utc::now() + Duration::seconds(30)
            } else {
                next_open_time(&location, Utc::now())?
            };

            let schedule = gateway.schedule_job(ScheduleJobRequest {
                job_type: "initial_follow_up".to_string(),
                target_id: Some(lead_id),
                execute_at: execute_at_utc.to_rfc3339(),
                payload_json: serde_json::to_string(&InitialFollowUpPayload { lead_id })?,
            });

            match schedule {
                Ok(_) => {
                    conn.execute(
                        "UPDATE leads SET next_action_at=? WHERE id=?",
                        params![execute_at_utc.to_rfc3339(), lead_id],
                    )?;
                }
                Err(err) => {
                    note = Some(format!(
                        "Lead created, but auto-follow-up not scheduled: {err}"
                    ));
                }
            }
        }

        Ok(LeadCreateResult {
            created: true,
            lead_id,
            duplicate_of: None,
            note,
        })
    });

    map_cmd_result(result, "create_lead", &app)
}

#[tauri::command]
fn list_leads(state: State<AppState>, app: AppHandle) -> Result<Vec<LeadSummary>, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let mut stmt = conn.prepare(
            "SELECT id, phone_e164, first_name, last_name, status, consent, opted_out, needs_staff_attention, created_at
             FROM leads
             ORDER BY datetime(created_at) DESC",
        )?;

        let rows = stmt.query_map(params![], |row| {
            Ok(LeadSummary {
                id: row.get(0)?,
                phone_e164: row.get(1)?,
                first_name: row.get(2)?,
                last_name: row.get(3)?,
                status: row.get(4)?,
                consent: i64_to_bool(row.get(5)?),
                opted_out: i64_to_bool(row.get(6)?),
                needs_staff_attention: i64_to_bool(row.get(7)?),
                created_at: row.get(8)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
    });

    map_cmd_result(result, "list_leads", &app)
}

#[tauri::command]
fn search_leads(
    state: State<AppState>,
    app: AppHandle,
    query: String,
) -> Result<Vec<LeadSummary>, String> {
    let query_trimmed = query.trim();
    let wildcard = format!("%{}%", query_trimmed.to_lowercase());
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let mut stmt = conn.prepare(
            "SELECT id, phone_e164, first_name, last_name, status, consent, opted_out, needs_staff_attention, created_at
             FROM leads
             WHERE LOWER(phone_e164) LIKE ?1
                OR LOWER(COALESCE(first_name, '')) LIKE ?1
                OR LOWER(COALESCE(last_name, '')) LIKE ?1
             ORDER BY datetime(created_at) DESC",
        )?;
        let rows = stmt.query_map(params![wildcard.clone()], |row| {
            Ok(LeadSummary {
                id: row.get(0)?,
                phone_e164: row.get(1)?,
                first_name: row.get(2)?,
                last_name: row.get(3)?,
                status: row.get(4)?,
                consent: i64_to_bool(row.get(5)?),
                opted_out: i64_to_bool(row.get(6)?),
                needs_staff_attention: i64_to_bool(row.get(7)?),
                created_at: row.get(8)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
    });
    map_cmd_result(result, "search_leads", &app)
}

#[tauri::command]
fn list_agent_queue(state: State<AppState>, app: AppHandle) -> Result<Vec<LeadSummary>, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let mut stmt = conn.prepare(
            "SELECT l.id, l.phone_e164, l.first_name, l.last_name, l.status, l.consent, l.opted_out, l.needs_staff_attention, l.created_at
             FROM leads l
             JOIN conversations c ON c.lead_id = l.id
             WHERE l.opted_out = 0
               AND l.needs_staff_attention = 0
               AND l.consent = 1
               AND (
                    (l.next_action_at IS NOT NULL AND datetime(l.next_action_at) <= datetime('now'))
                    OR (
                        c.last_inbound_at IS NOT NULL
                        AND datetime(c.last_inbound_at) >= datetime('now', '-3 days')
                        AND (
                            c.last_outbound_at IS NULL
                            OR datetime(c.last_inbound_at) > datetime(c.last_outbound_at)
                        )
                    )
               )
             ORDER BY datetime(COALESCE(l.next_action_at, c.last_inbound_at, l.created_at)) ASC",
        )?;

        let rows = stmt.query_map(params![], |row| {
            Ok(LeadSummary {
                id: row.get(0)?,
                phone_e164: row.get(1)?,
                first_name: row.get(2)?,
                last_name: row.get(3)?,
                status: row.get(4)?,
                consent: i64_to_bool(row.get(5)?),
                opted_out: i64_to_bool(row.get(6)?),
                needs_staff_attention: i64_to_bool(row.get(7)?),
                created_at: row.get(8)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(AppError::from)
    });

    map_cmd_result(result, "list_agent_queue", &app)
}

#[tauri::command]
fn get_lead_detail(
    state: State<AppState>,
    app: AppHandle,
    lead_id: i64,
) -> Result<LeadDetail, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;

        let lead = conn
            .query_row(
                "SELECT id, phone_e164, first_name, last_name, status, consent, consent_at, consent_source,
                        opted_out, needs_staff_attention, last_contact_at, next_action_at, created_at
                 FROM leads WHERE id=?",
                params![lead_id],
                |row| {
                    Ok(LeadDetailLead {
                        id: row.get(0)?,
                        phone_e164: row.get(1)?,
                        first_name: row.get(2)?,
                        last_name: row.get(3)?,
                        status: row.get(4)?,
                        consent: i64_to_bool(row.get(5)?),
                        consent_at: row.get(6)?,
                        consent_source: row.get(7)?,
                        opted_out: i64_to_bool(row.get(8)?),
                        needs_staff_attention: i64_to_bool(row.get(9)?),
                        last_contact_at: row.get(10)?,
                        next_action_at: row.get(11)?,
                        created_at: row.get(12)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| AppError::Validation("lead not found".to_string()))?;

        let conversation = conn.query_row(
            "SELECT id, state, state_json, last_inbound_at, last_outbound_at, repair_attempts
             FROM conversations WHERE lead_id=?",
            params![lead_id],
            |row| {
                Ok(ConversationView {
                    id: row.get(0)?,
                    state: row.get(1)?,
                    state_json: row.get(2)?,
                    last_inbound_at: row.get(3)?,
                    last_outbound_at: row.get(4)?,
                    repair_attempts: row.get(5)?,
                })
            },
        )?;

        let mut msg_stmt = conn.prepare(
            "SELECT id, direction, body, status, created_at
             FROM messages
             WHERE conversation_id=?
             ORDER BY datetime(created_at) ASC",
        )?;
        let msg_rows = msg_stmt.query_map(params![conversation.id], |row| {
            Ok(MessageView {
                id: row.get(0)?,
                direction: row.get(1)?,
                body: row.get(2)?,
                status: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?;
        let messages = msg_rows.collect::<Result<Vec<_>, _>>()?;

        let mut apt_stmt = conn.prepare(
            "SELECT id, start_at, end_at, status
             FROM appointments
             WHERE lead_id=?
             ORDER BY datetime(start_at) ASC",
        )?;
        let apt_rows = apt_stmt.query_map(params![lead_id], |row| {
            Ok(AppointmentView {
                id: row.get(0)?,
                start_at: row.get(1)?,
                end_at: row.get(2)?,
                status: row.get(3)?,
            })
        })?;
        let appointments = apt_rows.collect::<Result<Vec<_>, _>>()?;

        Ok(LeadDetail {
            lead,
            conversation,
            messages,
            appointments,
        })
    });

    map_cmd_result(result, "get_lead_detail", &app)
}

#[tauri::command]
fn simulate_inbound_sms(
    state: State<AppState>,
    app: AppHandle,
    lead_id: i64,
    body: String,
) -> Result<(), String> {
    let result = retry_db(|| {
        if body.trim().is_empty() {
            return Err(AppError::Validation(
                "inbound body cannot be empty".to_string(),
            ));
        }

        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;
        let lead = get_lead(&conn, lead_id)?;
        let conversation = get_conversation_by_lead_id(&conn, lead_id)?;

        let now = now_iso();
        conn.execute(
            "INSERT INTO messages (conversation_id, direction, body, status, created_at)
             VALUES (?, 'INBOUND', ?, 'received', ?)",
            params![conversation.id, body.trim(), now],
        )?;

        conn.execute(
            "UPDATE conversations SET last_inbound_at=? WHERE id=?",
            params![now, conversation.id],
        )?;
        conn.execute(
            "UPDATE leads SET last_contact_at=? WHERE id=?",
            params![now, lead_id],
        )?;

        process_inbound_state_machine(&conn, &location, &lead, &conversation, body.trim())
    });

    map_cmd_result(result, "simulate_inbound_sms", &app)
}

#[tauri::command]
fn get_today_report(state: State<AppState>, app: AppHandle) -> Result<TodayReport, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;

        let leads_created: i64 = conn.query_row(
            "SELECT COUNT(*) FROM leads WHERE date(created_at, 'localtime') = date('now', 'localtime')",
            params![],
            |row| row.get(0),
        )?;

        let contacted: i64 = conn.query_row(
            "SELECT COUNT(DISTINCT c.lead_id)
             FROM messages m
             JOIN conversations c ON c.id = m.conversation_id
             WHERE m.direction='OUTBOUND'
               AND date(m.created_at, 'localtime') = date('now', 'localtime')",
            params![],
            |row| row.get(0),
        )?;

        let booked: i64 = conn.query_row(
            "SELECT COUNT(*) FROM appointments
             WHERE status='booked'
               AND date(created_at, 'localtime') = date('now', 'localtime')",
            params![],
            |row| row.get(0),
        )?;

        let opt_outs: i64 = conn.query_row(
            "SELECT COUNT(*) FROM audit_log
             WHERE action_type='set_opt_out'
               AND success=1
               AND date(created_at, 'localtime') = date('now', 'localtime')",
            params![],
            |row| row.get(0),
        )?;

        let needs_attention: i64 = conn.query_row(
            "SELECT COUNT(*) FROM leads WHERE needs_staff_attention=1",
            params![],
            |row| row.get(0),
        )?;

        Ok(TodayReport {
            leads_created,
            contacted,
            booked,
            opt_outs,
            needs_attention,
        })
    });

    map_cmd_result(result, "get_today_report", &app)
}

#[tauri::command]
fn get_kill_switch(state: State<AppState>, app: AppHandle) -> Result<bool, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        is_kill_switch_enabled(&conn)
    });

    map_cmd_result(result, "get_kill_switch", &app)
}

#[tauri::command]
fn set_kill_switch(state: State<AppState>, app: AppHandle, enabled: bool) -> Result<(), String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;
        let now = now_iso();

        conn.execute(
            "INSERT INTO settings (key, value, updated_at)
             VALUES ('kill_switch', ?, ?)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            params![if enabled { "true" } else { "false" }, now],
        )?;

        let _ = insert_audit(
            &conn,
            "set_kill_switch",
            "settings",
            Some("kill_switch".to_string()),
            json!({ "enabled": enabled }),
            Some(json!({ "updated_at": now })),
            true,
            None,
        );

        if enabled {
            let gateway = ActionGateway::new(&conn, &location);
            gateway.cancel_jobs_on_kill_switch()?;
        }

        Ok(())
    });

    map_cmd_result(result, "set_kill_switch", &app)
}

#[tauri::command]
fn run_due_jobs(state: State<AppState>, app: AppHandle) -> Result<RunJobsResult, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;

        if is_kill_switch_enabled(&conn)? {
            let skipped: i64 = conn.query_row(
                "SELECT COUNT(*) FROM scheduled_jobs
                 WHERE status='pending' AND datetime(execute_at) <= datetime('now')",
                params![],
                |row| row.get(0),
            )?;
            return Ok(RunJobsResult {
                processed: 0,
                skipped,
                errors: 0,
            });
        }

        let mut stmt = conn.prepare(
            "SELECT id, job_type, target_id, execute_at, payload_json
             FROM scheduled_jobs
             WHERE status='pending' AND datetime(execute_at) <= datetime('now')
             ORDER BY datetime(execute_at) ASC",
        )?;

        let mut jobs: Vec<(i64, String, Option<i64>, String, String)> = Vec::new();
        let mapped = stmt.query_map(params![], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;
        for item in mapped {
            jobs.push(item?);
        }

        let mut processed = 0;
        let mut skipped = 0;
        let mut errors = 0;

        for (job_id, job_type, target_id, _execute_at, payload_json) in jobs {
            if is_kill_switch_enabled(&conn)? {
                skipped += 1;
                continue;
            }

            let run_result = match job_type.as_str() {
                "initial_follow_up" => {
                    let payload: InitialFollowUpPayload = serde_json::from_str(&payload_json)?;
                    execute_initial_follow_up(&conn, &location, payload.lead_id)
                }
                "appointment_reminder" => {
                    let payload: ReminderPayload = serde_json::from_str(&payload_json)?;
                    execute_appointment_reminder(&conn, &location, payload)
                }
                _ => Err(AppError::Validation(format!(
                    "unknown job_type: {job_type}"
                ))),
            };

            match run_result {
                Ok(()) => {
                    processed += 1;
                    conn.execute(
                        "UPDATE scheduled_jobs SET status='completed' WHERE id=?",
                        params![job_id],
                    )?;
                }
                Err(err) => {
                    errors += 1;
                    conn.execute(
                        "UPDATE scheduled_jobs SET status='failed' WHERE id=?",
                        params![job_id],
                    )?;
                    let _ = insert_audit(
                        &conn,
                        "run_scheduled_job",
                        "scheduled_job",
                        Some(job_id.to_string()),
                        json!({
                            "job_type": job_type,
                            "target_id": target_id,
                            "payload_json": payload_json
                        }),
                        None,
                        false,
                        Some(err.to_string()),
                    );
                }
            }
        }

        Ok(RunJobsResult {
            processed,
            skipped,
            errors,
        })
    });

    map_cmd_result(result, "run_due_jobs", &app)
}

#[tauri::command]
fn agent_dry_run(
    state: State<AppState>,
    app: AppHandle,
    req: AgentDryRunRequest,
) -> Result<AgentDryRunResult, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;
        let gateway = ActionGateway::new(&conn, &location);

        let validation = match &req.action {
            AgentAction::SendOutbound {
                lead_id,
                conversation_id,
                body,
                automated,
                allow_without_consent,
                allow_opted_out_once,
                allow_after_reply,
                ignore_business_hours,
            } => gateway.validate_agent_outbound(&OutboundRequest {
                lead_id: *lead_id,
                conversation_id: *conversation_id,
                body: body.clone(),
                automated: *automated,
                allow_without_consent: *allow_without_consent,
                allow_opted_out_once: *allow_opted_out_once,
                allow_after_reply: *allow_after_reply,
                ignore_business_hours: *ignore_business_hours,
            }),
            AgentAction::BookAppointment {
                lead_id,
                start_at,
                end_at,
                status,
            } => gateway.validate_appointment(&AppointmentRequest {
                lead_id: *lead_id,
                start_at: start_at.clone(),
                end_at: end_at.clone(),
                status: status.clone(),
            }),
            AgentAction::SetOptOut { lead_id, reason } => {
                gateway.validate_opt_out(&OptOutRequest {
                    lead_id: *lead_id,
                    reason: reason.clone(),
                })
            }
            AgentAction::ScheduleJob {
                job_type,
                target_id,
                execute_at,
                payload_json,
            } => gateway.validate_schedule_job(&ScheduleJobRequest {
                job_type: job_type.clone(),
                target_id: *target_id,
                execute_at: execute_at.clone(),
                payload_json: payload_json.clone(),
            }),
        };

        let normalized = Some(serde_json::to_value(&req.action)?);
        let response = match validation {
            Ok(()) => AgentDryRunResult {
                allowed: true,
                blocked_reason: None,
                warnings: Vec::new(),
                normalized,
            },
            Err(err) => AgentDryRunResult {
                allowed: false,
                blocked_reason: Some(err.to_string()),
                warnings: Vec::new(),
                normalized,
            },
        };

        let _ = insert_audit(
            &conn,
            "agent_dry_run",
            "agent_action",
            Some(req.action.action_type().as_str().to_string()),
            serde_json::to_value(&req)?,
            Some(json!({
                "allowed": response.allowed,
                "blocked_reason": response.blocked_reason,
                "warnings": response.warnings
            })),
            response.allowed,
            response.blocked_reason.clone(),
        );

        Ok(response)
    });

    map_cmd_result(result, "agent_dry_run", &app)
}

#[tauri::command]
fn agent_execute(
    state: State<AppState>,
    app: AppHandle,
    req: AgentExecuteRequest,
) -> Result<AgentExecuteResult, String> {
    let result = retry_db(|| {
        let conn = open_conn(&state)?;
        let location = get_location(&conn)?;
        let gateway = ActionGateway::new(&conn, &location);

        let execution = match &req.action {
            AgentAction::SendOutbound {
                lead_id,
                conversation_id,
                body,
                automated,
                allow_without_consent,
                allow_opted_out_once,
                allow_after_reply,
                ignore_business_hours,
            } => gateway
                .create_outbound_message_for_agent(OutboundRequest {
                    lead_id: *lead_id,
                    conversation_id: *conversation_id,
                    body: body.clone(),
                    automated: *automated,
                    allow_without_consent: *allow_without_consent,
                    allow_opted_out_once: *allow_opted_out_once,
                    allow_after_reply: *allow_after_reply,
                    ignore_business_hours: *ignore_business_hours,
                })
                .map(|message_id| json!({ "message_id": message_id })),
            AgentAction::BookAppointment {
                lead_id,
                start_at,
                end_at,
                status,
            } => gateway
                .create_appointment(AppointmentRequest {
                    lead_id: *lead_id,
                    start_at: start_at.clone(),
                    end_at: end_at.clone(),
                    status: status.clone(),
                })
                .map(|appointment_id| json!({ "appointment_id": appointment_id })),
            AgentAction::SetOptOut { lead_id, reason } => gateway
                .set_opt_out(OptOutRequest {
                    lead_id: *lead_id,
                    reason: reason.clone(),
                })
                .map(|_| json!({ "result": "opted_out" })),
            AgentAction::ScheduleJob {
                job_type,
                target_id,
                execute_at,
                payload_json,
            } => gateway
                .schedule_job(ScheduleJobRequest {
                    job_type: job_type.clone(),
                    target_id: *target_id,
                    execute_at: execute_at.clone(),
                    payload_json: payload_json.clone(),
                })
                .map(|job_id| json!({ "job_id": job_id })),
        };

        let response = match execution {
            Ok(result_json) => AgentExecuteResult {
                success: true,
                result_json: Some(result_json),
                error: None,
            },
            Err(err) => AgentExecuteResult {
                success: false,
                result_json: None,
                error: Some(err.to_string()),
            },
        };

        Ok(response)
    });

    map_cmd_result(result, "agent_execute", &app)
}

#[tauri::command]
fn log_client_error(
    _state: State<AppState>,
    app: AppHandle,
    message: String,
    stack: Option<String>,
    source: String,
) -> Result<(), String> {
    let app_dir = ensure_app_data_dir(&app)?;
    let log_path = app_dir.join("client_errors.log");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|err| format!("failed to open {}: {err}", log_path.display()))?;

    writeln!(file, "timestamp: {}", now_iso())
        .map_err(|err| format!("failed to write client error timestamp: {err}"))?;
    writeln!(file, "source: {}", source)
        .map_err(|err| format!("failed to write client error source: {err}"))?;
    writeln!(file, "message: {}", message)
        .map_err(|err| format!("failed to write client error message: {err}"))?;
    if let Some(stack_text) = stack.filter(|s| !s.trim().is_empty()) {
        writeln!(file, "stack:")
            .map_err(|err| format!("failed to write client error stack header: {err}"))?;
        writeln!(file, "{stack_text}")
            .map_err(|err| format!("failed to write client error stack: {err}"))?;
    }
    writeln!(file).map_err(|err| format!("failed to finish client error log line: {err}"))?;

    Ok(())
}

#[tauri::command]
fn open_devtools(app: AppHandle) -> Result<(), String> {
    let window = app
        .get_window("main")
        .ok_or_else(|| "main window not found".to_string())?;
    window.open_devtools();
    Ok(())
}

fn execute_initial_follow_up(
    conn: &Connection,
    location: &Location,
    lead_id: i64,
) -> AppResult<()> {
    let lead = get_lead(conn, lead_id)?;
    let conversation = get_conversation_by_lead_id(conn, lead_id)?;
    let gateway = ActionGateway::new(conn, location);

    let display_name = lead
        .first_name
        .clone()
        .unwrap_or_else(|| "there".to_string());

    gateway.create_outbound_message(OutboundRequest {
        lead_id,
        conversation_id: conversation.id,
        body: format!(
            "Hi {display_name}, this is {}. Reply YES to see two available intro session times.",
            location.gym_name
        ),
        automated: true,
        allow_without_consent: false,
        allow_opted_out_once: false,
        allow_after_reply: false,
        ignore_business_hours: false,
    })?;

    conn.execute(
        "UPDATE leads SET next_action_at=NULL WHERE id=?",
        params![lead_id],
    )?;
    Ok(())
}

fn execute_appointment_reminder(
    conn: &Connection,
    location: &Location,
    payload: ReminderPayload,
) -> AppResult<()> {
    let lead = get_lead(conn, payload.lead_id)?;
    let conversation = get_conversation_by_lead_id(conn, payload.lead_id)?;
    let gateway = ActionGateway::new(conn, location);

    let local_start = local_display(location, &payload.start_at)?;
    let display_name = lead
        .first_name
        .clone()
        .unwrap_or_else(|| "there".to_string());

    gateway.create_outbound_message(OutboundRequest {
        lead_id: payload.lead_id,
        conversation_id: conversation.id,
        body: format!(
            "Reminder {display_name}: your gym appointment is at {local_start}. Reply STOP to opt out."
        ),
        automated: true,
        allow_without_consent: false,
        allow_opted_out_once: false,
        allow_after_reply: false,
        ignore_business_hours: false,
    })?;

    Ok(())
}

fn process_inbound_state_machine(
    conn: &Connection,
    location: &Location,
    lead: &LeadRow,
    conversation: &ConversationRow,
    inbound_body: &str,
) -> AppResult<()> {
    let gateway = ActionGateway::new(conn, location);
    let normalized = inbound_body.trim().to_ascii_uppercase();
    let now = Utc::now();

    if normalized == "STOP" || normalized == "UNSUBSCRIBE" {
        gateway.set_opt_out(OptOutRequest {
            lead_id: lead.id,
            reason: "lead sent stop keyword".to_string(),
        })?;

        gateway.create_outbound_message(OutboundRequest {
            lead_id: lead.id,
            conversation_id: conversation.id,
            body: "You are unsubscribed and will receive no more automated messages.".to_string(),
            automated: false,
            allow_without_consent: true,
            allow_opted_out_once: true,
            allow_after_reply: true,
            ignore_business_hours: true,
        })?;

        return Ok(());
    }

    if lead.opted_out {
        return Ok(());
    }

    if let Some(last_outbound_at) = &conversation.last_outbound_at {
        let last_outbound = parse_ts(last_outbound_at)?;
        if now.signed_duration_since(last_outbound) >= Duration::hours(24) {
            gateway.create_outbound_message(OutboundRequest {
                lead_id: lead.id,
                conversation_id: conversation.id,
                body: "Reply YES to get the next two available intro session times.".to_string(),
                automated: false,
                allow_without_consent: false,
                allow_opted_out_once: false,
                allow_after_reply: true,
                ignore_business_hours: true,
            })?;

            let state = serde_json::to_string(&ConversationState::default())?;
            conn.execute(
                "UPDATE conversations SET state='awaiting_yes', state_json=?, repair_attempts=0 WHERE id=?",
                params![state, conversation.id],
            )?;
            conn.execute(
                "UPDATE leads SET status='awaiting_yes' WHERE id=?",
                params![lead.id],
            )?;
            return Ok(());
        }
    }

    match conversation.state.as_str() {
        "awaiting_yes" => {
            if normalized == "YES" || normalized == "Y" {
                let offered = generate_slot_choices(conn, location, now)?;
                if offered.len() < 2 {
                    flag_needs_staff_attention(conn, lead.id, "no_slots_available")?;
                    gateway.create_outbound_message(OutboundRequest {
                        lead_id: lead.id,
                        conversation_id: conversation.id,
                        body: "I couldn't find two matching slots right now. A staff member will follow up shortly."
                            .to_string(),
                        automated: false,
                        allow_without_consent: false,
                        allow_opted_out_once: false,
                        allow_after_reply: true,
                        ignore_business_hours: true,
                    })?;
                    return Ok(());
                }

                let state = ConversationState {
                    offered_slots: offered.clone(),
                };
                conn.execute(
                    "UPDATE conversations SET state='awaiting_time_choice', state_json=?, repair_attempts=0 WHERE id=?",
                    params![serde_json::to_string(&state)?, conversation.id],
                )?;
                conn.execute(
                    "UPDATE leads SET status='awaiting_time_choice' WHERE id=?",
                    params![lead.id],
                )?;

                gateway.create_outbound_message(OutboundRequest {
                    lead_id: lead.id,
                    conversation_id: conversation.id,
                    body: format_slot_offer(location, &offered)?,
                    automated: false,
                    allow_without_consent: false,
                    allow_opted_out_once: false,
                    allow_after_reply: true,
                    ignore_business_hours: true,
                })?;
            } else {
                gateway.create_outbound_message(OutboundRequest {
                    lead_id: lead.id,
                    conversation_id: conversation.id,
                    body: "Reply YES to get the next two available intro session times."
                        .to_string(),
                    automated: false,
                    allow_without_consent: false,
                    allow_opted_out_once: false,
                    allow_after_reply: true,
                    ignore_business_hours: true,
                })?;
            }
        }
        "awaiting_time_choice" => {
            let state: ConversationState =
                serde_json::from_str(&conversation.state_json).unwrap_or_default();
            if normalized == "1" || normalized == "2" {
                let index = if normalized == "1" { 0 } else { 1 };
                if let Some(slot) = state.offered_slots.get(index).cloned() {
                    let appointment_id = gateway.create_appointment(AppointmentRequest {
                        lead_id: lead.id,
                        start_at: slot.start_at.clone(),
                        end_at: slot.end_at.clone(),
                        status: "booked".to_string(),
                    })?;

                    conn.execute(
                        "UPDATE conversations SET state='booked', repair_attempts=0, state_json=? WHERE id=?",
                        params![serde_json::to_string(&ConversationState::default())?, conversation.id],
                    )?;

                    let local_slot = local_display(location, &slot.start_at)?;
                    gateway.create_outbound_message(OutboundRequest {
                        lead_id: lead.id,
                        conversation_id: conversation.id,
                        body: format!(
                            "Booked. Your intro session is confirmed for {local_slot}. We will send a reminder 2 hours before."
                        ),
                        automated: false,
                        allow_without_consent: false,
                        allow_opted_out_once: false,
                        allow_after_reply: true,
                        ignore_business_hours: true,
                    })?;

                    let reminder_at = parse_ts(&slot.start_at)? - Duration::hours(2);
                    if reminder_at > Utc::now() {
                        let _ = gateway.schedule_job(ScheduleJobRequest {
                            job_type: "appointment_reminder".to_string(),
                            target_id: Some(appointment_id),
                            execute_at: reminder_at.to_rfc3339(),
                            payload_json: serde_json::to_string(&ReminderPayload {
                                lead_id: lead.id,
                                appointment_id,
                                start_at: slot.start_at,
                            })?,
                        });
                    }
                } else {
                    handle_time_choice_repair(conn, location, lead, conversation)?;
                }
            } else {
                handle_time_choice_repair(conn, location, lead, conversation)?;
            }
        }
        "booked" => {
            gateway.create_outbound_message(OutboundRequest {
                lead_id: lead.id,
                conversation_id: conversation.id,
                body: "You're already booked. Reply if you need staff help rescheduling."
                    .to_string(),
                automated: false,
                allow_without_consent: false,
                allow_opted_out_once: false,
                allow_after_reply: true,
                ignore_business_hours: true,
            })?;
        }
        _ => {
            conn.execute(
                "UPDATE conversations SET state='awaiting_yes', state_json=?, repair_attempts=0 WHERE id=?",
                params![serde_json::to_string(&ConversationState::default())?, conversation.id],
            )?;
            gateway.create_outbound_message(OutboundRequest {
                lead_id: lead.id,
                conversation_id: conversation.id,
                body: "Reply YES to get the next two available intro session times.".to_string(),
                automated: false,
                allow_without_consent: false,
                allow_opted_out_once: false,
                allow_after_reply: true,
                ignore_business_hours: true,
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn test_execute_initial_follow_up(conn: &Connection, lead_id: i64) -> Result<(), String> {
    let location = get_location(conn).map_err(|err| err.to_string())?;
    execute_initial_follow_up(conn, &location, lead_id).map_err(|err| err.to_string())
}

#[cfg(test)]
pub(crate) fn test_process_inbound_state_machine(
    conn: &Connection,
    lead_id: i64,
    inbound_body: &str,
) -> Result<(), String> {
    let body = inbound_body.trim();
    if body.is_empty() {
        return Err("inbound body cannot be empty".to_string());
    }

    let location = get_location(conn).map_err(|err| err.to_string())?;
    let conversation = get_conversation_by_lead_id(conn, lead_id).map_err(|err| err.to_string())?;

    let now = now_iso();
    conn.execute(
        "INSERT INTO messages (conversation_id, direction, body, status, created_at)
         VALUES (?, 'INBOUND', ?, 'received', ?)",
        params![conversation.id, body, now],
    )
    .map_err(|err| err.to_string())?;
    conn.execute(
        "UPDATE conversations SET last_inbound_at=? WHERE id=?",
        params![now, conversation.id],
    )
    .map_err(|err| err.to_string())?;
    conn.execute(
        "UPDATE leads SET last_contact_at=? WHERE id=?",
        params![now, lead_id],
    )
    .map_err(|err| err.to_string())?;

    let lead = get_lead(conn, lead_id).map_err(|err| err.to_string())?;
    let refreshed_conversation =
        get_conversation_by_lead_id(conn, lead_id).map_err(|err| err.to_string())?;

    process_inbound_state_machine(conn, &location, &lead, &refreshed_conversation, body)
        .map_err(|err| err.to_string())
}

fn handle_time_choice_repair(
    conn: &Connection,
    location: &Location,
    lead: &LeadRow,
    conversation: &ConversationRow,
) -> AppResult<()> {
    let gateway = ActionGateway::new(conn, location);
    let attempts = conversation.repair_attempts + 1;
    let offered = generate_slot_choices(conn, location, Utc::now())?;

    if offered.len() < 2 {
        flag_needs_staff_attention(conn, lead.id, "repair_no_slots")?;
        gateway.create_outbound_message(OutboundRequest {
            lead_id: lead.id,
            conversation_id: conversation.id,
            body:
                "I couldn't match that response to a slot. A staff member has been flagged to help."
                    .to_string(),
            automated: false,
            allow_without_consent: false,
            allow_opted_out_once: false,
            allow_after_reply: true,
            ignore_business_hours: true,
        })?;
        return Ok(());
    }

    let mut body = format!(
        "Please reply with 1 or 2 so I can book your session.\n\n{}",
        format_slot_offer(location, &offered)?
    );

    if attempts >= 2 {
        flag_needs_staff_attention(conn, lead.id, "repair_attempts_exceeded")?;
        body = format!(
            "{}\n\nI also flagged this conversation for staff follow-up.",
            body
        );
    }

    conn.execute(
        "UPDATE conversations SET state='awaiting_time_choice', state_json=?, repair_attempts=? WHERE id=?",
        params![serde_json::to_string(&ConversationState { offered_slots: offered })?, attempts, conversation.id],
    )?;

    gateway.create_outbound_message(OutboundRequest {
        lead_id: lead.id,
        conversation_id: conversation.id,
        body,
        automated: false,
        allow_without_consent: false,
        allow_opted_out_once: false,
        allow_after_reply: true,
        ignore_business_hours: true,
    })?;

    Ok(())
}

fn flag_needs_staff_attention(conn: &Connection, lead_id: i64, reason: &str) -> AppResult<()> {
    conn.execute(
        "UPDATE leads SET needs_staff_attention=1 WHERE id=?",
        params![lead_id],
    )?;
    let _ = insert_audit(
        conn,
        "flag_needs_staff_attention",
        "lead",
        Some(lead_id.to_string()),
        json!({ "reason": reason }),
        Some(json!({ "needs_staff_attention": true })),
        true,
        None,
    );
    Ok(())
}

fn generate_slot_choices(
    conn: &Connection,
    location: &Location,
    from_utc: DateTime<Utc>,
) -> AppResult<Vec<SlotChoice>> {
    let tz = parse_tz(&location.timezone)?;
    let business_hours = parse_business_hours(&location.business_hours_json)?;

    let mut appointments_stmt = conn.prepare(
        "SELECT start_at, end_at FROM appointments WHERE status='booked' AND datetime(start_at) >= datetime('now', '-1 day')",
    )?;
    let appt_rows = appointments_stmt.query_map(params![], |row| {
        let start: String = row.get(0)?;
        let end: String = row.get(1)?;
        Ok((start, end))
    })?;

    let mut existing: Vec<(DateTime<Utc>, DateTime<Utc>)> = Vec::new();
    for row in appt_rows {
        let (start, end) = row?;
        existing.push((parse_ts(&start)?, parse_ts(&end)?));
    }

    let local_start = from_utc.with_timezone(&tz);
    let mut business_days_seen = 0;
    let mut day_offset = 0;
    let mut slots: Vec<SlotChoice> = Vec::new();

    while business_days_seen < 3 && day_offset < 14 {
        let day: NaiveDate = local_start.date_naive() + Duration::days(day_offset);
        let weekday = day.weekday();
        let ranges = business_hours.get(&weekday).cloned().unwrap_or_default();

        if !ranges.is_empty() {
            business_days_seen += 1;
            for (range_start, range_end) in ranges {
                let mut current_minutes =
                    range_start.hour() as i64 * 60 + range_start.minute() as i64;
                let end_minutes = range_end.hour() as i64 * 60 + range_end.minute() as i64;

                while current_minutes + 30 <= end_minutes {
                    let hour = (current_minutes / 60) as u32;
                    let minute = (current_minutes % 60) as u32;
                    let naive_time = NaiveTime::from_hms_opt(hour, minute, 0)
                        .ok_or_else(|| AppError::Validation("invalid time computed".to_string()))?;
                    let local_candidate = tz
                        .from_local_datetime(&day.and_time(naive_time))
                        .single()
                        .ok_or_else(|| {
                            AppError::Validation(
                                "could not resolve local appointment slot timestamp".to_string(),
                            )
                        })?;
                    let start_utc = local_candidate.with_timezone(&Utc);
                    let end_utc = start_utc + Duration::minutes(30);

                    if start_utc <= from_utc {
                        current_minutes += 40;
                        continue;
                    }

                    if !has_appointment_conflict(start_utc, end_utc, &existing) {
                        slots.push(SlotChoice {
                            start_at: start_utc.to_rfc3339(),
                            end_at: end_utc.to_rfc3339(),
                        });
                    }

                    if slots.len() == 2 {
                        return Ok(slots);
                    }

                    current_minutes += 40;
                }
            }
        }

        day_offset += 1;
    }

    Ok(slots)
}

fn has_appointment_conflict(
    candidate_start: DateTime<Utc>,
    candidate_end: DateTime<Utc>,
    existing: &[(DateTime<Utc>, DateTime<Utc>)],
) -> bool {
    let candidate_end_with_buffer = candidate_end + Duration::minutes(10);

    existing.iter().any(|(start, end)| {
        let existing_end_with_buffer = *end + Duration::minutes(10);
        candidate_start < existing_end_with_buffer && *start < candidate_end_with_buffer
    })
}

fn format_slot_offer(location: &Location, slots: &[SlotChoice]) -> AppResult<String> {
    if slots.len() < 2 {
        return Err(AppError::Validation(
            "expected at least 2 slots for offer".to_string(),
        ));
    }

    let first = local_display(location, &slots[0].start_at)?;
    let second = local_display(location, &slots[1].start_at)?;

    Ok(format!(
        "Choose a time:\n1) {first}\n2) {second}\n\nReply 1 or 2."
    ))
}

fn local_display(location: &Location, iso: &str) -> AppResult<String> {
    let tz = parse_tz(&location.timezone)?;
    let dt = parse_ts(iso)?.with_timezone(&tz);
    Ok(dt.format("%a %b %-d at %-I:%M %p").to_string())
}

fn parse_business_hours(input: &str) -> AppResult<HashMap<Weekday, Vec<(NaiveTime, NaiveTime)>>> {
    let raw: Value = serde_json::from_str(input)?;
    let obj = raw
        .as_object()
        .ok_or_else(|| AppError::Validation("invalid business_hours_json object".to_string()))?;

    let mut map: HashMap<Weekday, Vec<(NaiveTime, NaiveTime)>> = HashMap::new();

    for (key, value) in obj {
        let weekday = match key.as_str() {
            "mon" => Weekday::Mon,
            "tue" => Weekday::Tue,
            "wed" => Weekday::Wed,
            "thu" => Weekday::Thu,
            "fri" => Weekday::Fri,
            "sat" => Weekday::Sat,
            "sun" => Weekday::Sun,
            _ => continue,
        };

        let mut ranges: Vec<(NaiveTime, NaiveTime)> = Vec::new();
        if let Some(arr) = value.as_array() {
            for range in arr {
                let pair = range.as_array().ok_or_else(|| {
                    AppError::Validation("business hours range must be [start, end]".to_string())
                })?;
                if pair.len() != 2 {
                    continue;
                }
                let start = pair[0]
                    .as_str()
                    .ok_or_else(|| AppError::Validation("start time must be string".to_string()))?;
                let end = pair[1]
                    .as_str()
                    .ok_or_else(|| AppError::Validation("end time must be string".to_string()))?;

                let start_time = NaiveTime::parse_from_str(start, "%H:%M")?;
                let end_time = NaiveTime::parse_from_str(end, "%H:%M")?;
                ranges.push((start_time, end_time));
            }
        }
        map.insert(weekday, ranges);
    }

    Ok(map)
}

fn is_business_open(location: &Location, when_utc: DateTime<Utc>) -> AppResult<bool> {
    let tz = parse_tz(&location.timezone)?;
    let local = when_utc.with_timezone(&tz);
    let business_hours = parse_business_hours(&location.business_hours_json)?;
    let ranges = business_hours
        .get(&local.weekday())
        .cloned()
        .unwrap_or_default();

    let current_time = local.time();
    Ok(ranges
        .iter()
        .any(|(start, end)| current_time >= *start && current_time < *end))
}

fn next_open_time(location: &Location, from_utc: DateTime<Utc>) -> AppResult<DateTime<Utc>> {
    let tz = parse_tz(&location.timezone)?;
    let local = from_utc.with_timezone(&tz);
    let business_hours = parse_business_hours(&location.business_hours_json)?;

    for day_offset in 0..21 {
        let day = local.date_naive() + Duration::days(day_offset);
        let ranges = business_hours
            .get(&day.weekday())
            .cloned()
            .unwrap_or_default();
        for (start, _) in ranges {
            if day_offset == 0 && start <= local.time() {
                continue;
            }
            let local_dt = tz
                .from_local_datetime(&day.and_time(start))
                .single()
                .ok_or_else(|| {
                    AppError::Validation(
                        "unable to resolve local datetime for next business opening".to_string(),
                    )
                })?;
            return Ok(local_dt.with_timezone(&Utc));
        }
    }

    Ok(from_utc + Duration::hours(24))
}

fn get_location(conn: &Connection) -> AppResult<Location> {
    conn.query_row(
        "SELECT id, gym_name, timezone, business_hours_json FROM locations ORDER BY id LIMIT 1",
        params![],
        |row| {
            Ok(Location {
                id: row.get(0)?,
                gym_name: row.get(1)?,
                timezone: row.get(2)?,
                business_hours_json: row.get(3)?,
            })
        },
    )
    .map_err(AppError::from)
}

fn get_lead(conn: &Connection, lead_id: i64) -> AppResult<LeadRow> {
    conn.query_row(
        "SELECT id, phone_e164, first_name, last_name, consent, opted_out, status, needs_staff_attention
         FROM leads WHERE id=?",
        params![lead_id],
        |row| {
            Ok(LeadRow {
                id: row.get(0)?,
                phone_e164: row.get(1)?,
                first_name: row.get(2)?,
                last_name: row.get(3)?,
                consent: i64_to_bool(row.get(4)?),
                opted_out: i64_to_bool(row.get(5)?),
                status: row.get(6)?,
                needs_staff_attention: i64_to_bool(row.get(7)?),
            })
        },
    )
    .optional()?
    .ok_or_else(|| AppError::Validation("lead not found".to_string()))
}

fn get_conversation_by_lead_id(conn: &Connection, lead_id: i64) -> AppResult<ConversationRow> {
    conn.query_row(
        "SELECT id, lead_id, state, state_json, last_inbound_at, last_outbound_at, repair_attempts
         FROM conversations WHERE lead_id=?",
        params![lead_id],
        |row| {
            Ok(ConversationRow {
                id: row.get(0)?,
                lead_id: row.get(1)?,
                state: row.get(2)?,
                state_json: row.get(3)?,
                last_inbound_at: row.get(4)?,
                last_outbound_at: row.get(5)?,
                repair_attempts: row.get(6)?,
            })
        },
    )
    .map_err(AppError::from)
}

fn is_kill_switch_enabled(conn: &Connection) -> AppResult<bool> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM settings WHERE key='kill_switch' LIMIT 1",
            params![],
            |row| row.get(0),
        )
        .optional()?;

    Ok(matches!(raw.as_deref(), Some("true") | Some("1")))
}

fn insert_audit(
    conn: &Connection,
    action_type: &str,
    target_type: &str,
    target_id: Option<String>,
    request_json: Value,
    response_json: Option<Value>,
    success: bool,
    error_message: Option<String>,
) -> AppResult<()> {
    conn.execute(
        "INSERT INTO audit_log (action_type, target_type, target_id, request_json, response_json, success, error_message, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            action_type,
            target_type,
            target_id,
            serde_json::to_string(&request_json)?,
            response_json.map(|v| serde_json::to_string(&v)).transpose()?,
            bool_to_i64(success),
            error_message,
            now_iso()
        ],
    )?;
    Ok(())
}

fn open_conn(state: &State<AppState>) -> AppResult<Connection> {
    let conn = Connection::open(&state.db_path)?;
    conn.busy_timeout(StdDuration::from_millis(500))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

fn initialize_db(db_path: &Path) -> AppResult<()> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent).map_err(|e| AppError::Validation(e.to_string()))?;
    }

    let conn = Connection::open(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.execute_batch(include_str!("../migrations/001_init.sql"))?;

    let location_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM locations", params![], |row| {
            row.get(0)
        })?;
    if location_count == 0 {
        conn.execute(
            "INSERT INTO locations (gym_name, timezone, business_hours_json) VALUES (?, ?, ?)",
            params![
                "Demo Gym Downtown",
                "America/New_York",
                default_business_hours_json()
            ],
        )?;
    }

    conn.execute(
        "INSERT INTO settings (key, value, updated_at)
         VALUES ('kill_switch', 'false', ?)
         ON CONFLICT(key) DO NOTHING",
        params![now_iso()],
    )?;

    Ok(())
}

fn ensure_app_data_dir(app: &AppHandle) -> Result<PathBuf, String> {
    let app_dir = app
        .path_resolver()
        .app_local_data_dir()
        .ok_or_else(|| "failed to resolve app local data dir".to_string())?;
    fs::create_dir_all(&app_dir).map_err(|err| format!("failed to create app data dir: {err}"))?;
    Ok(app_dir)
}

fn map_cmd_result<T: Serialize>(
    result: AppResult<T>,
    action_name: &str,
    app: &AppHandle,
) -> Result<T, String> {
    match result {
        Ok(value) => Ok(value),
        Err(err) => {
            let message = format!("Alert: {err}");
            log_command_failure(app, action_name, &message);
            Err(message)
        }
    }
}

fn log_command_failure(app: &AppHandle, action_name: &str, message: &str) {
    if let Some(state) = app.try_state::<AppState>() {
        if let Ok(conn) = Connection::open(&state.db_path) {
            let _ = insert_audit(
                &conn,
                action_name,
                "command",
                None,
                json!({ "action": action_name }),
                None,
                false,
                Some(message.to_string()),
            );
        }
    }
}

fn retry_db<T, F>(mut f: F) -> AppResult<T>
where
    F: FnMut() -> AppResult<T>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match f() {
            Ok(value) => return Ok(value),
            Err(err) if err.is_busy_or_locked() && attempt < 5 => {
                thread::sleep(StdDuration::from_millis((attempt * 40) as u64));
            }
            Err(err) => return Err(err),
        }
    }
}

fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

fn parse_ts(input: &str) -> AppResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(input)?.with_timezone(&Utc))
}

fn bool_to_i64(v: bool) -> i64 {
    if v {
        1
    } else {
        0
    }
}

fn i64_to_bool(v: i64) -> bool {
    v != 0
}

fn parse_tz(tz_name: &str) -> AppResult<Tz> {
    tz_name
        .parse::<Tz>()
        .map_err(|_| AppError::Validation(format!("invalid timezone: {tz_name}")))
}

fn null_if_empty(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn default_business_hours_json() -> &'static str {
    r#"{"mon":[["09:00","17:00"]],"tue":[["09:00","17:00"]],"wed":[["09:00","17:00"]],"thu":[["09:00","17:00"]],"fri":[["09:00","17:00"]],"sat":[["10:00","14:00"]],"sun":[]}"#
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let app_dir = ensure_app_data_dir(&app.handle()).map_err(AppError::Validation)?;
            let db_path = app_dir.join("gym_lead_booker_demo.sqlite");
            initialize_db(&db_path)?;
            app.manage(AppState { db_path });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            create_lead,
            list_leads,
            search_leads,
            list_agent_queue,
            get_lead_detail,
            simulate_inbound_sms,
            get_today_report,
            get_kill_switch,
            set_kill_switch,
            log_client_error,
            open_devtools,
            run_due_jobs,
            agent_dry_run,
            agent_execute
        ])
        .run(tauri::generate_context!())
        .expect("error while running Gym Lead Booker app");
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_helpers::init_in_memory_db;

    fn ts(input: &str) -> DateTime<Utc> {
        parse_ts(input).expect("timestamp should parse")
    }

    fn set_business_hours(conn: &Connection, business_hours_json: &str) {
        conn.execute(
            "UPDATE locations SET business_hours_json=? WHERE id=1",
            params![business_hours_json],
        )
        .expect("failed to update location business hours");
    }

    fn insert_lead(conn: &Connection, phone_e164: &str) -> i64 {
        conn.execute(
            "INSERT INTO leads (phone_e164, consent, status, opted_out, needs_staff_attention, created_at)
             VALUES (?, 1, 'awaiting_yes', 0, 0, ?)",
            params![phone_e164, "2030-01-01T00:00:00Z"],
        )
        .expect("failed to insert test lead");
        conn.last_insert_rowid()
    }

    fn insert_booked_appointment(conn: &Connection, lead_id: i64, start_at: &str, end_at: &str) {
        conn.execute(
            "INSERT INTO appointments (lead_id, start_at, end_at, status, created_at)
             VALUES (?, ?, ?, 'booked', ?)",
            params![lead_id, start_at, end_at, "2030-01-01T00:00:00Z"],
        )
        .expect("failed to insert test appointment");
    }

    #[test]
    fn parse_business_hours_accepts_valid_json_with_multiple_ranges() {
        let _conn = init_in_memory_db();
        let parsed = parse_business_hours(
            r#"{"mon":[["09:00","12:00"],["13:00","17:00"]],"tue":[["10:00","11:30"]]}"#,
        )
        .expect("valid business hours json should parse");

        let mon_ranges = parsed.get(&Weekday::Mon).expect("missing monday ranges");
        assert_eq!(mon_ranges.len(), 2);
        assert_eq!(
            mon_ranges[0],
            (
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(12, 0, 0).unwrap()
            )
        );
        assert_eq!(
            mon_ranges[1],
            (
                NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap()
            )
        );
    }

    #[test]
    fn parse_business_hours_rejects_invalid_json() {
        let _conn = init_in_memory_db();
        assert!(parse_business_hours(r#"{"mon":[[123,"10:00"]]}"#).is_err());
    }

    #[test]
    fn has_appointment_conflict_detects_overlap_and_non_overlap() {
        let _conn = init_in_memory_db();
        let existing = vec![(ts("2030-01-07T14:00:00Z"), ts("2030-01-07T14:30:00Z"))];

        assert!(has_appointment_conflict(
            ts("2030-01-07T14:10:00Z"),
            ts("2030-01-07T14:40:00Z"),
            &existing
        ));
        assert!(!has_appointment_conflict(
            ts("2030-01-07T15:00:00Z"),
            ts("2030-01-07T15:30:00Z"),
            &existing
        ));
    }

    #[test]
    fn has_appointment_conflict_enforces_ten_minute_buffer() {
        let _conn = init_in_memory_db();
        let existing = vec![(ts("2030-01-07T14:00:00Z"), ts("2030-01-07T14:30:00Z"))];

        assert!(has_appointment_conflict(
            ts("2030-01-07T14:35:00Z"),
            ts("2030-01-07T15:05:00Z"),
            &existing
        ));
        assert!(!has_appointment_conflict(
            ts("2030-01-07T14:40:00Z"),
            ts("2030-01-07T15:10:00Z"),
            &existing
        ));
    }

    #[test]
    fn generate_slot_choices_returns_two_slots_when_exactly_two_are_available() {
        let conn = init_in_memory_db();
        set_business_hours(
            &conn,
            r#"{"mon":[["09:00","09:30"]],"tue":[["09:00","09:30"]],"wed":[["09:00","09:30"]],"thu":[],"fri":[],"sat":[],"sun":[]}"#,
        );
        let lead_id = insert_lead(&conn, "+15550000001");
        insert_booked_appointment(
            &conn,
            lead_id,
            "2030-01-08T14:00:00Z",
            "2030-01-08T14:30:00Z",
        );

        let location = get_location(&conn).expect("test location should exist");
        let slots =
            generate_slot_choices(&conn, &location, ts("2030-01-07T12:00:00Z")).unwrap();

        assert_eq!(slots.len(), 2);
        assert_eq!(parse_ts(&slots[0].start_at).unwrap(), ts("2030-01-07T14:00:00Z"));
        assert_eq!(parse_ts(&slots[1].start_at).unwrap(), ts("2030-01-09T14:00:00Z"));
    }

    #[test]
    fn generate_slot_choices_returns_fewer_than_two_when_capacity_is_limited() {
        let conn = init_in_memory_db();
        set_business_hours(
            &conn,
            r#"{"mon":[["09:00","09:30"]],"tue":[["09:00","09:30"]],"wed":[["09:00","09:30"]],"thu":[],"fri":[],"sat":[],"sun":[]}"#,
        );
        let lead_id = insert_lead(&conn, "+15550000002");
        insert_booked_appointment(
            &conn,
            lead_id,
            "2030-01-07T14:00:00Z",
            "2030-01-07T14:30:00Z",
        );
        insert_booked_appointment(
            &conn,
            lead_id,
            "2030-01-08T14:00:00Z",
            "2030-01-08T14:30:00Z",
        );

        let location = get_location(&conn).expect("test location should exist");
        let slots =
            generate_slot_choices(&conn, &location, ts("2030-01-07T12:00:00Z")).unwrap();

        assert_eq!(slots.len(), 1);
        assert_eq!(parse_ts(&slots[0].start_at).unwrap(), ts("2030-01-09T14:00:00Z"));
    }

    #[test]
    fn business_open_and_next_open_time_respect_open_close_edges() {
        let conn = init_in_memory_db();
        set_business_hours(
            &conn,
            r#"{"mon":[["09:00","17:00"]],"tue":[["09:00","17:00"]],"wed":[["09:00","17:00"]],"thu":[["09:00","17:00"]],"fri":[["09:00","17:00"]],"sat":[],"sun":[]}"#,
        );
        let location = get_location(&conn).expect("test location should exist");

        assert!(!is_business_open(&location, ts("2030-01-07T13:59:00Z")).unwrap());
        assert!(is_business_open(&location, ts("2030-01-07T14:00:00Z")).unwrap());
        assert!(is_business_open(&location, ts("2030-01-07T21:59:00Z")).unwrap());
        assert!(!is_business_open(&location, ts("2030-01-07T22:00:00Z")).unwrap());

        assert_eq!(
            next_open_time(&location, ts("2030-01-07T13:59:00Z")).unwrap(),
            ts("2030-01-07T14:00:00Z")
        );
        assert_eq!(
            next_open_time(&location, ts("2030-01-07T22:00:00Z")).unwrap(),
            ts("2030-01-08T14:00:00Z")
        );
    }
}
