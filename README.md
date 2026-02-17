# Gold Bot

Locked-down desktop PoC built with **Tauri v1 + React + TypeScript + Rust + SQLite**.

This project is **local-only**:
- No Twilio
- No webhooks
- No outbound network dependencies at runtime
- "Sending SMS" is simulated by writing rows to `messages` and `audit_log`

## Core Capabilities

- Manual lead intake with fields:
  - `name` (`first_name`, `last_name`)
  - `phone_e164`
  - `consent` (`true/false`)
  - `consent_at`
  - `source`
- Deduplication:
  - Existing `phone_e164` within last 30 days is treated as duplicate
  - Automation is not restarted
  - A note is written to `audit_log`
- Auto-follow-up:
  - New consented lead during business hours schedules first outbound within 60 seconds
  - Outside business hours schedules at next open time
- Inbound simulation:
  - Lead Detail has "Simulate inbound SMS"
  - Submitting creates INBOUND `messages` row and runs state machine
- State machine:
  - `awaiting_yes -> awaiting_time_choice -> booked`
  - STOP/UNSUBSCRIBE immediately sets opt-out + logs + one confirmation outbound + silence afterward
  - Repair logic for non `1/2` responses while waiting on slot selection
  - If inbound reply is after 24h from last outbound, reset to safe prompt: "Reply YES..."
  - After 2 repair attempts, `needs_staff_attention=true`
- Booking:
  - Internal SQLite appointment booking
  - Offers 2 slots in next 3 business days
  - Slots are 30 minutes with 10-minute buffer
  - Picking `1` or `2` books appointment and sends confirmation
  - Reminder outbound is scheduled 2 hours before appointment
- Reporting (Today):
  - leads created
  - contacted
  - booked
  - opt-outs
  - needs-attention count
- Kill switch:
  - Global kill switch blocks all automated message creation immediately
  - Pending jobs are cancelled on enable
  - Due jobs do not execute while kill switch is enabled

## ActionGateway Contract

All side-effects are centralized in Rust `ActionGateway` (`/src-tauri/src/main.rs`):

- `create_outbound_message`
- `create_appointment`
- `set_opt_out`
- `schedule_job`
- `cancel_jobs_on_kill_switch`

Safety checks in gateway:
- consent required (unless explicitly exempted for compliance path)
- opt-out blocking
- business-hours check
- rate limits:
  - max 4 outbound / lead / day
  - max 100 outbound / location / hour
  - min 2 hours between outbound to same lead unless lead just replied
- every attempt writes `audit_log`

## Database

Migration SQL:
- `src-tauri/migrations/001_init.sql`

Tables:
- `locations`
- `leads`
- `conversations`
- `messages`
- `appointments`
- `audit_log`
- `settings`
- `scheduled_jobs`

## Install & Run

## 1) Prerequisites

- Rust toolchain + Cargo
- Node.js 18+
- Tauri v1 system dependencies (platform-specific)

## 2) Install JS deps

```bash
npm install
```

## 3) Run desktop app

```bash
npm run tauri dev
```

## 4) Build production bundle

```bash
npm run tauri build
```

## Run tests

```bash
cargo test --manifest-path src-tauri/Cargo.toml
```

## Local-Only Notes

- All message activity is local SQLite data.
- No external SMS providers are used.
- Scheduled jobs are executed by `run_due_jobs` command (UI auto-polls every 15s and also has manual trigger).
- DB file is created in the app local data directory as `db/goldbot.sqlite`.

## Error Handling

- SQLite busy/locked retries up to 5 attempts with short backoff.
- Failures are written to `audit_log`.
- UI displays command failures as small alert text.

## Frontend Views

- Today metrics dashboard
- Kill switch controls
- Lead intake form
- Lead list
- Lead detail with conversation history
- Inbound SMS simulator

## Security / Local Constraints

- No webhook endpoints
- No external API calls for messaging
- No hidden side-effects outside `ActionGateway` for outbound, appointments, opt-out, and scheduling operations
