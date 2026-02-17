# Manual Test Checklist (Demo Mode)

1. Create lead with consent during business hours.
   Expected: lead is created, conversation is `awaiting_yes`, `scheduled_jobs` contains `initial_follow_up` due within ~60s.

2. Run due jobs (or wait for poller).
   Expected: one OUTBOUND message is created and corresponding `audit_log` entry exists.

3. Create lead with consent outside business hours.
   Expected: no immediate outbound; `scheduled_jobs.execute_at` is next business open.

4. Create duplicate lead using same `phone_e164` within 30 days.
   Expected: no new lead row, automation not restarted, UI note shown, `audit_log` note present.

5. In lead detail, simulate inbound `YES` in `awaiting_yes`.
   Expected: conversation transitions to `awaiting_time_choice`, two slots offered in outbound message.

6. While in `awaiting_time_choice`, simulate inbound `tomorrow morning?`.
   Expected: repair attempt increments, system re-offers slot choices and clarifies to reply `1` or `2`.

7. Repeat invalid time-choice reply again.
   Expected: `needs_staff_attention=true` and conversation is flagged; outbound indicates staff follow-up.

8. In `awaiting_time_choice`, simulate inbound `1` (or `2`).
   Expected: appointment is created, state becomes `booked`, confirmation outbound is created.

9. Verify reminder scheduling.
   Expected: booking creates `appointment_reminder` scheduled for 2 hours before appointment.

10. Simulate inbound `STOP` or `UNSUBSCRIBE`.
    Expected: lead `opted_out=true`, one confirmation outbound is created, subsequent automated outbound blocked.

11. Test 24-hour stale repair.
    Setup: adjust `conversations.last_outbound_at` to >24h ago and simulate inbound.
    Expected: state resets to `awaiting_yes` and outbound safe prompt asks to reply YES.

12. Toggle kill switch ON.
    Expected: pending jobs are cancelled, automated outbound creation attempts fail, `run_due_jobs` skips execution while enabled.

13. Agent dry-run outbound during business hours (valid consented lead/conversation).
    Expected: `allowed=true` and no new row appears in `messages`.

14. Agent execute outbound with same action payload.
    Expected: a new OUTBOUND `messages` row is created and matching `audit_log` entry exists for `create_outbound_message`.

15. Kill switch ON and automated outbound agent action.
    Expected: `agent_execute` outbound fails with kill-switch block (and `agent_dry_run` returns `allowed=false` for the same payload).
