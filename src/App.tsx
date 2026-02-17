import { useEffect, useMemo, useState } from 'react';
import { invoke } from '@tauri-apps/api/tauri';
import {
  createLead,
  exportDbPath,
  getLocationSettings,
  getKillSwitch,
  getLeadDetail,
  getTodayReport,
  listLeads,
  runDueJobs,
  setKillSwitch,
  simulateInboundSms,
  updateLocationSettings,
  wipeAllDataConfirmed
} from './api';
import type {
  LeadCreateInput,
  LeadDetail,
  LeadSummary,
  TodayReport,
  UpdateLocationSettingsInput
} from './types';

const emptyForm: LeadCreateInput = {
  first_name: '',
  last_name: '',
  phone_e164: '',
  consent: true,
  consent_at: null,
  source: 'front-desk'
};

const emptyLocationSettings: UpdateLocationSettingsInput = {
  gym_name: '',
  timezone: '',
  business_hours_json: ''
};

function formatTs(ts: string | null) {
  if (!ts) return '-';
  const date = new Date(ts);
  return Number.isNaN(date.getTime()) ? ts : date.toLocaleString();
}

export default function App() {
  const [form, setForm] = useState<LeadCreateInput>(emptyForm);
  const [leads, setLeads] = useState<LeadSummary[]>([]);
  const [selectedLeadId, setSelectedLeadId] = useState<number | null>(null);
  const [leadDetail, setLeadDetail] = useState<LeadDetail | null>(null);
  const [inboundText, setInboundText] = useState('');
  const [report, setReport] = useState<TodayReport | null>(null);
  const [killSwitch, setKillSwitchState] = useState(false);
  const [locationSettings, setLocationSettings] = useState<UpdateLocationSettingsInput>(emptyLocationSettings);
  const [dbPath, setDbPath] = useState('');
  const [alert, setAlert] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const selectedLead = useMemo(() => leads.find((l) => l.id === selectedLeadId) ?? null, [leads, selectedLeadId]);

  async function refreshBasics() {
    const [leadsData, reportData, killSwitchData, dbPathData] = await Promise.all([
      listLeads(),
      getTodayReport(),
      getKillSwitch(),
      exportDbPath()
    ]);
    setLeads(leadsData);
    setReport(reportData);
    setKillSwitchState(killSwitchData);
    setDbPath(dbPathData);
    if (!selectedLeadId && leadsData.length > 0) {
      setSelectedLeadId(leadsData[0].id);
    }
  }

  async function refreshLeadDetail(leadId: number | null = selectedLeadId) {
    if (!leadId) {
      setLeadDetail(null);
      return;
    }
    const detail = await getLeadDetail(leadId);
    setLeadDetail(detail);
  }

  async function refreshAll() {
    await refreshBasics();
    await refreshLeadDetail();
  }

  async function loadLocationSettings() {
    const settings = await getLocationSettings();
    setLocationSettings({
      gym_name: settings.gym_name,
      timezone: settings.timezone,
      business_hours_json: settings.business_hours_json
    });
  }

  useEffect(() => {
    Promise.all([refreshAll(), loadLocationSettings()]).catch((err) => setAlert(String(err)));
  }, []);

  useEffect(() => {
    refreshLeadDetail().catch((err) => setAlert(String(err)));
  }, [selectedLeadId]);

  async function submitLead() {
    setBusy(true);
    try {
      const payload: LeadCreateInput = {
        ...form,
        consent_at: form.consent ? new Date().toISOString() : null
      };
      const result = await createLead(payload);
      if (result.note) {
        setAlert(result.note);
      } else {
        setAlert(null);
      }
      setSelectedLeadId(result.lead_id);
      setForm(emptyForm);
      await refreshAll();
    } catch (err) {
      setAlert(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function submitInbound() {
    if (!selectedLeadId || !inboundText.trim()) return;
    setBusy(true);
    try {
      await simulateInboundSms(selectedLeadId, inboundText.trim());
      setInboundText('');
      await refreshAll();
    } catch (err) {
      setAlert(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function toggleKillSwitch(value: boolean) {
    setBusy(true);
    try {
      await setKillSwitch(value);
      setKillSwitchState(value);
      await refreshAll();
    } catch (err) {
      setAlert(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function saveSettings() {
    setBusy(true);
    try {
      await updateLocationSettings(locationSettings);
      await loadLocationSettings();
      setAlert('Settings saved.');
    } catch (err) {
      setAlert(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function wipeAllTestData() {
    if (!window.confirm('Wipe all test data? This removes leads, conversations, messages, jobs, appointments, and prior audit logs.')) {
      return;
    }

    setBusy(true);
    try {
      const result = await wipeAllDataConfirmed();
      setSelectedLeadId(null);
      setLeadDetail(null);
      setInboundText('');
      await refreshAll();
      setAlert(
        `Wiped test data: messages=${result.messages}, appointments=${result.appointments}, scheduled_jobs=${result.scheduled_jobs}, audit_log=${result.audit_log}, conversations=${result.conversations}, leads=${result.leads}`
      );
    } catch (err) {
      setAlert(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function openDevtools() {
    try {
      await invoke('open_devtools');
      return;
    } catch {
      // Fall through to direct window API.
    }

    try {
      const tauriWindow = (
        window as Window & {
          __TAURI__?: {
            window?: {
              getCurrent?: () => { openDevtools?: () => void };
            };
          };
        }
      ).__TAURI__?.window?.getCurrent?.();
      tauriWindow?.openDevtools?.();
    } catch {
      // Best-effort only.
    }
  }

  return (
    <main>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '12px' }}>
        <h1>Gold Bot</h1>
        {import.meta.env.DEV && (
          <button type="button" onClick={() => void openDevtools()}>
            Debug
          </button>
        )}
      </div>
      <p className="subtle">Local-only PoC. No real SMS or webhooks.</p>

      {alert && <div className="alert">{alert}</div>}

      <section className="grid two">
        <article className="panel">
          <h2>Today</h2>
          <div className="stats">
            <div>
              <span>Leads Created</span>
              <strong>{report?.leads_created ?? 0}</strong>
            </div>
            <div>
              <span>Contacted</span>
              <strong>{report?.contacted ?? 0}</strong>
            </div>
            <div>
              <span>Booked</span>
              <strong>{report?.booked ?? 0}</strong>
            </div>
            <div>
              <span>Opt-outs</span>
              <strong>{report?.opt_outs ?? 0}</strong>
            </div>
            <div>
              <span>Needs Attention</span>
              <strong>{report?.needs_attention ?? 0}</strong>
            </div>
          </div>
        </article>

        <article className="panel">
          <h2>Kill Switch</h2>
          <label className="switch-row">
            <input
              type="checkbox"
              checked={killSwitch}
              disabled={busy}
              onChange={(e) => toggleKillSwitch(e.currentTarget.checked)}
            />
            <span>Disable all automated message creation</span>
          </label>
          <p className="subtle">
            {killSwitch
              ? 'Automation paused (safe mode).'
              : 'Automation active (local simulated outbound).'}
          </p>
          <button disabled={busy} onClick={() => runDueJobs().then(refreshAll).catch((err) => setAlert(String(err)))}>
            Run Due Jobs Now
          </button>
        </article>
      </section>

      <section className="panel">
        <h2>Test Utilities</h2>
        <label>
          DB Path
          <input
            readOnly
            value={dbPath}
            onFocus={(e) => e.currentTarget.select()}
            style={{ fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}
          />
        </label>
        <button disabled={busy} onClick={wipeAllTestData}>
          Wipe All Test Data
        </button>
      </section>

      <section className="grid two">
        <article className="panel">
          <h2>Lead Intake</h2>
          <div className="form-grid">
            <label>
              First Name
              <input
                value={form.first_name}
                onChange={(e) => {
                  const value = e.currentTarget.value;
                  setForm((f) => ({ ...f, first_name: value }));
                }}
              />
            </label>
            <label>
              Last Name
              <input
                value={form.last_name}
                onChange={(e) => {
                  const value = e.currentTarget.value;
                  setForm((f) => ({ ...f, last_name: value }));
                }}
              />
            </label>
            <label>
              Phone (E.164)
              <input
                placeholder="+15555550123"
                value={form.phone_e164}
                onChange={(e) => {
                  const value = e.currentTarget.value;
                  setForm((f) => ({ ...f, phone_e164: value }));
                }}
              />
            </label>
            <label>
              Source
              <input
                value={form.source}
                onChange={(e) => {
                  const value = e.currentTarget.value;
                  setForm((f) => ({ ...f, source: value }));
                }}
              />
            </label>
            <label className="switch-row">
              <input
                type="checkbox"
                checked={form.consent}
                onChange={(e) => {
                  const checked = e.currentTarget.checked;
                  setForm((f) => ({ ...f, consent: checked }));
                }}
              />
              <span>Consent received</span>
            </label>
          </div>
          <button disabled={busy || !form.phone_e164.trim()} onClick={submitLead}>
            Create Lead
          </button>
        </article>

        <article className="panel">
          <h2>Leads</h2>
          <div className="list">
            {leads.map((lead) => (
              <button
                key={lead.id}
                className={`lead-row ${selectedLeadId === lead.id ? 'active' : ''}`}
                onClick={() => setSelectedLeadId(lead.id)}
              >
                <div>
                  <strong>
                    {lead.first_name || 'Unknown'} {lead.last_name || ''}
                  </strong>
                  <small>{lead.phone_e164}</small>
                </div>
                <div className="flags">
                  <small>{lead.status}</small>
                  {lead.opted_out && <small className="chip danger">opted out</small>}
                  {lead.needs_staff_attention && <small className="chip warn">needs staff</small>}
                </div>
              </button>
            ))}
            {leads.length === 0 && <p>No leads yet.</p>}
          </div>
        </article>
      </section>

      <section className="panel">
        <h2>Settings</h2>
        <div className="form-grid">
          <label>
            Gym Name
            <input
              value={locationSettings.gym_name}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setLocationSettings((current) => ({
                  ...current,
                  gym_name: value
                }));
              }}
            />
          </label>
          <label>
            Timezone
            <input
              value={locationSettings.timezone}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setLocationSettings((current) => ({
                  ...current,
                  timezone: value
                }));
              }}
            />
          </label>
          <label style={{ gridColumn: '1 / -1' }}>
            Business Hours JSON
            <textarea
              value={locationSettings.business_hours_json}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setLocationSettings((current) => ({
                  ...current,
                  business_hours_json: value
                }));
              }}
            />
          </label>
        </div>
        <button
          disabled={
            busy ||
            !locationSettings.gym_name.trim() ||
            !locationSettings.timezone.trim() ||
            !locationSettings.business_hours_json.trim()
          }
          onClick={saveSettings}
        >
          Save Settings
        </button>
      </section>

      <section className="panel">
        <h2>Lead Detail</h2>
        {!selectedLead || !leadDetail ? (
          <p>Select a lead to view conversation details.</p>
        ) : (
          <div className="detail-grid">
            <div>
              <p>
                <strong>Name:</strong> {leadDetail.lead.first_name || '-'} {leadDetail.lead.last_name || ''}
              </p>
              <p>
                <strong>Phone:</strong> {leadDetail.lead.phone_e164}
              </p>
              <p>
                <strong>State:</strong> {leadDetail.conversation.state}
              </p>
              <p>
                <strong>Repairs:</strong> {leadDetail.conversation.repair_attempts}
              </p>
              <p>
                <strong>Last Contact:</strong> {formatTs(leadDetail.lead.last_contact_at)}
              </p>
              <p>
                <strong>Next Action:</strong> {formatTs(leadDetail.lead.next_action_at)}
              </p>
              <h3>Simulate inbound SMS</h3>
              <textarea
                value={inboundText}
                onChange={(e) => setInboundText(e.currentTarget.value)}
                placeholder="Type inbound message"
              />
              <button disabled={busy || !inboundText.trim()} onClick={submitInbound}>
                Submit Inbound SMS
              </button>
            </div>

            <div>
              <h3>Messages</h3>
              <div className="message-list">
                {leadDetail.messages.map((msg) => (
                  <div key={msg.id} className={`message ${msg.direction === 'OUTBOUND' ? 'outbound' : 'inbound'}`}>
                    <small>
                      {msg.direction} · {formatTs(msg.created_at)} · {msg.status}
                    </small>
                    <p>{msg.body}</p>
                  </div>
                ))}
              </div>

              <h3>Appointments</h3>
              {leadDetail.appointments.length === 0 && <p>No appointments.</p>}
              {leadDetail.appointments.map((apt) => (
                <p key={apt.id}>
                  {formatTs(apt.start_at)} to {formatTs(apt.end_at)} ({apt.status})
                </p>
              ))}
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
