import { invoke } from '@tauri-apps/api/tauri';
import type {
  AgentDryRunRequest,
  AgentDryRunResult,
  AgentExecuteRequest,
  AgentExecuteResult,
  LeadCreateInput,
  LeadCreateResult,
  LeadDetail,
  LeadSummary,
  RunJobsResult,
  TodayReport
} from './types';

export type ClientErrorLogInput = {
  message: string;
  stack?: string;
  source: string;
};

export async function listLeads(): Promise<LeadSummary[]> {
  return invoke('list_leads');
}

export async function createLead(input: LeadCreateInput): Promise<LeadCreateResult> {
  return invoke('create_lead', { input });
}

export async function getLeadDetail(leadId: number): Promise<LeadDetail> {
  return invoke('get_lead_detail', { lead_id: leadId });
}

export async function simulateInboundSms(leadId: number, body: string): Promise<void> {
  return invoke('simulate_inbound_sms', { lead_id: leadId, body });
}

export async function getTodayReport(): Promise<TodayReport> {
  return invoke('get_today_report');
}

export async function getKillSwitch(): Promise<boolean> {
  return invoke('get_kill_switch');
}

export async function setKillSwitch(enabled: boolean): Promise<void> {
  return invoke('set_kill_switch', { enabled });
}

export async function runDueJobs(): Promise<RunJobsResult> {
  return invoke('run_due_jobs');
}

export async function listAgentQueue(): Promise<LeadSummary[]> {
  return invoke('list_agent_queue');
}

export async function agentDryRun(req: AgentDryRunRequest): Promise<AgentDryRunResult> {
  return invoke('agent_dry_run', { req });
}

export async function agentExecute(req: AgentExecuteRequest): Promise<AgentExecuteResult> {
  return invoke('agent_execute', { req });
}

export async function logClientError(input: ClientErrorLogInput): Promise<void> {
  return invoke('log_client_error', {
    message: input.message,
    stack: input.stack,
    source: input.source
  });
}
