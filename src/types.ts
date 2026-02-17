export type LeadSummary = {
  id: number;
  phone_e164: string;
  first_name: string | null;
  last_name: string | null;
  status: string;
  consent: boolean;
  opted_out: boolean;
  needs_staff_attention: boolean;
  created_at: string;
};

export type Message = {
  id: number;
  direction: 'INBOUND' | 'OUTBOUND';
  body: string;
  status: string;
  created_at: string;
};

export type Appointment = {
  id: number;
  start_at: string;
  end_at: string;
  status: string;
};

export type Conversation = {
  id: number;
  state: string;
  repair_attempts: number;
  last_inbound_at: string | null;
  last_outbound_at: string | null;
  state_json: string;
};

export type LeadDetail = {
  lead: LeadSummary & {
    consent_at: string | null;
    consent_source: string | null;
    next_action_at: string | null;
    last_contact_at: string | null;
  };
  conversation: Conversation;
  messages: Message[];
  appointments: Appointment[];
};

export type LeadCreateInput = {
  first_name: string;
  last_name: string;
  phone_e164: string;
  consent: boolean;
  consent_at: string | null;
  source: string;
};

export type LeadCreateResult = {
  created: boolean;
  lead_id: number;
  duplicate_of: number | null;
  note: string | null;
};

export type TodayReport = {
  leads_created: number;
  contacted: number;
  booked: number;
  opt_outs: number;
  needs_attention: number;
};

export type RunJobsResult = {
  processed: number;
  skipped: number;
  errors: number;
};

export type AgentAction =
  | {
      action_type: 'send_outbound';
      lead_id: number;
      conversation_id: number;
      body: string;
      automated: boolean;
      allow_without_consent: boolean;
      allow_opted_out_once: boolean;
      allow_after_reply: boolean;
      ignore_business_hours: boolean;
    }
  | {
      action_type: 'book_appointment';
      lead_id: number;
      start_at: string;
      end_at: string;
      status: string;
    }
  | {
      action_type: 'set_opt_out';
      lead_id: number;
      reason: string;
    }
  | {
      action_type: 'schedule_job';
      job_type: string;
      target_id: number | null;
      execute_at: string;
      payload_json: string;
    };

export type AgentDryRunRequest = {
  action: AgentAction;
};

export type AgentDryRunResult = {
  allowed: boolean;
  blocked_reason: string | null;
  warnings: string[];
  normalized: Record<string, unknown> | null;
};

export type AgentExecuteRequest = {
  action: AgentAction;
};

export type AgentExecuteResult = {
  success: boolean;
  result_json: Record<string, unknown> | null;
  error: string | null;
};
