import { logClientError } from './api';

const MAX_ERROR_TEXT_LENGTH = 10_000;

export type FatalClientError = {
  message: string;
  stack?: string;
  source: string;
};

type FatalListener = (error: FatalClientError) => void;

let fatalError: FatalClientError | null = null;
let handlersInstalled = false;
const fatalListeners = new Set<FatalListener>();

function truncateText(value: string | undefined): string | undefined {
  if (!value) return undefined;
  return value.length > MAX_ERROR_TEXT_LENGTH ? `${value.slice(0, MAX_ERROR_TEXT_LENGTH)}...(truncated)` : value;
}

function normalizeClientError(input: FatalClientError): FatalClientError {
  return {
    message: truncateText(input.message) ?? 'Unknown client error',
    stack: truncateText(input.stack),
    source: truncateText(input.source) ?? 'unknown'
  };
}

function notifyFatal(error: FatalClientError) {
  fatalError = error;
  for (const listener of fatalListeners) {
    try {
      listener(error);
    } catch {
      // Never let crash subscribers throw.
    }
  }
}

export function reportClientError(input: FatalClientError, opts?: { fatal?: boolean }) {
  try {
    const payload = normalizeClientError(input);
    if (opts?.fatal) {
      notifyFatal(payload);
    }
    void logClientError(payload).catch(() => {
      // Best-effort logging only.
    });
  } catch {
    // Never throw from error reporting.
  }
}

export function getFatalError(): FatalClientError | null {
  return fatalError;
}

export function subscribeFatalErrors(listener: FatalListener): () => void {
  fatalListeners.add(listener);
  return () => {
    fatalListeners.delete(listener);
  };
}

function toMessage(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  if (typeof reason === 'string') return reason;
  try {
    return JSON.stringify(reason);
  } catch {
    return String(reason);
  }
}

function toStack(reason: unknown): string | undefined {
  return reason instanceof Error ? reason.stack : undefined;
}

export function setupGlobalCrashHandlers() {
  if (handlersInstalled || typeof window === 'undefined') return;
  handlersInstalled = true;

  window.addEventListener('error', (event) => {
    try {
      const message = event.error instanceof Error ? event.error.message : event.message || 'Unhandled window error';
      const stack = event.error instanceof Error ? event.error.stack : undefined;
      const source = event.filename
        ? `window.error:${event.filename}:${event.lineno}:${event.colno}`
        : 'window.error';
      reportClientError({ message, stack, source }, { fatal: true });
    } catch {
      // Never throw from crash handlers.
    }
  });

  window.addEventListener('unhandledrejection', (event) => {
    try {
      reportClientError(
        {
          message: toMessage(event.reason),
          stack: toStack(event.reason),
          source: 'window.unhandledrejection'
        },
        { fatal: true }
      );
    } catch {
      // Never throw from crash handlers.
    }
  });
}
