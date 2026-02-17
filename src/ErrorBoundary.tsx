import React from 'react';
import { getFatalError, reportClientError, subscribeFatalErrors, type FatalClientError } from './crash';

type ErrorBoundaryProps = {
  children: React.ReactNode;
};

type ErrorBoundaryState = {
  fatalError: FatalClientError | null;
};

const MAX_ERROR_TEXT_LENGTH = 10_000;

function truncateText(value: string | undefined): string | undefined {
  if (!value) return undefined;
  return value.length > MAX_ERROR_TEXT_LENGTH ? `${value.slice(0, MAX_ERROR_TEXT_LENGTH)}...(truncated)` : value;
}

export default class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  private unsubscribeFatal?: () => void;

  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      fatalError: getFatalError()
    };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return {
      fatalError: {
        message: truncateText(error.message) ?? 'Unknown React error',
        stack: truncateText(error.stack),
        source: 'react.error_boundary'
      }
    };
  }

  componentDidMount() {
    this.unsubscribeFatal = subscribeFatalErrors((fatalError) => {
      this.setState({ fatalError });
    });
  }

  componentWillUnmount() {
    this.unsubscribeFatal?.();
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    reportClientError({
      message: truncateText(error.message) ?? 'Unknown React error',
      stack: truncateText(error.stack ?? errorInfo.componentStack ?? undefined),
      source: 'react.error_boundary'
    });
  }

  private getCopyText() {
    const { fatalError } = this.state;
    if (!fatalError) return 'No error details.';
    return [
      `source: ${fatalError.source}`,
      `message: ${fatalError.message}`,
      '',
      'stack:',
      fatalError.stack ?? '(none)'
    ].join('\n');
  }

  private handleCopy = async () => {
    try {
      if (!navigator.clipboard) return;
      await navigator.clipboard.writeText(this.getCopyText());
    } catch {
      // Best-effort only.
    }
  };

  render() {
    const { fatalError } = this.state;
    if (!fatalError) {
      return this.props.children;
    }

    const displayText = truncateText(
      [`message: ${fatalError.message}`, '', 'stack:', fatalError.stack ?? '(none)'].join('\n')
    );

    return (
      <div
        style={{
          minHeight: '100vh',
          width: '100%',
          background: '#111827',
          color: '#f9fafb',
          padding: '24px',
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace'
        }}
      >
        <h1 style={{ marginTop: 0, marginBottom: '12px' }}>Gold Bot crashed</h1>
        <pre
          style={{
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            background: '#0b1220',
            border: '1px solid #374151',
            borderRadius: '8px',
            padding: '12px',
            maxHeight: '65vh',
            overflow: 'auto'
          }}
        >
          {displayText}
        </pre>
        <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
          <button type="button" onClick={() => window.location.reload()}>
            Reload
          </button>
          <button type="button" onClick={() => void this.handleCopy()}>
            Copy error
          </button>
        </div>
      </div>
    );
  }
}
