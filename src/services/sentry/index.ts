/**
 * Sentry Integration for Meeting Bot
 *
 * Re-exports all public APIs from the Sentry service.
 */

export {
  // Initialisation & lifecycle
  initialiseSentry,
  isSentryEnabled,
  flushSentry,

  // Express middleware
  sentryErrorHandler,

  // Context
  setSentryMeetingContext,
  setSentryComponentContext,

  // Error capture
  captureErrorSafe,
  captureMessageSafe,

  // Breadcrumbs
  addBreadcrumb,

  // Tracing
  startSpan,
  startSpanAsync,

  // Direct SDK access
  getSentry,
} from './sentry-config';

export type {
  SpanOptions,
} from './sentry-config';

export type {
  SentryConfig,
  SentrySafeContext,
} from './types';

export { scrubPII, scrubString } from './pii-scrubber';
