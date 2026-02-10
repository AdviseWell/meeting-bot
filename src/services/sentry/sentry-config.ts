/**
 * Sentry Configuration for Meeting Bot (Node.js/Express)
 *
 * Adapted from the AdviseWell main platform patterns for a backend service.
 * Uses @sentry/node instead of @sentry/react.
 *
 * Key differences from the frontend implementation:
 * - No session replay (server-side)
 * - No browser tracing (uses Node.js HTTP integration)
 * - Express request handler and error handler middleware
 * - Process-level uncaughtException/unhandledRejection capture
 *
 * CRITICAL: sendDefaultPii is set to false.
 * All data is scrubbed via beforeSend hooks before leaving the environment.
 */

import * as Sentry from '@sentry/node';
import type { Event, EventHint, ErrorEvent } from '@sentry/node';
import type { TransactionEvent, Contexts } from '@sentry/core';
import { scrubPII, scrubString, parameteriseName } from './pii-scrubber';
import { ALLOWED_TAGS, ALLOWED_HEADERS } from './types';
import type { SentryConfig, SentrySafeContext } from './types';

// ─── State ───────────────────────────────────────────────────────────────────

let isInitialised = false;

// ─── Error Ignore Patterns ──────────────────────────────────────────────────

/**
 * Errors matching these patterns are silently dropped.
 * These are transient, expected, or third-party errors that create noise.
 */
const IGNORE_ERRORS: Array<string | RegExp> = [
  // Network (transient)
  'Network request failed',
  'Failed to fetch',
  'Load failed',
  /^NetworkError/,
  'AbortError',
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EPIPE',
  'socket hang up',

  // Playwright/Puppeteer (expected during meeting interactions)
  'Target closed',
  'Protocol error',
  'Navigation failed because page was closed',
  /Target page, context or browser has been closed/,
  /Execution context was destroyed/,
  /frame was detached/,

  // Redis (transient connection issues)
  /Redis connection/i,
  'Connection is closed',
];

// ─── beforeSend Hook ────────────────────────────────────────────────────────

/**
 * Strip all PII before sending to Sentry.
 * This is the last line of defence before data leaves our environment.
 */
function beforeSend(event: Event, _hint: EventHint): Event | null {
  // 1. Scrub user data — keep only user.id
  if (event.user) {
    event.user = { id: event.user.id };
  }

  // 2. Scrub request data
  if (event.request) {
    // Remove cookies entirely
    delete event.request.cookies;

    // Filter headers to allowlist only
    if (event.request.headers) {
      const safeHeaders: Record<string, string> = {};
      for (const [key, value] of Object.entries(event.request.headers)) {
        if (ALLOWED_HEADERS.includes(key.toLowerCase() as typeof ALLOWED_HEADERS[number])) {
          safeHeaders[key] = value;
        }
      }
      event.request.headers = safeHeaders;
    }

    // Redact query strings
    if (event.request.query_string) {
      event.request.query_string = '[REDACTED]';
    }

    // Scrub request body
    if (event.request.data) {
      event.request.data = scrubPII(event.request.data);
    }

    // Parameterise URL
    if (event.request.url) {
      event.request.url = parameteriseName(event.request.url);
    }
  }

  // 3. Scrub breadcrumbs
  if (event.breadcrumbs) {
    event.breadcrumbs = event.breadcrumbs.map((bc) => ({
      ...bc,
      message: bc.message ? scrubString(bc.message) : bc.message,
      data: bc.data ? (scrubPII(bc.data) as Record<string, unknown>) : bc.data,
    }));
  }

  // 4. Scrub extra context
  if (event.extra) {
    event.extra = scrubPII(event.extra) as Record<string, unknown>;
  }

  // 5. Filter tags to allowlist
  if (event.tags) {
    const safeTags: Record<string, string> = {};
    for (const [key, value] of Object.entries(event.tags)) {
      if (ALLOWED_TAGS.includes(key as typeof ALLOWED_TAGS[number])) {
        safeTags[key] = typeof value === 'string' ? scrubString(value) : String(value);
      }
    }
    event.tags = safeTags;
  }

  // 6. Scrub exception values
  if (event.exception?.values) {
    event.exception.values = event.exception.values.map((ex) => ({
      ...ex,
      value: ex.value ? scrubString(ex.value) : ex.value,
    }));
  }

  // 7. Only keep safe contexts: runtime, os, device (limited)
  if (event.contexts) {
    const safeContexts: Contexts = {};
    if (event.contexts.runtime) safeContexts.runtime = event.contexts.runtime;
    if (event.contexts.os) safeContexts.os = event.contexts.os;
    if (event.contexts.device) {
      const { family, model, brand } = event.contexts.device as Record<string, unknown>;
      safeContexts.device = { family: family as string, model: model as string, brand: brand as string };
    }
    event.contexts = safeContexts;
  }

  return event;
}

/**
 * Strip PII from transaction/performance data.
 */
function beforeSendTransaction(
  event: TransactionEvent,
  _hint: EventHint
): TransactionEvent | null {
  // Parameterise transaction names
  if (event.transaction) {
    event.transaction = parameteriseName(event.transaction);
  }

  // Scrub span data
  if (event.spans) {
    for (const span of event.spans) {
      if (span.data) {
        span.data = scrubPII(span.data) as Record<string, string>;
      }
      if (span.description) {
        span.description = parameteriseName(span.description);
      }
    }
  }

  return event;
}

// ─── Initialisation ─────────────────────────────────────────────────────────

/**
 * Initialise Sentry for the Meeting Bot Node.js/Express service.
 *
 * Safe to call multiple times — will only initialise once.
 * Sentry is disabled if no DSN is provided (safe for local dev).
 */
export function initialiseSentry(config?: SentryConfig): void {
  if (isInitialised) return;

  const dsn = config?.dsn || process.env.SENTRY_DSN;
  if (!dsn || config?.enabled === false) {
    console.log('[Sentry] Disabled — no DSN configured');
    return;
  }

  const environment =
    config?.environment ||
    process.env.SENTRY_ENVIRONMENT ||
    process.env.NODE_ENV ||
    'development';
  const release = config?.release || process.env.SENTRY_RELEASE;

  Sentry.init({
    dsn,

    // CRITICAL: Disable default PII collection
    sendDefaultPii: false,

    environment,
    release,

    // PII scrubbing hooks
    beforeSend: beforeSend as unknown as (event: ErrorEvent, hint: EventHint) => ErrorEvent | null,
    beforeSendTransaction: beforeSendTransaction as unknown as (event: TransactionEvent, hint: EventHint) => TransactionEvent | null,

    // Sampling
    sampleRate: config?.sampleRate ?? 1.0,
    tracesSampleRate:
      config?.tracesSampleRate ??
      (environment === 'production' ? 0.1 : 1.0),

    // Error filtering
    ignoreErrors: IGNORE_ERRORS,

    // Breadcrumbs
    maxBreadcrumbs: 50,

    // Integrations for Node.js/Express
    integrations: [
      Sentry.httpIntegration(),
      Sentry.expressIntegration(),
    ],
  });

  isInitialised = true;
  console.log(`[Sentry] Initialised — env=${environment}`);
}

/**
 * Check if Sentry is currently enabled and initialised.
 */
export function isSentryEnabled(): boolean {
  return isInitialised;
}

// ─── Express Middleware ─────────────────────────────────────────────────────

/**
 * Sentry Express request handler middleware.
 * Must be added BEFORE all route handlers.
 */
export function sentryRequestHandler() {
  return Sentry.expressIntegration();
}

/**
 * Sentry Express error handler middleware.
 * Must be added AFTER all route handlers and BEFORE any other error handlers.
 */
export function sentryErrorHandler() {
  return Sentry.expressErrorHandler();
}

// ─── Context Setters ────────────────────────────────────────────────────────

/**
 * Set the meeting platform context for error grouping.
 */
export function setSentryMeetingContext(
  platform: 'google' | 'microsoft' | 'zoom',
  jobId?: string
): void {
  if (!isInitialised) return;
  Sentry.setTag('meeting_platform', platform);
  if (jobId) {
    Sentry.setTag('job_id', parameteriseName(jobId));
  }
}

/**
 * Set the component context (bot, recorder, uploader, etc.).
 */
export function setSentryComponentContext(
  component: 'bot' | 'recorder' | 'uploader' | 'redis-consumer'
): void {
  if (!isInitialised) return;
  Sentry.setTag('component', component);
}

// ─── Safe Error Capture ─────────────────────────────────────────────────────

/**
 * Capture an error with safe context tags only.
 */
export function captureErrorSafe(
  error: Error,
  context?: SentrySafeContext
): void {
  if (!isInitialised) return;

  Sentry.withScope((scope) => {
    if (context?.meetingPlatform) scope.setTag('meeting_platform', context.meetingPlatform);
    if (context?.jobId) scope.setTag('job_id', parameteriseName(context.jobId));
    if (context?.component) scope.setTag('component', context.component);
    if (context?.feature) scope.setTag('feature', context.feature);
    if (context?.action) scope.setTag('action', context.action);
    Sentry.captureException(error);
  });
}

/**
 * Capture a message with safe context.
 */
export function captureMessageSafe(
  message: string,
  level: Sentry.SeverityLevel = 'info',
  context?: SentrySafeContext
): void {
  if (!isInitialised) return;

  const scrubbed = scrubString(message);

  Sentry.withScope((scope) => {
    scope.setLevel(level);
    if (context?.meetingPlatform) scope.setTag('meeting_platform', context.meetingPlatform);
    if (context?.jobId) scope.setTag('job_id', parameteriseName(context.jobId));
    if (context?.component) scope.setTag('component', context.component);
    if (context?.feature) scope.setTag('feature', context.feature);
    if (context?.action) scope.setTag('action', context.action);
    Sentry.captureMessage(scrubbed);
  });
}

// ─── Breadcrumbs ────────────────────────────────────────────────────────────

/**
 * Add a breadcrumb with automatic PII scrubbing.
 */
export function addBreadcrumb(
  message: string,
  category: string,
  data?: Record<string, unknown>
): void {
  if (!isInitialised) return;

  Sentry.addBreadcrumb({
    message: scrubString(message),
    category,
    data: data ? (scrubPII(data) as Record<string, unknown>) : undefined,
    level: 'info',
  });
}

// ─── Tracing ────────────────────────────────────────────────────────────────

export interface SpanOptions {
  op: string;
  name: string;
  attributes?: Record<string, string | number | boolean>;
}

/**
 * Start a synchronous span for performance tracing.
 */
export function startSpan<T>(
  options: SpanOptions,
  callback: (span: Sentry.Span) => T
): T {
  if (!isInitialised) {
    return callback(undefined as unknown as Sentry.Span);
  }

  return Sentry.startSpan(
    {
      op: options.op,
      name: parameteriseName(options.name),
      attributes: options.attributes,
    },
    callback
  );
}

/**
 * Start an async span for performance tracing.
 */
export async function startSpanAsync<T>(
  options: SpanOptions,
  callback: (span: Sentry.Span) => Promise<T>
): Promise<T> {
  if (!isInitialised) {
    return callback(undefined as unknown as Sentry.Span);
  }

  return Sentry.startSpan(
    {
      op: options.op,
      name: parameteriseName(options.name),
      attributes: options.attributes,
    },
    callback
  );
}

// ─── Shutdown ───────────────────────────────────────────────────────────────

/**
 * Flush all pending Sentry events before process exit.
 * Call this during graceful shutdown.
 *
 * @param timeout - Maximum time to wait for flush (ms)
 */
export async function flushSentry(timeout = 2000): Promise<void> {
  if (!isInitialised) return;

  try {
    await Sentry.flush(timeout);
    console.log('[Sentry] Flushed pending events');
  } catch (error) {
    console.error('[Sentry] Failed to flush:', error);
  }
}

/**
 * Direct access to the Sentry SDK (use sparingly).
 */
export function getSentry(): typeof Sentry {
  return Sentry;
}
