/**
 * Sentry Type Definitions for Meeting Bot
 *
 * Adapted from the AdviseWell main platform patterns for a Node.js/Express backend service.
 */

export interface SentrySafeContext {
  meetingPlatform?: 'google' | 'microsoft' | 'zoom';
  jobId?: string;
  action?: string;
  feature?: string;
  component?: 'bot' | 'recorder' | 'uploader' | 'redis-consumer';
}

export interface SentryConfig {
  dsn?: string;
  environment?: string;
  release?: string;
  enabled?: boolean;
  sampleRate?: number;
  tracesSampleRate?: number;
}

/**
 * Only these tags are allowed through PII filtering.
 * All other tags are stripped in beforeSend.
 */
export const ALLOWED_TAGS = [
  'environment',
  'release',
  'version',
  'meeting_platform',
  'job_id',
  'component',
  'feature',
  'action',
  'source',
] as const;

/**
 * Only these request headers pass through — everything else is stripped.
 */
export const ALLOWED_HEADERS = [
  'content-type',
  'accept',
  'user-agent',
] as const;
