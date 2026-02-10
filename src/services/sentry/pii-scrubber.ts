/**
 * PII Scrubber for Sentry
 *
 * Comprehensive PII scrubbing utilities for sensitive data.
 * All data is scrubbed BEFORE leaving the local environment.
 *
 * Follows the exact patterns from the AdviseWell main platform.
 *
 * IMPORTANT: This module handles sensitive client data.
 * All personally identifiable information must be stripped.
 */

/**
 * Patterns for sensitive field names.
 * Any object key matching these patterns will have its value redacted.
 */
const SENSITIVE_KEY_PATTERNS: RegExp[] = [
  // Personal identifiers
  /email/i,
  /phone/i,
  /mobile/i,
  /address/i,
  /^name$/i,
  /firstName/i,
  /lastName/i,
  /fullName/i,
  /first_name/i,
  /last_name/i,
  /full_name/i,
  /dob/i,
  /dateOfBirth/i,
  /date_of_birth/i,
  /birthDate/i,
  /birth_date/i,
  /ssn/i,
  /socialSecurity/i,
  /social_security/i,
  /tfn/i,
  /taxFileNumber/i,
  /tax_file_number/i,
  /passport/i,
  /licen[sc]e/i,
  /medicare/i,

  // Financial identifiers
  /accountNumber/i,
  /account_number/i,
  /bsb/i,
  /cardNumber/i,
  /card_number/i,
  /cvv/i,
  /cvc/i,
  /iban/i,
  /swift/i,
  /abn/i,
  /acn/i,
  /routing/i,

  // Authentication
  /password/i,
  /token/i,
  /secret/i,
  /apiKey/i,
  /api_key/i,
  /^auth/i,
  /credential/i,
  /session/i,
  /cookie/i,

  // Financial data
  /balance/i,
  /income/i,
  /salary/i,
  /superannuation/i,
  /^super$/i,
  /investment/i,
  /portfolio/i,
  /netWorth/i,
  /net_worth/i,
  /asset/i,
  /liability/i,
  /debt/i,
  /loan/i,
  /mortgage/i,
  /insurance/i,
  /beneficiary/i,

  // Client-specific
  /clientId/i,
  /client_id/i,
  /customerId/i,
  /customer_id/i,
  /userId/i,
  /user_id/i,
  /uid/i,
];

/**
 * Regex patterns for detecting PII in string values.
 * Each pattern has a replacement string.
 */
const PII_VALUE_PATTERNS: Array<{ pattern: RegExp; replacement: string }> = [
  // Email addresses
  {
    pattern: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,
    replacement: '[EMAIL_REDACTED]',
  },
  // Australian phone numbers (various formats)
  {
    pattern: /(\+?61|0)[2-478][\s.-]?\d{4}[\s.-]?\d{4}/g,
    replacement: '[PHONE_REDACTED]',
  },
  // Australian mobile numbers
  {
    pattern: /(\+?61|0)4[\s.-]?\d{2}[\s.-]?\d{3}[\s.-]?\d{3}/g,
    replacement: '[PHONE_REDACTED]',
  },
  // Tax File Number (9 digits, various formats)
  {
    pattern: /\b\d{3}[\s.-]?\d{3}[\s.-]?\d{3}\b/g,
    replacement: '[TFN_REDACTED]',
  },
  // Credit card numbers (16 digits, various formats)
  {
    pattern: /\b\d{4}[\s.-]?\d{4}[\s.-]?\d{4}[\s.-]?\d{4}\b/g,
    replacement: '[CARD_REDACTED]',
  },
  // ABN (11 digits)
  {
    pattern: /\b\d{2}[\s.-]?\d{3}[\s.-]?\d{3}[\s.-]?\d{3}\b/g,
    replacement: '[ABN_REDACTED]',
  },
  // BSB (6 digits with dash)
  {
    pattern: /\b\d{3}[-]?\d{3}\b/g,
    replacement: '[BSB_REDACTED]',
  },
  // Medicare number (10-11 digits)
  {
    pattern: /\b\d{4}[\s.-]?\d{5}[\s.-]?\d{1,2}\b/g,
    replacement: '[MEDICARE_REDACTED]',
  },
];

/**
 * Check if a key matches any sensitive pattern.
 */
function isSensitiveKey(key: string): boolean {
  return SENSITIVE_KEY_PATTERNS.some((pattern) => pattern.test(key));
}

/**
 * Scrub PII patterns from a string value.
 */
export function scrubString(value: string): string {
  let result = value;
  for (const { pattern, replacement } of PII_VALUE_PATTERNS) {
    // Reset regex lastIndex for global patterns
    pattern.lastIndex = 0;
    result = result.replace(pattern, replacement);
  }
  return result;
}

/**
 * Recursively scrub PII from any value.
 * - Strings are scanned for PII patterns
 * - Objects have sensitive keys redacted
 * - Arrays are recursively processed
 *
 * @param value - The value to scrub
 * @param depth - Current recursion depth (max 10 to prevent infinite loops)
 * @returns Scrubbed value
 */
export function scrubPII(value: unknown, depth = 0): unknown {
  // Prevent infinite recursion
  if (depth > 10) {
    return '[MAX_DEPTH]';
  }

  // Handle null/undefined
  if (value === null || value === undefined) {
    return value;
  }

  // Handle strings - scan for PII patterns
  if (typeof value === 'string') {
    return scrubString(value);
  }

  // Handle arrays - recursively process each element
  if (Array.isArray(value)) {
    return value.map((item) => scrubPII(item, depth + 1));
  }

  // Handle objects - check keys and recursively process values
  if (typeof value === 'object') {
    const scrubbed: Record<string, unknown> = {};

    for (const [key, val] of Object.entries(value)) {
      if (isSensitiveKey(key)) {
        // Redact sensitive keys entirely
        scrubbed[key] = '[REDACTED]';
      } else {
        // Recursively scrub non-sensitive keys
        scrubbed[key] = scrubPII(val, depth + 1);
      }
    }

    return scrubbed;
  }

  // Return primitives (numbers, booleans) unchanged
  return value;
}

/**
 * Parameterise a URL or transaction name by replacing IDs with placeholders.
 * This prevents PII leakage through URL patterns.
 *
 * @param name - The URL or transaction name
 * @returns Parameterised string
 */
export function parameteriseName(name: string): string {
  let result = name;

  // Replace UUIDs
  result = result.replace(
    /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
    '[ID]'
  );

  // Replace Firebase document IDs (20 alphanumeric characters)
  result = result.replace(/\/[a-zA-Z0-9]{20,}(?=\/|$|\?)/g, '/[ID]');

  // Replace numeric IDs in URL paths
  result = result.replace(/\/\d+(?=\/|$|\?)/g, '/[ID]');

  return result;
}
