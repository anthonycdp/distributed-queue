import type { BackoffConfig } from '../types/index.js';

/**
 * Calculate backoff delay based on configuration
 */
export function calculateBackoff(
  attemptsMade: number,
  config: BackoffConfig
): number {
  switch (config.type) {
    case 'fixed':
      return config.delay;

    case 'exponential':
      return calculateExponentialBackoff(attemptsMade, config.delay, config.maxDelay);

    case 'custom':
      if (config.customFunction) {
        return config.customFunction(attemptsMade);
      }
      return config.delay;

    default:
      return config.delay;
  }
}

/**
 * Calculate exponential backoff with optional jitter
 */
export function calculateExponentialBackoff(
  attemptsMade: number,
  baseDelay: number,
  maxDelay?: number,
  jitter: boolean = true
): number {
  // Exponential delay: baseDelay * 2^(attemptsMade - 1)
  const exponentialDelay = baseDelay * Math.pow(2, attemptsMade - 1);

  // Apply max delay cap
  let delay = maxDelay ? Math.min(exponentialDelay, maxDelay) : exponentialDelay;

  // Add jitter (±25%) to prevent thundering herd
  if (jitter) {
    const jitterAmount = delay * 0.25;
    delay = delay + (Math.random() * jitterAmount * 2 - jitterAmount);
  }

  return Math.round(delay);
}

/**
 * Create a custom backoff function for BullMQ
 */
export function createBullMQBackoff(config: BackoffConfig): (attemptsMade: number) => number {
  return (attemptsMade: number) => calculateBackoff(attemptsMade, config);
}

/**
 * Predefined backoff strategies
 */
export const BackoffStrategies = {
  /**
   * Fixed delay - same delay for all retries
   */
  fixed: (delay: number): BackoffConfig => ({
    type: 'fixed',
    delay,
  }),

  /**
   * Exponential backoff - doubles delay each retry
   */
  exponential: (baseDelay: number, maxDelay?: number): BackoffConfig => ({
    type: 'exponential',
    delay: baseDelay,
    maxDelay,
  }),

  /**
   * Linear backoff - increases by fixed amount each retry
   */
  linear: (incrementMs: number): BackoffConfig => ({
    type: 'custom',
    delay: incrementMs,
    customFunction: (attemptsMade: number) => attemptsMade * incrementMs,
  }),

  /**
   * Fibonacci backoff - uses fibonacci sequence for delays
   */
  fibonacci: (baseDelay: number): BackoffConfig => ({
    type: 'custom',
    delay: baseDelay,
    customFunction: (attemptsMade: number) => {
      const fib = (n: number): number => (n <= 1 ? n : fib(n - 1) + fib(n - 2));
      return fib(attemptsMade + 1) * baseDelay;
    },
  }),

  /**
   * Decorrelated jitter - recommended for distributed systems
   * Based on AWS's jitter strategy
   */
  decorrelatedJitter: (
    baseDelay: number,
    maxDelay: number,
    sleep?: number
  ): BackoffConfig => ({
    type: 'custom',
    delay: baseDelay,
    customFunction: (_attemptsMade: number, prevSleep = sleep ?? baseDelay) => {
      const sleep = Math.min(maxDelay, Math.random() * (prevSleep * 3 - baseDelay) + baseDelay);
      return Math.round(sleep);
    },
  }),
} as const;

/**
 * Retry policy with backoff configuration
 */
export interface RetryPolicy {
  /** Maximum number of retry attempts */
  maxAttempts: number;
  /** Initial delay in milliseconds */
  initialDelay: number;
  /** Maximum delay cap */
  maxDelay: number;
  /** Backoff type */
  backoffType: 'exponential' | 'fixed' | 'linear' | 'decorrelated';
  /** Errors that should trigger immediate retry (no backoff) */
  immediateRetryErrors?: string[];
  /** Errors that should not be retried */
  nonRetryableErrors?: string[];
}

/**
 * Default retry policies
 */
export const DefaultRetryPolicies = {
  /**
   * Standard retry policy for most operations
   */
  standard: (): RetryPolicy => ({
    maxAttempts: 3,
    initialDelay: 1000,
    maxDelay: 30000,
    backoffType: 'exponential',
  }),

  /**
   * Aggressive retry for critical operations
   */
  aggressive: (): RetryPolicy => ({
    maxAttempts: 10,
    initialDelay: 500,
    maxDelay: 60000,
    backoffType: 'exponential',
  }),

  /**
   * Conservative retry for rate-limited APIs
   */
  conservative: (): RetryPolicy => ({
    maxAttempts: 3,
    initialDelay: 5000,
    maxDelay: 120000,
    backoffType: 'decorrelated',
  }),

  /**
   * Minimal retry - single retry with short delay
   */
  minimal: (): RetryPolicy => ({
    maxAttempts: 2,
    initialDelay: 1000,
    maxDelay: 1000,
    backoffType: 'fixed',
  }),
} as const;

/**
 * Check if an error should trigger an immediate retry
 */
export function shouldRetryImmediately(error: Error, policy: RetryPolicy): boolean {
  return policy.immediateRetryErrors?.some(
    (errType) => error.name === errType || error.message.includes(errType)
  ) ?? false;
}

/**
 * Check if an error is retryable
 */
export function isRetryable(error: Error, policy: RetryPolicy): boolean {
  // Check if error is in non-retryable list
  if (policy.nonRetryableErrors?.some(
    (errType) => error.name === errType || error.message.includes(errType)
  )) {
    return false;
  }

  return true;
}
