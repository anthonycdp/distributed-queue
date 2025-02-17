import { describe, it, expect } from 'vitest';
import {
  calculateBackoff,
  calculateExponentialBackoff,
  BackoffStrategies,
  DefaultRetryPolicies,
  shouldRetryImmediately,
  isRetryable,
} from '../src/core/backoff.js';

describe('Backoff Strategies', () => {
  describe('calculateBackoff', () => {
    it('should calculate fixed backoff', () => {
      const config = BackoffStrategies.fixed(1000);

      expect(calculateBackoff(1, config)).toBe(1000);
      expect(calculateBackoff(2, config)).toBe(1000);
      expect(calculateBackoff(5, config)).toBe(1000);
    });

    it('should calculate exponential backoff', () => {
      const config = BackoffStrategies.exponential(1000);

      // Without jitter, delay should be 1000 * 2^(n-1)
      // With jitter, we check it's in a reasonable range
      const delay1 = calculateBackoff(1, config);
      const delay2 = calculateBackoff(2, config);
      const delay3 = calculateBackoff(3, config);

      // Allow for jitter (±25%)
      expect(delay1).toBeGreaterThan(750);
      expect(delay1).toBeLessThan(1250);
      expect(delay2).toBeGreaterThan(1500);
      expect(delay2).toBeLessThan(2500);
      expect(delay3).toBeGreaterThan(3000);
      expect(delay3).toBeLessThan(5000);
    });

    it('should respect max delay in exponential backoff', () => {
      const config = BackoffStrategies.exponential(1000, 5000);

      const delay = calculateBackoff(10, config);
      expect(delay).toBeLessThanOrEqual(6250); // 5000 + 25% jitter
    });

    it('should use custom backoff function', () => {
      const config: ReturnType<typeof BackoffStrategies.fixed> = {
        type: 'custom',
        delay: 100,
        customFunction: (attempts: number) => attempts * 500,
      };

      expect(calculateBackoff(1, config)).toBe(500);
      expect(calculateBackoff(2, config)).toBe(1000);
      expect(calculateBackoff(3, config)).toBe(1500);
    });
  });

  describe('calculateExponentialBackoff', () => {
    it('should calculate exponential delays with jitter', () => {
      const delay1 = calculateExponentialBackoff(1, 1000, undefined, true);
      const delay2 = calculateExponentialBackoff(2, 1000, undefined, true);

      expect(delay1).toBeGreaterThan(750);
      expect(delay1).toBeLessThan(1250);
      expect(delay2).toBeGreaterThan(1500);
      expect(delay2).toBeLessThan(2500);
    });

    it('should calculate exponential delays without jitter', () => {
      const delay1 = calculateExponentialBackoff(1, 1000, undefined, false);
      const delay2 = calculateExponentialBackoff(2, 1000, undefined, false);
      const delay3 = calculateExponentialBackoff(3, 1000, undefined, false);

      expect(delay1).toBe(1000);
      expect(delay2).toBe(2000);
      expect(delay3).toBe(4000);
    });

    it('should cap at max delay', () => {
      const delay = calculateExponentialBackoff(10, 1000, 5000, false);
      expect(delay).toBe(5000);
    });
  });

  describe('BackoffStrategies', () => {
    it('should create linear backoff', () => {
      const config = BackoffStrategies.linear(500);

      expect(config.type).toBe('custom');
      expect(config.customFunction?.(1)).toBe(500);
      expect(config.customFunction?.(2)).toBe(1000);
      expect(config.customFunction?.(5)).toBe(2500);
    });

    it('should create fibonacci backoff', () => {
      const config = BackoffStrategies.fibonacci(100);

      expect(config.type).toBe('custom');
      // Fib sequence used: fib(n+1) where fib(0)=0, fib(1)=1, fib(2)=1, fib(3)=2, fib(4)=3...
      // attempts=1 → fib(2) = 1 → 100
      // attempts=2 → fib(3) = 2 → 200
      // attempts=3 → fib(4) = 3 → 300
      expect(config.customFunction?.(1)).toBe(100);
      expect(config.customFunction?.(2)).toBe(200);
      expect(config.customFunction?.(3)).toBe(300);
    });
  });

  describe('DefaultRetryPolicies', () => {
    it('should have standard policy', () => {
      const policy = DefaultRetryPolicies.standard();

      expect(policy.maxAttempts).toBe(3);
      expect(policy.initialDelay).toBe(1000);
      expect(policy.maxDelay).toBe(30000);
      expect(policy.backoffType).toBe('exponential');
    });

    it('should have aggressive policy', () => {
      const policy = DefaultRetryPolicies.aggressive();

      expect(policy.maxAttempts).toBe(10);
      expect(policy.initialDelay).toBe(500);
    });

    it('should have conservative policy', () => {
      const policy = DefaultRetryPolicies.conservative();

      expect(policy.maxAttempts).toBe(3);
      expect(policy.initialDelay).toBe(5000);
      expect(policy.backoffType).toBe('decorrelated');
    });

    it('should have minimal policy', () => {
      const policy = DefaultRetryPolicies.minimal();

      expect(policy.maxAttempts).toBe(2);
      expect(policy.backoffType).toBe('fixed');
    });
  });

  describe('Retry decision helpers', () => {
    it('should determine if error should retry immediately', () => {
      const policy = {
        ...DefaultRetryPolicies.standard(),
        immediateRetryErrors: ['TimeoutError'],
      };

      const timeoutError = new Error('Connection timeout');
      timeoutError.name = 'TimeoutError';

      const otherError = new Error('Some other error');

      expect(shouldRetryImmediately(timeoutError, policy)).toBe(true);
      expect(shouldRetryImmediately(otherError, policy)).toBe(false);
    });

    it('should determine if error is retryable', () => {
      const policy = {
        ...DefaultRetryPolicies.standard(),
        nonRetryableErrors: ['ValidationError'],
      };

      const validationError = new Error('Invalid input');
      validationError.name = 'ValidationError';

      const serverError = new Error('Server unavailable');

      expect(isRetryable(validationError, policy)).toBe(false);
      expect(isRetryable(serverError, policy)).toBe(true);
    });
  });
});
