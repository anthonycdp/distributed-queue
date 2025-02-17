import { beforeAll, afterAll, vi } from 'vitest';

// Mock environment variables for tests
process.env['REDIS_HOST'] = process.env['REDIS_HOST'] || 'localhost';
process.env['REDIS_PORT'] = process.env['REDIS_PORT'] || '6379';
process.env['LOG_LEVEL'] = 'error'; // Reduce log noise in tests
process.env['TRACING_ENABLED'] = 'false';

// Global test timeout
vi.setConfig({
  testTimeout: 30000,
  hookTimeout: 30000,
});
