import { describe, it, expect, beforeEach } from 'vitest';
import { MetricsCollector } from '../src/observability/metrics.js';
import { createLogger } from '../src/observability/logger.js';

describe('MetricsCollector', () => {
  let collector: MetricsCollector;
  const logger = createLogger('error');

  beforeEach(() => {
    collector = new MetricsCollector(logger);
  });

  it('should record messages produced', async () => {
    collector.recordProduced('email');
    collector.recordProduced('email');
    collector.recordProduced('notifications');

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_messages_produced_total{topic="email"} 2');
    expect(metrics).toContain('dmq_messages_produced_total{topic="notifications"} 1');
  });

  it('should record messages consumed', async () => {
    collector.recordConsumed('email', 'success');
    collector.recordConsumed('email', 'success');
    collector.recordConsumed('email', 'failure');

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_messages_consumed_total{topic="email",status="success"} 2');
    expect(metrics).toContain('dmq_messages_consumed_total{topic="email",status="failure"} 1');
  });

  it('should record failures', async () => {
    collector.recordFailure('email', 'TimeoutError');
    collector.recordFailure('email', 'ConnectionError');

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_messages_failed_total{topic="email",error_type="TimeoutError"} 1');
    expect(metrics).toContain('dmq_messages_failed_total{topic="email",error_type="ConnectionError"} 1');
  });

  it('should record dead lettered messages', async () => {
    collector.recordDeadLettered('email');
    collector.recordDeadLettered('email');
    collector.recordDeadLettered('orders');

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_messages_dead_lettered_total{topic="email"} 2');
    expect(metrics).toContain('dmq_messages_dead_lettered_total{topic="orders"} 1');
  });

  it('should record retries', async () => {
    collector.recordRetry('email', 1);
    collector.recordRetry('email', 2);
    collector.recordRetry('email', 1);

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_retries_total{topic="email",attempt="1"} 2');
    expect(metrics).toContain('dmq_retries_total{topic="email",attempt="2"} 1');
  });

  it('should record idempotency hits', async () => {
    collector.recordIdempotencyHit('email');
    collector.recordIdempotencyHit('orders');

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_idempotency_hits_total{topic="email"} 1');
    expect(metrics).toContain('dmq_idempotency_hits_total{topic="orders"} 1');
  });

  it('should record processing duration', async () => {
    collector.recordProcessingDuration('email', 100);
    collector.recordProcessingDuration('email', 200);
    collector.recordProcessingDuration('email', 500);

    const metrics = await collector.getMetrics();

    // Should contain histogram buckets (labels are alphabetically ordered by prom-client)
    expect(metrics).toContain('dmq_processing_duration_seconds_bucket{le="0.1",topic="email"}');
    expect(metrics).toContain('dmq_processing_duration_seconds_bucket{le="0.5",topic="email"}');
    expect(metrics).toContain('dmq_processing_duration_seconds_bucket{le="1",topic="email"}');
  });

  it('should record queue latency', async () => {
    collector.recordQueueLatency('email', 50);
    collector.recordQueueLatency('email', 150);
    collector.recordQueueLatency('email', 500);

    const metrics = await collector.getMetrics();

    // Labels are alphabetically ordered by prom-client (le comes before topic)
    expect(metrics).toContain('dmq_queue_latency_seconds_bucket{le="0.1",topic="email"');
  });

  it('should update queue depth gauge', async () => {
    collector.updateQueueDepth('email', 10);
    collector.updateQueueDepth('notifications', 5);

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_queue_depth{topic="email"} 10');
    expect(metrics).toContain('dmq_queue_depth{topic="notifications"} 5');
  });

  it('should update DLQ size gauge', async () => {
    collector.updateDLQSize('email', 3);
    collector.updateDLQSize('orders', 1);

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_dlq_size{topic="email"} 3');
    expect(metrics).toContain('dmq_dlq_size{topic="orders"} 1');
  });

  it('should update active workers gauge', async () => {
    collector.updateActiveWorkers('email', 5);
    collector.updateActiveWorkers('orders', 2);

    const metrics = await collector.getMetrics();

    expect(metrics).toContain('dmq_active_workers{topic="email"} 5');
    expect(metrics).toContain('dmq_active_workers{topic="orders"} 2');
  });

  it('should include default metrics', async () => {
    const metrics = await collector.getMetrics();

    // Should include Node.js default metrics
    expect(metrics).toContain('nodejs_');
  });

  it('should return registry', () => {
    const registry = collector.getRegistry();

    expect(registry).toBeDefined();
    expect(typeof registry.metrics).toBe('function');
  });
});
