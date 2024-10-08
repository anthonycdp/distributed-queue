import client from 'prom-client';
import type { Logger } from '../types/index.js';

/**
 * Metrics collector for the distributed queue
 */
export class MetricsCollector {
  private readonly registry: client.Registry;
  private readonly logger: Logger;

  // Counters
  public readonly messagesProduced: client.Counter<string>;
  public readonly messagesConsumed: client.Counter<string>;
  public readonly messagesFailed: client.Counter<string>;
  public readonly messagesDeadLettered: client.Counter<string>;
  public readonly retriesTotal: client.Counter<string>;
  public readonly idempotencyHits: client.Counter<string>;

  // Histograms
  public readonly processingDuration: client.Histogram<string>;
  public readonly queueLatency: client.Histogram<string>;

  // Gauges
  public readonly queueDepth: client.Gauge<string>;
  public readonly dlqSize: client.Gauge<string>;
  public readonly activeWorkers: client.Gauge<string>;

  constructor(logger: Logger) {
    this.registry = new client.Registry();
    this.logger = logger;

    // Add default metrics
    client.collectDefaultMetrics({ register: this.registry });

    // Initialize counters
    this.messagesProduced = new client.Counter({
      name: 'dmq_messages_produced_total',
      help: 'Total number of messages produced',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });

    this.messagesConsumed = new client.Counter({
      name: 'dmq_messages_consumed_total',
      help: 'Total number of messages consumed',
      labelNames: ['topic', 'status'] as const,
      registers: [this.registry],
    });

    this.messagesFailed = new client.Counter({
      name: 'dmq_messages_failed_total',
      help: 'Total number of messages that failed processing',
      labelNames: ['topic', 'error_type'] as const,
      registers: [this.registry],
    });

    this.messagesDeadLettered = new client.Counter({
      name: 'dmq_messages_dead_lettered_total',
      help: 'Total number of messages moved to dead letter queue',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });

    this.retriesTotal = new client.Counter({
      name: 'dmq_retries_total',
      help: 'Total number of message retries',
      labelNames: ['topic', 'attempt'] as const,
      registers: [this.registry],
    });

    this.idempotencyHits = new client.Counter({
      name: 'dmq_idempotency_hits_total',
      help: 'Total number of duplicate messages detected',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });

    // Initialize histograms
    this.processingDuration = new client.Histogram({
      name: 'dmq_processing_duration_seconds',
      help: 'Time spent processing messages',
      labelNames: ['topic'] as const,
      buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10],
      registers: [this.registry],
    });

    this.queueLatency = new client.Histogram({
      name: 'dmq_queue_latency_seconds',
      help: 'Time between message production and processing start',
      labelNames: ['topic'] as const,
      buckets: [0.1, 0.5, 1, 5, 10, 30, 60, 120, 300],
      registers: [this.registry],
    });

    // Initialize gauges
    this.queueDepth = new client.Gauge({
      name: 'dmq_queue_depth',
      help: 'Current number of messages waiting in queue',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });

    this.dlqSize = new client.Gauge({
      name: 'dmq_dlq_size',
      help: 'Current number of messages in dead letter queue',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });

    this.activeWorkers = new client.Gauge({
      name: 'dmq_active_workers',
      help: 'Number of active workers',
      labelNames: ['topic'] as const,
      registers: [this.registry],
    });
  }

  /**
   * Record a message being produced
   */
  recordProduced(topic: string): void {
    this.messagesProduced.inc({ topic });
    this.logger.debug('Message produced', { topic });
  }

  /**
   * Record a message being consumed
   */
  recordConsumed(topic: string, status: 'success' | 'failure'): void {
    this.messagesConsumed.inc({ topic, status });
    this.logger.debug('Message consumed', { topic, status });
  }

  /**
   * Record a message failure
   */
  recordFailure(topic: string, errorType: string): void {
    this.messagesFailed.inc({ topic, error_type: errorType });
    this.logger.debug('Message failed', { topic, errorType });
  }

  /**
   * Record a message being moved to DLQ
   */
  recordDeadLettered(topic: string): void {
    this.messagesDeadLettered.inc({ topic });
    this.dlqSize.inc({ topic });
    this.logger.debug('Message dead lettered', { topic });
  }

  /**
   * Record a retry attempt
   */
  recordRetry(topic: string, attempt: number): void {
    this.retriesTotal.inc({ topic, attempt: attempt.toString() });
    this.logger.debug('Message retry', { topic, attempt });
  }

  /**
   * Record idempotency hit (duplicate detected)
   */
  recordIdempotencyHit(topic: string): void {
    this.idempotencyHits.inc({ topic });
    this.logger.debug('Idempotency hit', { topic });
  }

  /**
   * Record processing duration
   */
  recordProcessingDuration(topic: string, durationMs: number): void {
    this.processingDuration.observe({ topic }, durationMs / 1000);
  }

  /**
   * Record queue latency (time from produce to process)
   */
  recordQueueLatency(topic: string, latencyMs: number): void {
    this.queueLatency.observe({ topic }, latencyMs / 1000);
  }

  /**
   * Update queue depth
   */
  updateQueueDepth(topic: string, depth: number): void {
    this.queueDepth.set({ topic }, depth);
  }

  /**
   * Update DLQ size
   */
  updateDLQSize(topic: string, size: number): void {
    this.dlqSize.set({ topic }, size);
  }

  /**
   * Update active workers count
   */
  updateActiveWorkers(topic: string, count: number): void {
    this.activeWorkers.set({ topic }, count);
  }

  /**
   * Get metrics in Prometheus format
   */
  async getMetrics(): Promise<string> {
    return this.registry.metrics();
  }

  /**
   * Get the registry for custom metrics
   */
  getRegistry(): client.Registry {
    return this.registry;
  }
}
