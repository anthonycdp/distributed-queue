import {
  trace,
  context,
  SpanStatusCode,
  SpanKind,
  type Span as OTSpan,
  type Tracer,
  type Attributes,
} from '@opentelemetry/api';
import { BasicTracerProvider } from '@opentelemetry/sdk-trace-base';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { Resource } from '@opentelemetry/resources';
import { SEMRESATTRS_SERVICE_NAME, SEMRESATTRS_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import type { Span, Logger } from '../types/index.js';

/**
 * Tracing service for distributed tracing
 */
export class TracingService {
  private tracer!: Tracer;
  private enabled: boolean;
  private logger: Logger;

  constructor(logger: Logger, enabled: boolean = true, endpoint?: string) {
    this.enabled = enabled;
    this.logger = logger;

    if (enabled) {
      try {
        const resource = Resource.default().merge(
          new Resource({
            [SEMRESATTRS_SERVICE_NAME]: 'distributed-message-queue',
            [SEMRESATTRS_SERVICE_VERSION]: '1.0.0',
          })
        );

        const provider = new BasicTracerProvider({ resource });

        if (endpoint) {
          const exporter = new OTLPTraceExporter({
            url: endpoint,
          });
          provider.addSpanProcessor(new BatchSpanProcessor(exporter));
        }

        provider.register();
        this.tracer = trace.getTracer('distributed-queue', '1.0.0');
        this.logger.info('Tracing initialized', { endpoint });
      } catch (error) {
        this.logger.warn('Failed to initialize tracing, running without traces', {
          error: error instanceof Error ? error.message : String(error),
        });
        this.enabled = false;
      }
    }
  }

  /**
   * Start a new span
   */
  startSpan(name: string, options?: { parent?: Span; kind?: SpanKind }): Span {
    if (!this.enabled) {
      return this.createNoopSpan();
    }

    const activeContext = context.active();
    const span = this.tracer.startSpan(
      name,
      {
        kind: options?.kind ?? SpanKind.INTERNAL,
      },
      activeContext
    );

    return this.wrapSpan(span);
  }

  /**
   * Start a span for message production
   */
  startProducerSpan(topic: string, _messageId: string): Span {
    return this.startSpan(`produce ${topic}`, { kind: SpanKind.PRODUCER });
  }

  /**
   * Start a span for message consumption
   */
  startConsumerSpan(topic: string, _messageId: string, parentSpan?: Span): Span {
    return this.startSpan(`consume ${topic}`, {
      parent: parentSpan,
      kind: SpanKind.CONSUMER,
    });
  }

  /**
   * Execute a function within a span context
   */
  async withSpan<T>(name: string, fn: (span: Span) => Promise<T>): Promise<T> {
    const span = this.startSpan(name);
    try {
      const result = await fn(span);
      span.setStatus('ok');
      return result;
    } catch (error) {
      span.setStatus('error', error instanceof Error ? error.message : String(error));
      throw error;
    } finally {
      span.end();
    }
  }

  /**
   * Wrap OpenTelemetry span to our Span interface
   */
  private wrapSpan(otSpan: OTSpan): Span {
    return {
      spanId: otSpan.spanContext().spanId,
      traceId: otSpan.spanContext().traceId,
      setAttribute: (key: string, value: string | number | boolean) => {
        otSpan.setAttribute(key, value);
      },
      addEvent: (name: string, attributes?: Attributes) => {
        otSpan.addEvent(name, attributes);
      },
      setStatus: (status: 'ok' | 'error', description?: string) => {
        otSpan.setStatus({
          code: status === 'ok' ? SpanStatusCode.OK : SpanStatusCode.ERROR,
          message: description,
        });
      },
      end: () => {
        otSpan.end();
      },
    };
  }

  /**
   * Create a no-op span for when tracing is disabled
   */
  private createNoopSpan(): Span {
    return {
      spanId: '',
      traceId: '',
      setAttribute: () => {},
      addEvent: () => {},
      setStatus: () => {},
      end: () => {},
    };
  }
}

/**
 * No-op tracing service that doesn't record anything
 */
export class NoopTracingService extends TracingService {
  constructor(logger: Logger) {
    super(logger, false);
  }
}
