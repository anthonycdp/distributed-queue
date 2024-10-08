import winston from 'winston';
import type { Logger } from '../types/index.js';

/**
 * Create a Winston-based logger instance
 */
export function createLogger(level: string = 'info', service: string = 'distributed-queue'): Logger {
  const winstonLogger = winston.createLogger({
    level,
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.errors({ stack: true }),
      winston.format.json()
    ),
    defaultMeta: { service },
    transports: [
      new winston.transports.Console({
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.printf(({ timestamp, level, message, ...meta }) => {
            const metaStr = Object.keys(meta).length ? JSON.stringify(meta) : '';
            return `${timestamp} [${level}]: ${message} ${metaStr}`;
          })
        ),
      }),
    ],
  });

  return {
    debug: (message: string, meta?: Record<string, unknown>) =>
      winstonLogger.debug(message, meta),
    info: (message: string, meta?: Record<string, unknown>) =>
      winstonLogger.info(message, meta),
    warn: (message: string, meta?: Record<string, unknown>) =>
      winstonLogger.warn(message, meta),
    error: (message: string, meta?: Record<string, unknown>) =>
      winstonLogger.error(message, meta),
  };
}

/**
 * Default logger instance
 */
export const logger = createLogger(
  process.env['LOG_LEVEL'] || 'info'
);
