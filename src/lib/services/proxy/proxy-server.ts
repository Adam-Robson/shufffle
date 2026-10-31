import http from 'node:http';
import type { Socket } from 'node:net';
import { Logger } from '../../../logger/logger.js';
import type { ConfigType } from '../../types/config.js';
import type { Hooks } from '../../types/hooks.js';
import { HttpHandler } from '../handlers/http-handler.js';
import { WebSocketHandler } from '../handlers/websocket-handler.js';
import { HeadersService } from '../headers/headers-service.js';

/**
 * Top-level reverse-proxy server.
 *
 * Binds an HTTP server that forwards incoming requests and WebSocket
 * upgrades to upstream servers according to the provided configuration
 * and lifecycle hooks.
 *
 */
export class ProxyServer {
  private readonly server: http.Server;
  private readonly httpHandler: HttpHandler;
  private readonly wsHandler: WebSocketHandler;
  private readonly logger: Logger;
  private listening = false;

  constructor(
    config: ConfigType,
    hooks: Hooks = {},
    logger: Logger | undefined = undefined,
  ) {
    this.logger = logger ?? new Logger();

    const headersService = new HeadersService(
      config.headers,
      config.forwardIp ?? true,
    );
    this.httpHandler = new HttpHandler(
      config,
      hooks,
      headersService,
      this.logger,
    );
    this.wsHandler = new WebSocketHandler(
      this.httpHandler,
      headersService,
      hooks,
    );

    this.server = http.createServer((req, res) => {
      this.httpHandler.handler(req, res).catch(async (err) => {
        const error = err instanceof Error ? err : new Error(String(err));
        if (!res.headersSent) {
          res.writeHead(502, { 'content-type': 'text/plain' });
          res.end('502 Bad Gateway');
        }
        this.logger.error('Unhandled request error', {
          url: req.url,
          message: error.message,
        });
        try {
          await hooks.onError?.(error, { req, res });
        } catch (hookErr) {
          this.logger.error('hooks.onError threw', {
            message: String(hookErr),
          });
        }
      });
    });

    this.server.on('upgrade', (req, socket: Socket, head) => {
      this.wsHandler.upgrade(req, socket, head).catch(async (err) => {
        const error = err instanceof Error ? err : new Error(String(err));
        this.logger.error('WebSocket upgrade failed', {
          url: req.url,
          message: error.message,
        });
        try {
          await hooks.onError?.(error, { req });
        } catch (hookErr) {
          this.logger.error('hooks.onError threw', {
            message: String(hookErr),
          });
        }
      });
    });
  }

  /**
   * Start health-check probes and bind the server to the configured
   * host and port. Rejects if the underlying server emits an error
   * (e.g. EADDRINUSE) or if listen() has already been called.
   */
  listen(): Promise<void> {
    if (this.listening) {
      return Promise.reject(new Error('ProxyServer.listen() already called'));
    }
    this.listening = true;

    const { port, host = '0.0.0.0' } = this.httpHandler.config;
    return new Promise((resolve, reject) => {
      this.httpHandler.start();

      const onError = (err: Error) => {
        this.httpHandler.stop();
        this.listening = false;
        reject(err);
      };

      this.server.once('error', onError);
      this.server.listen(port, host, () => {
        this.server.removeListener('error', onError);
        const addr = this.server.address();
        const boundHost =
          typeof addr === 'object' && addr ? addr.address : host;
        const boundPort = typeof addr === 'object' && addr ? addr.port : port;
        this.logger.info(`Listening on ${boundHost}:${boundPort}`);
        resolve();
      });
    });
  }

  /**
   * Gracefully shut down the server.
   *
   * Stops health-check probes, closes idle connections immediately,
   * and force-closes remaining connections, including WebSocket tunnels,
   * after {@link drainTimeoutMs}.
   */
  close(drainTimeoutMs = 10_000): Promise<void> {
    return new Promise((resolve, reject) => {
      this.logger.info('Shutting down…');
      this.httpHandler.stop();
      this.server.closeIdleConnections();

      const timer = setTimeout(() => {
        this.server.closeAllConnections();
        this.wsHandler.destroyAll();
      }, drainTimeoutMs);
      timer.unref();

      this.server.close((err) => {
        clearTimeout(timer);
        this.listening = false;
        if (err) {
          reject(err);
        } else {
          this.logger.info('Server closed');
          resolve();
        }
      });
    });
  }

  /** Expose the underlying http.Server for advanced use (e.g. attach socket.io). */
  get httpServer(): http.Server {
    return this.server;
  }
}

/**
 * Create and start a proxy server in one call.
 *
 * @example
 * ```ts
 * const proxy = await createProxy({ port: 8080, routes: [...] });
 * process.on('SIGTERM', () => proxy.close());
 * ```
 */
export async function createProxy(
  config: ConfigType,
  hooks?: Hooks,
  logger?: Logger,
): Promise<ProxyServer> {
  const server = new ProxyServer(config, hooks, logger);
  await server.listen();
  return server;
}
