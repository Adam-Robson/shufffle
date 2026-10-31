import type { IncomingMessage } from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import { matchRoute } from '../../../utils/match-route.js';
import { rewritePath } from '../../../utils/rewrite-path.js';
import type { Hooks } from '../../types/hooks.js';
import type { Route } from '../../types/route.js';
import type { Upstream } from '../../types/upstream.js';
import type { HeadersService } from '../headers/headers-service.js';
import type { HttpHandler } from './http-handler.js';

export class WebSocketHandler {
  private readonly sockets = new Set<net.Socket>();

  constructor(
    private readonly httpHandler: HttpHandler,
    private readonly headersService: HeadersService,
    private readonly hooks: Hooks = {},
  ) {}

  /**
   * Destroy every tracked client socket. `http.Server` no longer owns a
   * socket once it has been upgraded, so `closeAllConnections()` misses them.
   */
  destroyAll(): void {
    for (const socket of this.sockets) socket.destroy();
  }

  /**
   * Run the `onRequest` hook, then tunnel a WebSocket upgrade between the
   * client socket and a chosen upstream. Resolves once the upstream
   * connection is established and piping has been wired, or once the hook
   * has denied the upgrade; rejects on any error along the way.
   *
   * On rejection the handler has already responded to the client (502 +
   * destroy) and cleaned up; the caller's only job is to surface the
   * error (logs, hooks).
   */
  async upgrade(
    req: IncomingMessage,
    socket: net.Socket,
    head: Buffer,
  ): Promise<void> {
    this.sockets.add(socket);
    socket.once('close', () => this.sockets.delete(socket));

    const { config } = this.httpHandler;
    const url = new URL(req.url ?? '/', 'http://localhost');
    const route = matchRoute(config.routes, url.pathname);

    if (!route || route.upstreams.length === 0) {
      this.fail(socket, 502, 'Bad Gateway');
      throw new Error(
        route
          ? `No upstreams configured for route '${url.pathname}'`
          : `No matching route for WebSocket upgrade '${url.pathname}'`,
      );
    }

    const candidates = this.httpHandler.healthyCandidates(route.upstreams);
    const upstream = this.httpHandler.getBalancer(route).pick(candidates);
    const targetPath =
      rewritePath(url.pathname, route.rewrite) + (url.search ?? '');

    if (this.hooks.onRequest) {
      // Node removes its socket error listener on upgrade, so an error
      // while the hook is pending would otherwise be uncaught.
      socket.on('error', () => socket.destroy());

      let proceed: boolean;
      try {
        proceed = await this.hooks.onRequest({
          req,
          route,
          upstream,
          targetPath,
        });
      } catch (err) {
        this.fail(socket, 502, 'Bad Gateway');
        throw err instanceof Error ? err : new Error(String(err));
      }

      if (!proceed) {
        this.fail(socket, 403, 'Forbidden');
        return;
      }
      if (socket.destroyed) return;
    }

    await this.tunnel(req, socket, head, route, upstream, targetPath);
  }

  private tunnel(
    req: IncomingMessage,
    socket: net.Socket,
    head: Buffer,
    route: Route,
    upstream: Upstream,
    targetPath: string,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const { config } = this.httpHandler;
      const upgradeHeaders = this.headersService.buildUpgradeHeaders(
        req,
        route.headers,
        upstream,
      );

      const requestLine = `${req.method ?? 'GET'} ${targetPath} HTTP/1.1\r\n`;
      const headerBlock = `${Object.entries(upgradeHeaders)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(', ') : v}`)
        .join('\r\n')}\r\n\r\n`;

      const useTls = upstream.protocol === 'https';
      const connectOptions = { host: upstream.host, port: upstream.port };
      let settled = false;
      let connected = false;

      const teardown = () => {
        upstreamSocket.setTimeout(0);
        if (!socket.destroyed) socket.destroy();
        if (!upstreamSocket.destroyed) upstreamSocket.destroy();
      };

      const settleResolve = () => {
        if (settled) return;
        settled = true;
        resolve();
      };

      const fail = (err: Error) => {
        if (settled) {
          teardown();
          return;
        }
        settled = true;
        if (!connected) {
          this.fail(socket, 502, 'Bad Gateway');
          upstreamSocket.setTimeout(0);
          if (!upstreamSocket.destroyed) upstreamSocket.destroy();
        } else {
          teardown();
        }
        reject(err);
      };

      const onConnect = () => {
        connected = true;
        upstreamSocket.setTimeout(0);
        upstreamSocket.write(requestLine + headerBlock);
        if (head.length > 0) upstreamSocket.write(head);
        socket.pipe(upstreamSocket);
        upstreamSocket.pipe(socket);
        settleResolve();
      };

      const upstreamSocket = useTls
        ? tls.connect(connectOptions, onConnect)
        : net.createConnection(connectOptions, onConnect);

      upstreamSocket.on('error', (err) =>
        fail(err instanceof Error ? err : new Error(String(err))),
      );
      upstreamSocket.setTimeout(route.timeout ?? config.timeout ?? 30000, () =>
        fail(
          new Error(
            `Upstream WebSocket connection timed out ${upstream.host}:${upstream.port}`,
          ),
        ),
      );

      socket.on('error', (err) =>
        fail(err instanceof Error ? err : new Error(String(err))),
      );
      socket.on('close', teardown);
      upstreamSocket.on('close', teardown);
    });
  }

  private fail(socket: net.Socket, status: number, message: string): void {
    if (!socket.destroyed && socket.writable) {
      socket.write(`HTTP/1.1 ${status} ${message}\r\n\r\n`);
    }
    socket.destroy();
  }
}
