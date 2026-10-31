import type { IncomingMessage, ServerResponse } from 'node:http';
import http from 'node:http';
import https from 'node:https';
import { Logger } from '../../../logger/logger.js';
import { matchRoute } from '../../../utils/match-route.js';
import { rewritePath } from '../../../utils/rewrite-path.js';
import type { ConfigType } from '../../types/config.js';
import type { Context } from '../../types/context.js';
import type { Hooks } from '../../types/hooks.js';
import { LoadBalancerStrategy } from '../../types/load-balancer-strategy.js';

import type { Upstream } from '../../types/upstream.js';
import { HeadersService } from '../headers/headers-service.js';
import { HealthService } from '../health/health-service.js';
import { createBalancer } from '../load-balancers/create-balancer.js';
import type { LoadBalancer } from '../load-balancers/load-balancer.js';

const RETRY_SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);

export class HttpHandler {
  private readonly balancers = new Map<object, LoadBalancer>();
  private readonly globalBalancer: LoadBalancer;
  private readonly httpAgent = new http.Agent({ keepAlive: true });
  private readonly httpsAgent = new https.Agent({ keepAlive: true });
  private readonly healthService: HealthService;
  private readonly headersService: HeadersService;
  private readonly logger: Logger;

  constructor(
    readonly config: ConfigType,
    private readonly hooks: Hooks = {},
    headersService?: HeadersService,
    logger?: Logger,
  ) {
    this.logger = logger ?? new Logger();
    this.globalBalancer = createBalancer(
      config.balancer ?? LoadBalancerStrategy.RoundRobin,
    );

    this.headersService =
      headersService ??
      new HeadersService(config.headers, config.forwardIp ?? true);

    const allUpstreams = [
      ...new Map(
        config.routes
          .flatMap((r) => r.upstreams)
          .map((u) => [`${u.host}:${u.port}`, u]),
      ).values(),
    ];
    this.healthService = new HealthService(
      allUpstreams,
      config.healthCheck?.interval,
      config.healthCheck?.timeout,
    );
  }

  start(): void {
    this.healthService.start();
  }

  stop(): void {
    this.healthService.stop();
  }

  async handler(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const pathname = url.pathname;

    const route = matchRoute(this.config.routes, pathname);
    if (!route) {
      this.sendError(res, 502, 'Bad Gateway');
      return;
    }

    const maxBodySize = route.maxBodySize ?? this.config.maxBodySize;
    if (maxBodySize !== undefined) {
      const contentLength = Number(req.headers['content-length']);
      if (!Number.isNaN(contentLength) && contentLength > maxBodySize) {
        req.resume();
        this.sendError(res, 413, 'Payload too large.');
        return;
      }
    }

    const candidates = this.healthyCandidates(route.upstreams);
    if (candidates.length === 0) {
      await this.handleError(
        new Error(`No upstreams configured for route '${pathname}'`),
        { req, route },
        res,
      );
      return;
    }

    const balancer = this.getBalancer(route);
    const upstream = balancer.pick(candidates);

    const targetPath =
      rewritePath(pathname, route.rewrite) + (url.search ?? '');

    const ctx: Context = { req, res, route, upstream, targetPath };

    if (this.hooks.onRequest) {
      try {
        const proceed = await this.hooks.onRequest({
          req,
          route,
          upstream,
          targetPath,
        });
        if (!proceed) {
          this.sendError(res, 403, 'Forbidden');
          return;
        }
      } catch (err) {
        await this.handleError(
          err instanceof Error ? err : new Error(String(err)),
          ctx,
          res,
        );
        return;
      }
    }
    await this.forward(ctx, new Set<Upstream>([upstream]));
  }

  private forward(ctx: Context, tried: Set<Upstream>): Promise<void> {
    return new Promise((resolve) => {
      const { req, res, upstream, targetPath, route } = ctx;

      if (req.socket.destroyed) {
        resolve();
        return;
      }

      const protocol = upstream.protocol ?? 'http';
      const transport = protocol === 'https' ? https : http;

      const forwardHeaders = this.headersService.buildRequestHeaders(
        req,
        route.headers,
        upstream,
      );

      const options: http.RequestOptions = {
        hostname: upstream.host,
        port: upstream.port,
        method: req.method,
        path: targetPath,
        headers: forwardHeaders,
        timeout: route.timeout ?? this.config.timeout ?? 30000,
        agent: protocol === 'https' ? this.httpsAgent : this.httpAgent,
      };

      let settled = false;
      let bodyTooLarge = false;
      let bodyConsumed = false;

      /**
       * Claim the outcome of this attempt. Destroying the upstream request
       * emits further errors, so only the first caller may act on it.
       */
      const settle = (): boolean => {
        if (settled) return false;
        settled = true;
        res.off('close', onClientClose);
        req.off('error', fail);
        return true;
      };

      const fail = (err: unknown): void => {
        if (!settle()) return;
        proxyReq.destroy();
        this.handleError(
          err instanceof Error ? err : new Error(String(err)),
          ctx,
          res,
        ).then(resolve);
      };

      const onClientClose = (): void => {
        if (res.writableFinished || !settle()) return;
        proxyReq.destroy();
        resolve();
      };

      const proxyReq = transport.request(options, (proxyRes) => {
        res.statusCode = proxyRes.statusCode ?? 502;

        for (const [k, v] of Object.entries(proxyRes.headers)) {
          if (v !== undefined) res.setHeader(k, v);
        }

        this.headersService.applyResponseHeaders(res, route.headers);

        proxyRes.on('error', fail);

        Promise.resolve()
          .then(() => this.hooks.onResponse?.(ctx, res.statusCode))
          .then(() => {
            if (settled) return;
            proxyRes.on('end', () => {
              settle();
              resolve();
            });
            proxyRes.pipe(res, { end: true });
          }, fail);
      });

      proxyReq.on('timeout', () => {
        fail(
          new Error(
            `Upstream request timed out ${upstream.host}:${upstream.port}`,
          ),
        );
      });

      proxyReq.on('error', (err) => {
        if (settled) return;

        if (bodyTooLarge) {
          settle();
          this.sendError(res, 413, 'Payload Too Large');
          resolve();
          return;
        }

        if (!res.headersSent && this.canRetry(req, bodyConsumed)) {
          tried.add(upstream);

          const remaining = route.upstreams.filter((u) => !tried.has(u));
          const healthyRemaining = remaining.filter((u) =>
            this.healthService.isHealthy(u),
          );
          const candidates =
            healthyRemaining.length > 0 ? healthyRemaining : remaining;

          if (candidates.length > 0) {
            settle();
            const balancer = this.getBalancer(route);
            const nextUpstream = balancer.pick(candidates);
            const nextCtx = { ...ctx, upstream: nextUpstream };
            this.forward(nextCtx, tried).then(resolve);
            return;
          }
        }

        fail(err);
      });

      res.on('close', onClientClose);
      req.on('error', fail);

      const maxBodySize = route.maxBodySize ?? this.config.maxBodySize;
      let bodyBytes = 0;
      req.on('data', (chunk: Buffer) => {
        bodyConsumed = true;
        if (maxBodySize === undefined) return;
        bodyBytes += chunk.length;
        if (!bodyTooLarge && bodyBytes > maxBodySize) {
          bodyTooLarge = true;
          req.resume();
          proxyReq.destroy();
        }
      });

      req.pipe(proxyReq, { end: true });
    });
  }

  /**
   * Retry is only safe when the upstream connection failed before any
   * request body was streamed. For idempotent methods with no body the
   * retry is a no-op replay; for others the body has been consumed by
   * the first pipe and cannot be re-sent.
   */
  private canRetry(req: IncomingMessage, bodyConsumed: boolean): boolean {
    if (bodyConsumed) return false;
    const method = (req.method ?? 'GET').toUpperCase();
    if (!RETRY_SAFE_METHODS.has(method)) return false;
    const clRaw = req.headers['content-length'];
    const clStr = Array.isArray(clRaw) ? clRaw[0] : clRaw;
    if (clStr !== undefined) {
      const clNum = Number(clStr);
      if (!Number.isFinite(clNum) || clNum !== 0) return false;
    }
    if (req.headers['transfer-encoding']) return false;
    return true;
  }

  healthyCandidates(upstreams: Upstream[]): Upstream[] {
    const healthy = upstreams.filter((u) => this.healthService.isHealthy(u));
    return healthy.length > 0 ? healthy : upstreams;
  }

  getBalancer(route: {
    upstreams: Upstream[];
    balancer?: LoadBalancerStrategy;
  }): LoadBalancer {
    if (!route.balancer) return this.globalBalancer;

    if (!this.balancers.has(route)) {
      this.balancers.set(route, createBalancer(route.balancer));
    }
    return this.balancers.get(route) ?? this.globalBalancer;
  }

  private async handleError(
    err: Error,
    ctx: Partial<Context>,
    res: ServerResponse,
  ): Promise<void> {
    if (res.headersSent) {
      // Too late for a status code; cutting the connection is the only way
      // to tell the client that the response is incomplete.
      res.destroy();
    } else {
      this.sendError(res, 502, 'Bad Gateway');
    }
    try {
      await this.hooks.onError?.(err, ctx);
    } catch (hookErr) {
      this.logger.error('hooks.onError threw', {
        message: String(hookErr),
      });
    }
  }

  private sendError(
    res: ServerResponse,
    status: number,
    message: string,
  ): void {
    if (res.headersSent) return;
    // Headers copied from the upstream, like content-length, don't describe this body.
    for (const name of res.getHeaderNames()) res.removeHeader(name);
    res.writeHead(status, { 'content-type': 'text/plain' });
    res.end(`${status} ${message}`);
  }
}
