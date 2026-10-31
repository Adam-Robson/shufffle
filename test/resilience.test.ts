import http from 'node:http';
import net from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ProxyServer } from '../src/lib/services/proxy/proxy-server.js';
import type { ConfigType } from '../src/lib/types/config.js';
import type { Hooks } from '../src/lib/types/hooks.js';
import { Logger } from '../src/logger/logger.js';

const silentLogger = new Logger('silent');
const UPGRADE_REQUEST =
  'GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n' +
  'Sec-WebSocket-Key: a\r\nSec-WebSocket-Version: 13\r\n\r\n';
const UPGRADE_RESPONSE =
  'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n';

const cleanups: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function portOf(server: net.Server): number {
  return (server.address() as net.AddressInfo).port;
}

async function startUpstream(
  listener?: http.RequestListener,
): Promise<http.Server> {
  const server = http.createServer(listener);
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  cleanups.push(() => {
    server.closeAllConnections();
    server.close();
  });
  return server;
}

async function startProxy(
  upstream: http.Server,
  hooks: Hooks = {},
  overrides: Partial<ConfigType> = {},
): Promise<{ proxy: ProxyServer; port: number }> {
  const proxy = new ProxyServer(
    {
      port: 0,
      host: '127.0.0.1',
      routes: [
        {
          match: '/',
          upstreams: [{ host: '127.0.0.1', port: portOf(upstream) }],
        },
      ],
      ...overrides,
    },
    hooks,
    silentLogger,
  );
  await proxy.listen();
  cleanups.push(() => {
    proxy.httpServer.closeAllConnections();
    return proxy.close(0).catch(() => {});
  });
  return { proxy, port: portOf(proxy.httpServer) };
}

/** Open a raw socket to the proxy, destroyed at the end of the test. */
function connect(port: number): net.Socket {
  const socket = net.connect(port, '127.0.0.1');
  socket.on('error', () => {});
  cleanups.push(() => {
    socket.destroy();
  });
  return socket;
}

/** Collect a client's first bytes from the proxy. */
function firstBytes(socket: net.Socket): Promise<string> {
  return new Promise((resolve) => {
    socket.once('data', (chunk) => resolve(chunk.toString()));
  });
}

/** Resolve once the client's response has been cut off or has finished. */
function clientOutcome(port: number): Promise<'ended' | 'aborted'> {
  return new Promise((resolve, reject) => {
    const req = http.get({ port, host: '127.0.0.1' }, (res) => {
      res.resume();
      res.on('end', () => resolve('ended'));
      res.on('error', () => resolve('aborted'));
    });
    req.on('error', reject);
  });
}

describe('hook failures', () => {
  const body = 'z'.repeat(50_000);

  async function fetchWithThrowingHook(onResponse: Hooks['onResponse']) {
    const upstream = await startUpstream((_req, res) => {
      res.writeHead(200, { 'content-length': body.length });
      res.end(body);
    });
    const onError = vi.fn();
    const { port } = await startProxy(upstream, { onResponse, onError });
    const res = await fetch(`http://127.0.0.1:${port}/`);
    return { res, text: await res.text(), onError, port };
  }

  it('survives a synchronous throw from onResponse', async () => {
    const { res, text, onError, port } = await fetchWithThrowingHook(() => {
      throw new Error('boom');
    });

    expect(res.status).toBe(502);
    expect(text).toBe('502 Bad Gateway');
    expect(onError).toHaveBeenCalledOnce();
    expect((await fetch(`http://127.0.0.1:${port}/`)).status).toBe(502);
  });

  it('sends a well-formed 502 when onResponse rejects', async () => {
    const { res, text, onError } = await fetchWithThrowingHook(async () => {
      throw new Error('boom');
    });

    expect(res.status).toBe(502);
    expect(res.headers.get('content-length')).not.toBe(String(body.length));
    expect(text).toBe('502 Bad Gateway');
    expect(onError).toHaveBeenCalledOnce();
  });
});

describe('WebSocket upgrades and onRequest', () => {
  async function startWsUpstream(): Promise<{
    upstream: http.Server;
    upgrades: () => number;
  }> {
    let upgrades = 0;
    const upstream = await startUpstream();
    upstream.on('upgrade', (_req, socket) => {
      upgrades++;
      socket.on('error', () => {});
      socket.write(UPGRADE_RESPONSE);
    });
    return { upstream, upgrades: () => upgrades };
  }

  it('rejects the upgrade with 403 when onRequest returns false', async () => {
    const { upstream, upgrades } = await startWsUpstream();
    const onRequest = vi.fn(() => false);
    const { port } = await startProxy(upstream, { onRequest });

    const client = connect(port);
    client.write(UPGRADE_REQUEST);

    expect(await firstBytes(client)).toContain('403 Forbidden');
    expect(onRequest).toHaveBeenCalledOnce();
    expect(upgrades()).toBe(0);
  });

  it('tunnels the upgrade when onRequest resolves true', async () => {
    const { upstream, upgrades } = await startWsUpstream();
    const onRequest = vi.fn(async () => true);
    const { port } = await startProxy(upstream, { onRequest });

    const client = connect(port);
    client.write(UPGRADE_REQUEST);

    expect(await firstBytes(client)).toContain('101 Switching Protocols');
    expect(onRequest.mock.calls[0]).toEqual([
      expect.objectContaining({ targetPath: '/' }),
    ]);
    expect(upgrades()).toBe(1);
  });

  it('fails the upgrade with 502 when onRequest throws', async () => {
    const { upstream, upgrades } = await startWsUpstream();
    const onError = vi.fn();
    const { port } = await startProxy(upstream, {
      onRequest() {
        throw new Error('auth backend down');
      },
      onError,
    });

    const client = connect(port);
    client.write(UPGRADE_REQUEST);

    expect(await firstBytes(client)).toContain('502 Bad Gateway');
    await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());
    expect(upgrades()).toBe(0);
  });

  it('tears down the tunnel if the client left during onRequest', async () => {
    const upstream = await startUpstream();
    const upstreamClosed = new Promise<void>((resolve) => {
      upstream.on('upgrade', (_req, socket) => {
        socket.on('error', () => {});
        socket.on('end', () => socket.end());
        socket.on('close', () => resolve());
        socket.resume();
      });
    });
    let allow: ((value: boolean) => void) | undefined;
    const { port } = await startProxy(upstream, {
      onRequest: () => new Promise<boolean>((r) => (allow = r)),
    });

    const client = connect(port);
    client.write(UPGRADE_REQUEST);
    await vi.waitFor(() => expect(allow).toBeDefined());
    client.destroy();
    await new Promise((r) => setTimeout(r, 50));
    allow?.(true);

    await upstreamClosed;
  });
});

describe('failures after the response has started', () => {
  it('aborts the client when the upstream dies mid-body', async () => {
    const upstream = await startUpstream((_req, res) => {
      res.writeHead(200, { 'content-length': 1000 });
      res.write('partial');
      setTimeout(() => res.socket?.destroy(), 20);
    });
    const onError = vi.fn();
    const { port } = await startProxy(upstream, { onError });

    expect(await clientOutcome(port)).toBe('aborted');
    expect(onError).toHaveBeenCalledOnce();
  });

  it('aborts the client when a stream goes idle past the timeout', async () => {
    const upstream = await startUpstream((_req, res) => {
      res.writeHead(200);
      res.write('data: 1\n\n');
    });
    const onError = vi.fn();
    const { port } = await startProxy(upstream, { onError }, { timeout: 100 });

    expect(await clientOutcome(port)).toBe('aborted');
    expect(onError).toHaveBeenCalledOnce();
  });

  it('reports a timeout before the response once, not twice', async () => {
    const upstream = await startUpstream(() => {});
    const onError = vi.fn();
    const { port } = await startProxy(upstream, { onError }, { timeout: 100 });

    const res = await fetch(`http://127.0.0.1:${port}/`);
    await new Promise((r) => setTimeout(r, 50));

    expect(res.status).toBe(502);
    expect(onError).toHaveBeenCalledOnce();
  });
});

describe('ProxyServer.close()', () => {
  it('force-closes open WebSocket tunnels after the drain timeout', async () => {
    const upstream = await startUpstream();
    upstream.on('upgrade', (_req, socket) => {
      socket.on('error', () => {});
      socket.write(UPGRADE_RESPONSE);
    });
    const { proxy, port } = await startProxy(upstream);

    const client = connect(port);
    client.write(UPGRADE_REQUEST);
    await firstBytes(client);
    const clientClosed = new Promise((r) => client.once('close', r));

    const closed = proxy.close(50).then(() => 'closed');
    const outcome = await Promise.race([
      closed,
      new Promise((r) => setTimeout(() => r('still pending'), 1000)),
    ]);

    expect(outcome).toBe('closed');
    await clientClosed;
  });
});

describe('client disconnects', () => {
  /** Wait for an upstream request's connection to be torn down. */
  function waitForClose(res: http.ServerResponse): Promise<void> {
    return new Promise((r) => res.once('close', () => r()));
  }

  it('aborts the upstream when the client leaves mid-response', async () => {
    let upstreamClosed: Promise<void> | undefined;
    const upstream = await startUpstream((_req, res) => {
      upstreamClosed = waitForClose(res);
      res.writeHead(200);
      res.write('first chunk');
    });
    const onError = vi.fn();
    const { port } = await startProxy(upstream, { onError });

    await new Promise<void>((resolve) => {
      const req = http.get({ port, host: '127.0.0.1' }, (res) => {
        res.once('data', () => {
          req.destroy();
          resolve();
        });
      });
      req.on('error', () => {});
    });

    await upstreamClosed;
    expect(onError).not.toHaveBeenCalled();
  });

  it('aborts the upstream when the client leaves mid-upload', async () => {
    let upstreamClosed: Promise<void> | undefined;
    const upstream = await startUpstream((req, res) => {
      upstreamClosed = waitForClose(res);
      req.resume();
    });
    const { port } = await startProxy(upstream);

    const client = connect(port);
    client.write(
      'POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\nabc',
    );
    await vi.waitFor(() => expect(upstreamClosed).toBeDefined());
    client.destroy();

    await upstreamClosed;
  });

  it('skips the upstream if the client leaves during onRequest', async () => {
    const onUpstream = vi.fn();
    const upstream = await startUpstream((_req, res) => {
      onUpstream();
      res.end();
    });
    let allow: ((value: boolean) => void) | undefined;
    const { port } = await startProxy(upstream, {
      onRequest: () => new Promise<boolean>((r) => (allow = r)),
    });

    const client = connect(port);
    client.write('GET / HTTP/1.1\r\nHost: x\r\n\r\n');
    await vi.waitFor(() => expect(allow).toBeDefined());
    client.destroy();
    await new Promise((r) => setTimeout(r, 50));
    allow?.(true);
    await new Promise((r) => setTimeout(r, 50));

    expect(onUpstream).not.toHaveBeenCalled();
  });
});
