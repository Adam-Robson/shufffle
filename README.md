# shufffle

A zero-dependency, config-driven reverse proxy for Node.js. Define routes and upstreams in JSON, run it from the CLI, or embed it in your own server.

## Install

```bash
npm install @responsedotok/shufffle
```

Requires Node.js >= 22.

## CLI Usage

Create `proxy.config.json`:

```json
{
  "port": 8080,
  "balancer": "round-robin",
  "routes": [
    {
      "match": "/api",
      "rewrite": { "stripPrefix": "/api" },
      "upstreams": [
        { "host": "localhost", "port": 3001 },
        { "host": "localhost", "port": 3002 }
      ]
    },
    {
      "match": "/",
      "upstreams": [{ "host": "localhost", "port": 3000 }]
    }
  ]
}
```

Run it:

```bash
npx shufffle --config ./proxy.config.json
```

CLI flags: `-c/--config`, `-l/--log-level`, `-e/--env key=value`, `-b/--balancer`, `-h/--help`.

## Programmatic Usage

Create an async function, import `createProxy()`, and call it with await. pass an object
with the following shapes `{ port: number, routes: Route* }`.  instead of calling `listen()` is async and starts the server for you — `await` it; don't call `listen()` yourself.

```ts
import { createProxy } from "@responsedotok/shufffle";

async function main() {
  const proxy = await createProxy({
    port: 8080,
    routes: [
      { match: "/", upstreams: [{ host: "localhost", port: 3000 }] },
    ],
  });

  process.on("SIGTERM", () => proxy.close());
  process.on("SIGINT", () => proxy.close());
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

If you'd rather control the lifecycle yourself, construct the server directly:

```ts
import { ProxyServer } from "@responsedotok/shufffle";

const proxy = new ProxyServer({ port: 8080, routes: [/* … */] });
await proxy.listen();
```

## Features

- Path-prefix or function-based route matching
- Multiple upstreams per route with `round-robin`, `random`, or `weighted` load balancing
- Path rewrites (e.g. `stripPrefix`)
- Per-route and global header rules
- Configurable timeouts, max body size, and `X-Forwarded-For` forwarding
- WebSocket proxying, with `onRequest` hooks applied to upgrades as well as HTTP requests

## License

MIT
