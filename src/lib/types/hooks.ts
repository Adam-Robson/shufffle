import type { Context } from './context.js';

/**
 * Hook function type definition. A hook is a function that can be executed at
 * specific points during the request handling process. It receives a Context
 * object as an argument, which contains information about:
 *   - the incoming request
 *   - the response,
 *   - the route being handled,
 *   - the upstream server,
 *   - the target path
 *  @property {function} [onRequest] - Called prior to proxying the request, including WebSocket upgrades. Returns false to cancel request.
 *  @property {function} [onResponse] - Called after the response is received from upstream.
 *  @property {function} [onError] - Called when an error occurs during request handling. Receives the error object as a second argument.
 *
 */

export interface Hooks {
  onRequest?: (context: Omit<Context, 'res'>) => boolean | Promise<boolean>;
  onResponse?: (context: Context, statusCode: number) => void | Promise<void>;
  onError?: (err: Error, ctx: Partial<Context>) => void | Promise<void>;
}
