# Redis Connection Errors Go to the Logger

## Bug Fix

The connection the Redis adapter creates from options had no `error` listener. During a Redis
outage, ioredis wrote an `Unhandled error event` to stderr for each failed reconnection, and the
configured logger received nothing about it.

The adapter now listens to the errors of the connection it creates and logs them at the `error`
level through the configured logger. A connection passed to `redis(connection)` belongs to the
application, which keeps listening to its errors: the adapter adds no listener to it.

Adapter factories now receive a context with the QueueManager `logger`
(`AdapterFactoryContext`). The parameter is optional, so existing factories still work.
