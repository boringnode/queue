# Type-Checking Without Optional Peers

## Bug Fixes

- The published declarations no longer require optional peers. With `skipLibCheck: false`, importing
  `@boringnode/queue` required `knex`, and importing `@boringnode/queue/types` required
  `@opentelemetry/api` and `@opentelemetry/instrumentation`, even when the application used neither.
  Each of these types now lives in the module of the driver or feature that needs it.
- The published declarations now compile with `@types/node` 26, which changed the type parameters of
  `diagnostics_channel.tracingChannel()`. They still compile with `@types/node` 25.

## Breaking Changes

`QueueSchemaService` is renamed `KnexQueueSchemaService` and is exported from the Knex driver module,
like `KyselyQueueSchemaService` in the Kysely driver module:

```typescript
// Before
import { QueueSchemaService } from '@boringnode/queue'

// After
import { KnexQueueSchemaService } from '@boringnode/queue/drivers/knex_adapter'
```

Its methods are unchanged. Update existing migrations that create the queue tables.

`QueueInstrumentationConfig` is no longer exported from `@boringnode/queue/types`:

```typescript
// Before
import type { QueueInstrumentationConfig } from '@boringnode/queue/types'

// After
import type { QueueInstrumentationConfig } from '@boringnode/queue/otel'
```
