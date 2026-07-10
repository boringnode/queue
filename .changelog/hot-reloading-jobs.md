# Hot Reloading Jobs

## New Feature

Workers can now execute the latest saved version of a job without restarting during development.

Enable `hotReload` when initializing the queue manager. Jobs discovered from `locations` will then
be resolved from their module again before every execution.

```typescript
await QueueManager.init({
  default: 'redis',
  adapters: {
    redis: redis({ host: 'localhost', port: 6379 }),
  },
  locations: ['./app/jobs/**/*.ts'],
  hotReload: process.env.NODE_ENV === 'development',
})
```

Hot reload integrates with [Hot Hook](https://github.com/Julien-R44/hot-hook). The queue provides
the dynamic import boundary, while the application remains responsible for installing and
initializing Hot Hook. AdonisJS applications can use `node ace serve --hmr`; standalone worker
processes must initialize Hot Hook themselves.

`Locator.registerFromGlob()` also accepts the option directly:

```typescript
await Locator.registerFromGlob(['./app/jobs/**/*.ts'], { hotReload: true })
```

## Upgrade Notes

Hot reload is disabled by default and should only be enabled in development.

Only jobs discovered from `locations` or registered with `Locator.registerFromGlob()` can be
reloaded. Jobs registered manually with `Locator.register()` do not have a module path to reload.

Changes to the set of registered jobs still require a restart. This includes adding, deleting,
moving, or renaming a job, as well as changing its configured `name`. A job that is already running
keeps its current implementation; the next execution receives the updated version.

Avoid import-time side effects in hot-reloaded job modules, since their module code can execute
again after an invalidation.
