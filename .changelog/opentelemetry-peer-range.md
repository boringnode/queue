# OpenTelemetry Instrumentation Peer Range

## Bug Fix

The optional `@opentelemetry/instrumentation` peer dependency was declared as `^0.200.0`. For a `0.x`
version, this range only accepts `0.200.x`, so installing the package next to any newer release
failed with `ERESOLVE`.

The range is now `>=0.200.0 <0.300.0`, which covers the instrumentation releases of the
OpenTelemetry JS SDK 2.x line.
