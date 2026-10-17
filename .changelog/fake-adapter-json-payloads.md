# Fake Adapter Stores Jobs as JSON

## Bug Fix

The Fake adapter kept each dispatched job by reference, while real adapters store it as JSON. A test
could pass on the fake and the code fail in production:

- a payload mutated after dispatch changed the recorded job;
- a `Date` stayed a `Date`, and a class instance kept its class, where a worker gets plain JSON;
- a `BigInt` dispatched fine, where real adapters throw.

The fake now stores a JSON copy of each pushed job.

## Breaking Changes

Assertions on the payload of a faked job see what a worker would get: a `Date` is an ISO string, and
a class instance is a plain object. A payload JSON cannot encode, such as a `BigInt`, makes the
dispatch reject. Update tests that compare payloads to `Date` or class instances.
