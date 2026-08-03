export interface TrackedPromise<T> {
  readonly promise: Promise<T>
  readonly settled: boolean
}

/** Exposes whether a promise has settled without changing its result. */
export function trackPromise<T>(promise: Promise<T>): TrackedPromise<T> {
  let settled = false
  const tracked = promise.finally(() => {
    settled = true
  })

  return {
    promise: tracked,
    get settled() {
      return settled
    },
  }
}
