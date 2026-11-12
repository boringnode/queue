import { createServer, connect, type Socket } from 'node:net'

/**
 * TCP proxy to a server. `silence()` stops forwarding on open connections
 * without closing them, as a stuck network path would: all of them, or the
 * one from the client port `clientPort`. Later connections are forwarded.
 */
export async function startSilentProxy(target: { host: string; port: number }) {
  const connections = new Map<number, { client: Socket; upstream: Socket; silent: boolean }>()

  const server = createServer((client) => {
    const upstream = connect(target)
    const entry = { client, upstream, silent: false }
    const clientPort = client.remotePort!
    connections.set(clientPort, entry)

    for (const [from, to] of [
      [client, upstream],
      [upstream, client],
    ]) {
      from.on('data', (chunk) => {
        if (!entry.silent) to.write(chunk)
      })
      from.on('error', () => to.destroy())
      from.on('close', () => {
        connections.delete(clientPort)
        to.destroy()
      })
    }
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))

  return {
    port: (server.address() as { port: number }).port,
    silence(clientPort?: number) {
      for (const [port, entry] of connections) {
        if (clientPort === undefined || port === clientPort) entry.silent = true
      }
    },
    async close() {
      for (const { client, upstream } of connections.values()) {
        client.destroy()
        upstream.destroy()
      }
      await new Promise((resolve) => server.close(resolve))
    },
  }
}
