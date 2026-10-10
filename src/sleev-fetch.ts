import type { LogFn } from "./logger"

export type Fetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>

const SESSION_CONFLICT =
  "This session advanced while the request was waiting. Retry with the latest conversation history or use a distinct session ID."
const MAX_INTERRUPTED_SESSIONS = 256

/**
 * Sleev 1.8.8 does not forward a disconnect while awaiting upstream headers.
 * Send cancellation straight to the embedded Meridian. Keep the bridge alive
 * through SSE consumption too: Bun can leave the upstream connection running
 * even after the downstream fetch has been aborted.
 */
export function createSleevFetch(
  baseURL: string,
  log: LogFn,
  fetchRequest: Fetch = fetch,
): Fetch {
  const interrupted = new Map<string, Promise<void>>()

  const cancel = async (sessionID: string, headers: Headers) => {
    try {
      const auth = new Headers()
      for (const name of ["x-api-key", "authorization"]) {
        const value = headers.get(name)
        if (value) auth.set(name, value)
      }
      // Use native fetch, not the provider transport: this must bypass Sleev
      // and must not inherit the already-aborted inference request's signal.
      const response = await fetch(
        `${baseURL}/v1/sessions/${encodeURIComponent(sessionID)}/cancel`,
        { method: "POST", headers: auth, signal: AbortSignal.timeout(2_000) },
      )
      await response.body?.cancel()
      if (!response.ok) throw new Error(`HTTP ${response.status}`)
      void log("debug", `forwarded cancellation to Meridian: ${sessionID}`).catch(() => {})
    } catch (error) {
      void log("warn", `Meridian cancellation failed: ${String(error)}`).catch(() => {})
    }
  }

  return async (input, init) => {
    const request = input instanceof Request ? input : undefined
    const method = (init?.method ?? request?.method ?? "GET").toUpperCase()
    const url = new URL(request ? request.url : String(input))
    const headers = new Headers(init?.headers ?? request?.headers)
    const sessionID = headers.get("x-opencode-session")
    if (method !== "POST" || !/^\/(v1\/)?messages$/.test(url.pathname) || !sessionID) {
      return fetchRequest(input, init)
    }

    const signal = init?.signal === undefined ? request?.signal : init.signal
    signal?.throwIfAborted()
    const previous = interrupted.get(sessionID)
    // A rapid follow-up must not overtake the out-of-band cancel and get
    // cancelled along with the old turn.
    if (previous) await previous
    signal?.throwIfAborted()

    let cancelled = false
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    const cancelUpstream = () => {
      if (cancelled) return
      cancelled = true
      interrupted.delete(sessionID)
      interrupted.set(sessionID, cancel(sessionID, headers))
      if (interrupted.size > MAX_INTERRUPTED_SESSIONS) {
        const oldest = interrupted.keys().next().value
        if (oldest !== undefined) interrupted.delete(oldest)
      }
    }
    const onAbort = () => {
      cancelUpstream()
      void reader?.cancel(signal?.reason).catch(() => {})
    }
    const detach = () => signal?.removeEventListener("abort", onAbort)
    signal?.addEventListener("abort", onAbort, { once: true })
    let streaming = false
    try {
      let response = await fetchRequest(input, init && { ...init })
      // Cancellation cleanup may change the durable generation while the
      // follow-up waits for Meridian's lease. This specific 400 occurs before
      // inference starts; replaying the same JSON once is safe. Do not retry
      // other errors, streams, or conflicts without a local interruption.
      if (previous && response.status === 400 && !request && typeof init?.body === "string") {
        const body = await response.clone().json().catch(() => undefined)
        if (body?.error?.type === "invalid_request_error" && body.error.message === SESSION_CONFLICT) {
          signal?.throwIfAborted()
          await response.body?.cancel()
          void log("info", `retrying Meridian session conflict after interruption: ${sessionID}`).catch(() => {})
          response = await fetchRequest(input, { ...init })
        }
      }
      signal?.throwIfAborted()
      if (interrupted.get(sessionID) === previous) interrupted.delete(sessionID)
      if (response.ok && response.body && response.headers.get("content-type")?.includes("text/event-stream")) {
        const source = response.body.getReader()
        reader = source
        const body = new ReadableStream<Uint8Array>({
          async pull(controller) {
            try {
              const chunk = await source.read()
              signal?.throwIfAborted()
              if (chunk.done) {
                detach()
                controller.close()
              } else {
                controller.enqueue(chunk.value)
              }
            } catch (error) {
              detach()
              controller.error(error)
            }
          },
          async cancel(reason) {
            cancelUpstream()
            detach()
            await source.cancel(reason)
          },
        })
        streaming = true
        return new Response(body, {
          status: response.status,
          statusText: response.statusText,
          headers: response.headers,
        })
      }
      return response
    } finally {
      if (!streaming) detach()
    }
  }
}
