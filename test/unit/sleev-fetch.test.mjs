import assert from "node:assert/strict"
import { createServer } from "node:http"
import { setTimeout as delay } from "node:timers/promises"
import test from "node:test"
import { createSleevFetch } from "../../src/sleev-fetch.ts"

const CONFLICT = "This session advanced while the request was waiting. Retry with the latest conversation history or use a distinct session ID."
const gateway = "http://gateway.invalid/messages"
const log = async () => {}
const conflict = () => Response.json({ type: "error", error: { type: "invalid_request_error", message: CONFLICT } }, { status: 400 })

function message(sessionID = "session-1", signal) {
  return {
    method: "POST",
    headers: { "X-OpenCode-Session": sessionID, "x-opencode-request": "message-1", "x-api-key": "fixture-key", "sleev-harness": "opencode" },
    body: JSON.stringify({ model: "claude-sonnet-4-6", messages: [{ role: "user", content: "fixture" }], stream: true }),
    signal,
  }
}

async function cancelServer(t, handle) {
  const calls = []
  const server = createServer((req, res) => {
    calls.push({ path: req.url, method: req.method, headers: req.headers })
    if (handle) handle(req, res)
    else res.end(JSON.stringify({ cancelled: { requests: 1 } }))
  })
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve))
  t.after(async () => {
    server.closeAllConnections()
    await new Promise(resolve => server.close(resolve))
  })
  return { calls, baseURL: `http://127.0.0.1:${server.address().port}` }
}

function pendingUntilAbort(signal) {
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(signal.reason), { once: true })
  })
}

test("pre-header abort bypasses the gateway; immediate follow-up waits and recovers a cleanup conflict", { timeout: 5000 }, async t => {
  const received = Promise.withResolvers()
  const { baseURL, calls: cancellations } = await cancelServer(t, (_, res) => received.resolve(res))
  const requests = []
  const transport = createSleevFetch(baseURL, log, async (url, init) => {
    requests.push({ url, init })
    if (requests.length === 1) return pendingUntilAbort(init.signal)
    if (init.headers["X-OpenCode-Session"] === "unrelated") return new Response("other session")
    if (requests.length === 3) return conflict()
    return new Response("recovered")
  })

  const controller = new AbortController()
  const first = transport(gateway, message("session-1", controller.signal))
  const aborted = assert.rejects(first, { name: "AbortError" })
  controller.abort()
  await aborted
  const followup = message()
  const next = transport(gateway, followup)
  const cancelResponse = await received.promise
  await delay(20)
  assert.equal(requests.length, 1, "follow-up cannot overtake cancellation")
  assert.equal(await (await transport(gateway, message("unrelated"))).text(), "other session")
  cancelResponse.end(JSON.stringify({ cancelled: { requests: 1 } }))

  assert.equal(await (await next).text(), "recovered")
  assert.equal(requests.length, 4, "exactly one retry after the queued 400")
  for (const call of requests.slice(2)) {
    assert.equal(call.url, gateway, "inference still uses Sleev")
    assert.deepEqual(call.init, followup, "retry preserves body, session and request IDs")
  }
  assert.equal(cancellations.length, 1)
  assert.equal(cancellations[0].path, "/v1/sessions/session-1/cancel")
  assert.equal(cancellations[0].method, "POST")
  assert.equal(cancellations[0].headers["x-api-key"], "fixture-key")
  assert.equal(cancellations[0].headers["sleev-harness"], undefined)

  // Successful recovery consumes the marker: an unrelated later conflict is
  // not silently treated as another cancelled turn.
  const baseline = requests.length
  await transport(gateway, message())
  assert.equal(requests.length, baseline + 1)
})

for (const [name, response, expectedCalls] of [
  ["persistent session conflict is retried only once", conflict, 3],
  ["ordinary 400 is not retried", () => Response.json({ error: { type: "invalid_request_error", message: "bad tool schema" } }, { status: 400 }), 2],
  ["server error is not retried", () => new Response("unavailable", { status: 503 }), 2],
  ["malformed error body is preserved", () => new Response("not json", { status: 400 }), 2],
]) {
  test(name, async t => {
    const { baseURL } = await cancelServer(t)
    let calls = 0
    const transport = createSleevFetch(baseURL, log, async (_, init) => {
      if (++calls === 1) return pendingUntilAbort(init.signal)
      return response()
    })
    const controller = new AbortController()
    const first = transport(gateway, message("session-1", controller.signal))
    const aborted = assert.rejects(first, { name: "AbortError" })
    controller.abort()
    await aborted
    const result = await transport(gateway, message())
    const expected = response()
    assert.equal(result.status, expected.status)
    assert.equal(await result.text(), await expected.text())
    assert.equal(calls, expectedCalls)
  })
}

test("a conflict without a local interruption is not retried", async () => {
  let calls = 0
  const transport = createSleevFetch("http://127.0.0.1:1", log, async () => { calls++; return conflict() })
  assert.equal((await transport(gateway, message())).status, 400)
  assert.equal(calls, 1)
})

test("Request objects supply the abort signal and session header; session IDs are URL-encoded", async t => {
  const received = Promise.withResolvers()
  const { baseURL, calls } = await cancelServer(t, (_, res) => { res.end("{}"); received.resolve() })
  const transport = createSleevFetch(baseURL, log, async input => pendingUntilAbort(input.signal))
  const controller = new AbortController()
  const input = new Request(gateway, message("session/with?characters", controller.signal))
  const aborted = assert.rejects(transport(input), { name: "AbortError" })
  controller.abort()
  await aborted
  await received.promise
  assert.equal(calls[0].path, "/v1/sessions/session%2Fwith%3Fcharacters/cancel")
})

test("an already-aborted request neither dispatches nor cancels an unrelated active turn", async t => {
  const { baseURL, calls } = await cancelServer(t)
  const transport = createSleevFetch(baseURL, log, async () => assert.fail("must not dispatch"))
  await assert.rejects(transport(gateway, message("session-1", AbortSignal.abort())), { name: "AbortError" })
  assert.equal(calls.length, 0)
})

test("aborting a follow-up while cancellation is pending never dispatches that follow-up", async t => {
  const received = Promise.withResolvers()
  const { baseURL, calls } = await cancelServer(t, (_, res) => received.resolve(res))
  let dispatched = 0
  const transport = createSleevFetch(baseURL, log, async (_, init) => { dispatched++; return pendingUntilAbort(init.signal) })
  const firstController = new AbortController()
  const first = assert.rejects(transport(gateway, message("session-1", firstController.signal)), { name: "AbortError" })
  firstController.abort()
  await first
  const secondController = new AbortController()
  const second = assert.rejects(transport(gateway, message("session-1", secondController.signal)), { name: "AbortError" })
  secondController.abort()
  const response = await received.promise
  response.end("{}")
  await second
  assert.equal(dispatched, 1)
  assert.equal(calls.length, 1)
})

test("streaming abort cancels Meridian and the source stream exactly once", async t => {
  const received = Promise.withResolvers()
  const { baseURL, calls } = await cancelServer(t, (_, res) => { res.end("{}"); received.resolve() })
  let streamCancelled = false
  const response = new Response(new ReadableStream({ cancel() { streamCancelled = true } }), {
    headers: { "content-type": "text/event-stream" },
  })
  const transport = createSleevFetch(baseURL, log, async () => response)
  const controller = new AbortController()
  const result = await transport(gateway, message("session-1", controller.signal))
  const reading = assert.rejects(result.text(), { name: "AbortError" })
  controller.abort()
  await reading
  await received.promise
  assert.equal(streamCancelled, true)
  assert.equal(calls.length, 1)
})

test("downstream stream cancellation is forwarded even without a signal abort", async t => {
  const received = Promise.withResolvers()
  const { baseURL, calls } = await cancelServer(t, (_, res) => { res.end("{}"); received.resolve() })
  let streamCancelled = false
  const transport = createSleevFetch(baseURL, log, async () => new Response(new ReadableStream({
    cancel() { streamCancelled = true },
  }), { headers: { "content-type": "text/event-stream" } }))
  const controller = new AbortController()
  const result = await transport(gateway, message("session-1", controller.signal))
  await result.body.cancel()
  controller.abort()
  await received.promise
  assert.equal(streamCancelled, true)
  assert.equal(calls.length, 1)
})

test("completed streams preserve contents and headers and detach cancellation listeners", async t => {
  const { baseURL, calls } = await cancelServer(t)
  const contents = "event: message_stop\ndata: {}\n\n"
  const transport = createSleevFetch(baseURL, log, async () => new Response(contents, {
    headers: { "content-type": "text/event-stream", "x-fixture": "preserved" },
  }))
  const controller = new AbortController()
  const result = await transport(gateway, message("session-1", controller.signal))
  assert.equal(result.headers.get("x-fixture"), "preserved")
  assert.equal(await result.text(), contents)
  controller.abort()
  assert.equal(calls.length, 0)
})

test("cancel endpoint failure preserves the original abort and allows the next request", async t => {
  const { baseURL } = await cancelServer(t, (_, res) => { res.statusCode = 500; res.end("failed") })
  let count = 0
  const logs = []
  const transport = createSleevFetch(baseURL, async (...entry) => { logs.push(entry) }, async (_, init) => {
    if (++count === 1) return pendingUntilAbort(init.signal)
    return new Response("next")
  })
  const controller = new AbortController()
  const first = assert.rejects(transport(gateway, message("session-1", controller.signal)), { name: "AbortError" })
  controller.abort()
  await first
  assert.equal(await (await transport(gateway, message())).text(), "next")
  assert.ok(logs.some(([level, text]) => level === "warn" && text.includes("HTTP 500")))
})

test("detached requests and non-message endpoints use the original transport", async t => {
  const { baseURL, calls } = await cancelServer(t)
  let count = 0
  const transport = createSleevFetch(baseURL, log, async () => { count++; return new Response("ok") })
  const controller = new AbortController()
  const detached = message("session-1", controller.signal)
  delete detached.headers["X-OpenCode-Session"]
  await transport(gateway, detached)
  await transport("http://gateway.invalid/v1/models", message("session-1", controller.signal))
  controller.abort()
  assert.equal(count, 2)
  assert.equal(calls.length, 0)
})

test("an unresponsive cancel endpoint has a bounded wait", { timeout: 5000 }, async t => {
  const { baseURL } = await cancelServer(t, () => {})
  let count = 0
  const transport = createSleevFetch(baseURL, log, async (_, init) => {
    if (++count === 1) return pendingUntilAbort(init.signal)
    return new Response("next")
  })
  const controller = new AbortController()
  const first = assert.rejects(transport(gateway, message("session-1", controller.signal)), { name: "AbortError" })
  controller.abort()
  await first
  assert.equal(await (await transport(gateway, message())).text(), "next")
})
