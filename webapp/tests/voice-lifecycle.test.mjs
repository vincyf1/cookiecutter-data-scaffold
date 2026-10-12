import assert from 'node:assert/strict'
import test from 'node:test'
import { VoiceSession } from '../src/voice.ts'

async function flush() { for (let i = 0; i < 30; i++) await Promise.resolve() }

function harness(t, { emitStarted = true } = {}) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const peers = [], requests = [], seen = []
  let microphoneRequests = 0
  const track = { readyState: 'live', stopped: false, stop() { this.stopped = true }, addEventListener() {} }
  const stream = { getTracks: () => [track], getAudioTracks: () => [track] }
  class Channel {
    readyState = 'open'
    sent = []
    listeners = new Map()
    addEventListener(name, listener) { this.listeners.set(name, [...(this.listeners.get(name) || []), listener]) }
    emit(name, event = {}) { for (const listener of this.listeners.get(name) || []) listener(event) }
    send(raw) { this.sent.push(JSON.parse(raw)) }
    close() { this.readyState = 'closed'; this.emit('close') }
    message(event) { this.emit('message', { data: JSON.stringify(event) }) }
  }
  class Peer {
    connectionState = 'connected'
    iceConnectionState = 'connected'
    iceGatheringState = 'complete'
    channel = new Channel()
    localDescription = { sdp: 'v=0\noffer' }
    constructor() { peers.push(this) }
    createDataChannel() { return this.channel }
    addTrack() {}
    async createOffer() { return this.localDescription }
    async setLocalDescription() {}
    async setRemoteDescription() { if (emitStarted) this.channel.message({ type: 'session.started', session: { id: `live_${peers.length}` } }) }
    addEventListener() {}
    removeEventListener() {}
    close() { this.connectionState = 'closed'; this.onconnectionstatechange?.() }
    changeState(state) { this.connectionState = state; this.onconnectionstatechange?.() }
  }
  class Audio { muted = true; autoplay = false; srcObject = null; play() { return Promise.resolve() } pause() {} }
  const globals = {
    RTCPeerConnection: Peer, Audio,
    navigator: { mediaDevices: { getUserMedia: async () => { microphoneRequests++; return stream } } },
    fetch: async (url, options) => {
      requests.push({ url, options })
      return { ok: true, json: async () => ({ sdp: 'v=0\nanswer', sessionId: `live_${peers.length}` }) }
    },
  }
  for (const [name, value] of Object.entries(globals)) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, name)
    Object.defineProperty(globalThis, name, { value, writable: true, configurable: true })
    t.after(() => descriptor ? Object.defineProperty(globalThis, name, descriptor) : delete globalThis[name])
  }
  const session = new VoiceSession(event => seen.push(event))
  t.after(() => session.stop())
  return { session, peers, requests, seen, track, microphoneRequests: () => microphoneRequests }
}

test('a dropped data channel after two minutes reconnects using the same microphone', async t => {
  const h = harness(t)
  await h.session.start()
  t.mock.timers.tick(120000)
  h.peers[0].channel.close()
  t.mock.timers.tick(1000)
  await flush()
  assert.equal(h.peers.length, 2)
  assert.equal(h.microphoneRequests(), 1)
  assert.equal(h.track.stopped, false)
  assert.ok(h.seen.some(event => event.type === 'activity' && /channel.*closed/i.test(event.text)))
})

test('an expired session logs its reason and resumes context without repeating delegated work', async t => {
  const h = harness(t)
  await h.session.start()
  h.peers[0].channel.message({ type: 'session.input_transcript.delta', delta: 'Fix the downstream column.', start_ms: 0, end_ms: 1000 })
  h.peers[0].channel.message({ type: 'session.delegation.created', delegation: { id: 'old_delegation', target: 'client' } })
  t.mock.timers.tick(1000)
  h.peers[0].channel.message({ type: 'session.closed', reason: 'expired', usage: { seconds: 120 } })
  t.mock.timers.tick(1000)
  await flush()
  assert.equal(h.peers.length, 2)
  assert.ok(h.seen.some(event => event.type === 'activity' && /expired/.test(event.text) && /120/.test(event.text)))
  const body = JSON.parse(h.requests.at(-1).options.body)
  assert.ok(body.history.some(item => item.content.includes('Fix the downstream column.')))
  assert.equal(h.seen.filter(event => event.type === 'delegate').length, 1)
  h.session.commentary('old_delegation', 'The edit is deployed; validation is pending.')
  assert.equal(h.peers[1].channel.sent.at(-1).delegation_id, null)
})

test('a transient disconnected state can recover without replacing the session', async t => {
  const h = harness(t)
  await h.session.start()
  h.peers[0].changeState('disconnected')
  t.mock.timers.tick(2000)
  h.peers[0].changeState('connected')
  t.mock.timers.tick(10000)
  await flush()
  assert.equal(h.peers.length, 1)
})

test('stopping voice cancels an automatic reconnect and releases the microphone', async t => {
  const h = harness(t)
  await h.session.start()
  h.peers[0].channel.close()
  h.session.stop()
  t.mock.timers.tick(10000)
  await flush()
  assert.equal(h.peers.length, 1)
  assert.equal(h.track.stopped, true)
})

test('content termination ends voice instead of automatically reopening it', async t => {
  const h = harness(t)
  await h.session.start()
  h.peers[0].channel.message({ type: 'session.closed', reason: 'content' })
  t.mock.timers.tick(10000)
  await flush()
  assert.equal(h.peers.length, 1)
  assert.equal(h.track.stopped, true)
  assert.ok(h.seen.some(event => event.type === 'ended'))
})

test('a negotiated connection that never starts does not stay stuck forever', async t => {
  const h = harness(t, { emitStarted: false })
  await h.session.start()
  t.mock.timers.tick(21000)
  await flush()
  t.mock.timers.tick(1000)
  await flush()
  assert.ok(h.peers.length > 1)
})

test('backend results that arrive during recovery are buffered for the new session', async t => {
  const h = harness(t)
  await h.session.start()
  h.peers[0].channel.message({ type: 'session.input_transcript.delta', delta: 'Check the DAG.', start_ms: 0, end_ms: 1000 })
  h.peers[0].channel.message({ type: 'session.delegation.created', delegation: { id: 'in_flight', target: 'client' } })
  t.mock.timers.tick(1000)
  h.peers[0].channel.close()
  h.session.commentary('in_flight', 'The DAG is still running.')
  t.mock.timers.tick(1000)
  await flush()
  assert.equal(h.peers[1].channel.sent.at(-1).content, 'The DAG is still running.')
  assert.equal(h.peers[1].channel.sent.at(-1).delegation_id, null)
  assert.equal(h.seen.filter(event => event.type === 'delegate').length, 1)
})

test('recovery stops after five failed attempts instead of retrying indefinitely', async t => {
  const h = harness(t)
  await h.session.start()
  globalThis.fetch = async () => { throw new Error('Offline') }
  h.peers[0].channel.close()
  for (let attempt = 0; attempt < 6; attempt++) { t.mock.timers.tick(10000); await flush() }
  assert.equal(h.peers.length, 6)
  assert.equal(h.track.stopped, true)
  assert.ok(h.seen.some(event => event.type === 'ended' && /five attempts/.test(event.reason)))
})

test('stopping during microphone permission cannot create a late voice connection', async t => {
  const h = harness(t)
  let resolveMicrophone
  navigator.mediaDevices.getUserMedia = () => new Promise(resolve => { resolveMicrophone = resolve })
  const starting = h.session.start()
  h.session.stop()
  resolveMicrophone({ getTracks: () => [h.track] })
  await starting
  assert.equal(h.peers.length, 0)
  assert.equal(h.track.stopped, true)
})
