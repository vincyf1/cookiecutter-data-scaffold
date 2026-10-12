import assert from 'node:assert/strict'
import test from 'node:test'
import { VoiceSession } from '../src/voice.ts'

function sessionHarness() {
  const sent = []
  const seen = []
  const session = new VoiceSession(event => seen.push(event))
  session.ready = true
  session.channel = { readyState: 'open', send: payload => sent.push(JSON.parse(payload)), close() {} }
  session.audio = { muted: true }
  return { session, sent, seen }
}

test('voice activity follows the user and assistant turns', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const { session, seen } = sessionHarness()
  try {
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Check the pipeline.', start_ms: 0, end_ms: 1000 }))
    assert.equal(seen.at(-1)?.phase, 'hearing')
    t.mock.timers.tick(900)
    assert.equal(seen.at(-1)?.phase, 'listening')
    session.handleMessage(JSON.stringify({ type: 'session.output_transcript.delta', delta: 'The pipeline is healthy.' }))
    assert.equal(seen.at(-1)?.phase, 'speaking')
    t.mock.timers.tick(900)
    assert.equal(seen.at(-1)?.phase, 'listening')
  } finally { session.stop() }
})

test('presentation narration without a Live delegation stays out of the worker', () => {
  const { session, seen } = sessionHarness()
  session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'I will explain the schema migration to the audience.', start_ms: 0, end_ms: 1800 }))
  session.settleInput()
  assert.equal(seen.some(event => event.type === 'delegate'), false)
  session.stop()
})

test('a direct request without a wake word can delegate to Terra', () => {
  const { session, seen } = sessionHarness()
  session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Could you inspect the revenue failure?', start_ms: 0, end_ms: 2000 }))
  session.handleMessage(JSON.stringify({ type: 'session.delegation.created', delegation: { id: 'd1', target: 'client' } }))
  session.settleInput()
  assert.deepEqual(seen.find(event => event.type === 'delegate'), { type: 'delegate', request: 'Could you inspect the revenue failure?', delegationId: 'd1' })
  session.stop()
})

test('a direct spoken reply becomes audible without a wake word', async () => {
  const { session, seen } = sessionHarness()
  try {
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'What is the pipeline status?', start_ms: 0, end_ms: 1800 }))
    await new Promise(resolve => setTimeout(resolve, 400))
    assert.equal(session.audio.muted, false)
    assert.equal(seen.some(event => event.type === 'delegate'), false)
  } finally { session.stop() }
})

test('an empty transcript cannot start a delegated investigation', () => {
  const { session, seen } = sessionHarness()
  session.handleMessage(JSON.stringify({ type: 'session.delegation.created', delegation: { id: 'd2', target: 'client' } }))
  session.settleInput()
  assert.equal(seen.some(event => event.type === 'delegate'), false)
  session.stop()
})

test('one Live delegation starts at most one worker for a turn', () => {
  const { session, seen } = sessionHarness()
  session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Please inspect the DAG.', start_ms: 0, end_ms: 1800 }))
  session.handleMessage(JSON.stringify({ type: 'session.delegation.created', delegation: { id: 'd3', target: 'client' } }))
  session.settleInput()
  session.handleMessage(JSON.stringify({ type: 'session.delegation.created', delegation: { id: 'd4', target: 'client' } }))
  session.settleInput()
  assert.equal(seen.filter(event => event.type === 'delegate').length, 1)
  session.stop()
})

test('a verified delegation result uses Live commentary', () => {
  const { session, sent } = sessionHarness()
  session.engaged = true
  session.commentary('d5', 'The missing column caused the failure.')
  assert.equal(sent[0].type, 'session.commentary.append')
  assert.equal(sent[0].delegation_id, 'd5')
  session.stop()
})

test('a late result during a new presentation turn remains quiet', () => {
  const { session, sent } = sessionHarness()
  session.commentary('d6', 'The missing column caused the failure.')
  assert.equal(sent[0].type, 'session.thinking.append')
  assert.equal(session.audio.muted, true)
  session.stop()
})

test('a late spoken reply stays audible after the old 15-second playback window', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const { session } = sessionHarness()
  session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Explain the diagnosis.', start_ms: 0, end_ms: 1000 }))
  t.mock.timers.tick(400)
  t.mock.timers.tick(16000)
  session.handleMessage(JSON.stringify({ type: 'session.output_transcript.delta', delta: 'The source contract changed.' }))
  assert.equal(session.audio.muted, false)
  session.stop()
})

test('a Live delegation starts promptly while the presenter keeps talking', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const { session, seen } = sessionHarness()
  try {
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Trigger a fresh run and tell me where it failed.' }))
    session.handleMessage(JSON.stringify({ type: 'session.delegation.created', delegation: { id: 'd7', target: 'client' } }))
    for (let i = 0; i < 4; i++) {
      t.mock.timers.tick(500)
      session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: ' While it works, let me explain the setup.' }))
    }
    const delegated = seen.find(event => event.type === 'delegate')
    assert.equal(delegated?.delegationId, 'd7')
    assert.match(delegated.request, /^Trigger a fresh run/)
  } finally { session.stop() }
})

test('a Live delegation that arrives before its transcript is not dropped', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const { session, seen } = sessionHarness()
  try {
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Say hello to the audience.', start_ms: 0, end_ms: 1500 }))
    t.mock.timers.tick(5000)
    session.handleMessage(JSON.stringify({ type: 'session.delegation.created', offset_ms: 9000, delegation: { id: 'd8', target: 'client' } }))
    t.mock.timers.tick(200)
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Trigger a fresh run.', start_ms: 7000, end_ms: 8800 }))
    t.mock.timers.tick(1000)
    assert.deepEqual(seen.filter(event => event.type === 'delegate').map(event => [event.delegationId, event.request]), [['d8', 'Trigger a fresh run.']])
  } finally { session.stop() }
})

test('a Live delegation for an earlier request keeps that request when new speech starts', t => {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const { session, seen } = sessionHarness()
  try {
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'Check the newest run.', start_ms: 0, end_ms: 1500 }))
    t.mock.timers.tick(1000)
    session.handleMessage(JSON.stringify({ type: 'session.delegation.created', offset_ms: 2500, delegation: { id: 'd9', target: 'client' } }))
    t.mock.timers.tick(300)
    session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'So, everyone, this is the key moment.', start_ms: 2800, end_ms: 4000 }))
    assert.deepEqual(seen.filter(event => event.type === 'delegate').map(event => [event.delegationId, event.request]), [['d9', 'Check the newest run.']])
  } finally { session.stop() }
})
