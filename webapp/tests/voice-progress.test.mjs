import assert from 'node:assert/strict'
import test from 'node:test'
import { VoiceSession } from '../src/voice.ts'
import { describeStep } from '../src/progress.ts'

function harness(t) {
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] })
  const sent = []
  const session = new VoiceSession(() => {})
  session.ready = true
  session.channel = { readyState: 'open', send: payload => sent.push(JSON.parse(payload)), close() {} }
  session.audio = { muted: true }
  t.after(() => session.stop())
  return { session, sent }
}
const speak = 'Terra is reading the ingest_supabase log.'
// Advance second by second so each re-armed watch timer fires, as on a real clock.
const wait = (t, ms) => { for (let i = 0; i < ms; i += 1000) t.mock.timers.tick(1000) }

test('a step is spoken once the room has been quiet', t => {
  const { session, sent } = harness(t)
  session.watch('d1')
  t.mock.timers.tick(4000)
  session.progress('d1', speak)
  assert.equal(sent.at(-1).type, 'session.commentary.append')
  assert.equal(sent.at(-1).delegation_id, 'd1')
  assert.match(sent.at(-1).content, /no result yet/)
  assert.equal(session.audio.muted, false)
})

test('a step arriving right after speech is kept as silent context', t => {
  const { session, sent } = harness(t)
  session.watch('d1')
  t.mock.timers.tick(1000)
  session.progress('d1', speak)
  assert.equal(sent.at(-1).type, 'session.thinking.append')
})

test('nothing is spoken over the presenter', t => {
  const { session, sent } = harness(t)
  session.watch('d1')
  t.mock.timers.tick(20000)
  sent.length = 0
  session.handleMessage(JSON.stringify({ type: 'session.input_transcript.delta', delta: 'While it works, notice the lineage.', start_ms: 0, end_ms: 900 }))
  session.progress('d1', speak)
  t.mock.timers.tick(500)
  assert.deepEqual(sent.map(item => item.type), ['session.thinking.append'])
})

test('a long silence nudges GPT-Live to engage the audience, at most six times', t => {
  const { session, sent } = harness(t)
  session.watch('d1')
  session.progress('d1', speak)
  wait(t, 9000)
  const nudge = sent.find(item => item.type === 'session.instructions.append')
  assert.ok(nudge)
  assert.equal(nudge.delegation_id, 'd1')
  assert.match(nudge.content, /still running \(9s so far; latest step: Terra is reading/)
  assert.match(nudge.content, /Do not state, guess, or imply any result/)
  wait(t, 120000)
  assert.equal(sent.filter(item => item.type === 'session.instructions.append').length, 6)
})

test('GPT-Live speech resets the silence clock', t => {
  const { session, sent } = harness(t)
  session.watch('d1')
  t.mock.timers.tick(8000)
  session.handleMessage(JSON.stringify({ type: 'session.output_transcript.delta', delta: 'Checking the logs now.' }))
  t.mock.timers.tick(8000)
  assert.equal(sent.some(item => item.type === 'session.instructions.append'), false)
})

test('the verified result ends the waiting updates', t => {
  const { session, sent } = harness(t)
  session.engaged = true
  session.watch('d1')
  session.commentary('d1', 'The source renamed customer_segment.')
  t.mock.timers.tick(60000)
  session.progress('d1', speak)
  assert.deepEqual(sent.map(item => item.type), ['session.commentary.append'])
})

test('a typed request while voice is on is watched without a Live delegation ID', t => {
  const { session, sent } = harness(t)
  session.watch(null)
  t.mock.timers.tick(9000)
  assert.equal(sent.at(-1).type, 'session.instructions.append')
  assert.equal(sent.at(-1).delegation_id, null)
})

test('every delegate tool has a description that promises no result', () => {
  for (const name of ['get_dag_status', 'get_task_logs', 'get_supabase_schema', 'edit_airflow_file', 'trigger_airflow_dag', 'run_dbt_tests']) {
    assert.match(describeStep(name, { task_id: 'ingest_supabase', dataset: 'customers', path: 'src/x/ingest.py', model: 'm' }), /^Terra is /)
  }
  assert.equal(describeStep('some_new_tool'), 'Terra is running some new tool.')
})
