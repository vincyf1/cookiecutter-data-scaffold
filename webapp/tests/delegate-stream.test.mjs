import assert from 'node:assert/strict'
import test from 'node:test'
import { delegateInvestigation } from '../src/api.ts'

function streamResponse(chunks) {
  return { ok: true, body: new ReadableStream({ start(controller) {
    for (const chunk of chunks) controller.enqueue(new TextEncoder().encode(chunk))
    controller.close()
  } }) }
}

test('updates split across chunks arrive in order before the result', async t => {
  t.mock.method(globalThis, 'fetch', async () => streamResponse([
    '{"type":"step","name":"get_task_logs","args":{"task_id":"ingest_supabase"}}\n{"type":"evi',
    'dence","name":"get_task_logs","args":{},"result":{"ok":true}}\n',
    '{"type":"result","result":{"answer":"Done.","evidence":[],"model":"gpt-5.6-terra","reasoningEffort":"medium"}}\n',
  ]))
  const updates = []
  const result = await delegateInvestigation('Why?', update => updates.push(update.type))
  assert.deepEqual(updates, ['step', 'evidence'])
  assert.equal(result.answer, 'Done.')
})

test('a server-side error line rejects with its message', async t => {
  t.mock.method(globalThis, 'fetch', async () => streamResponse(['{"type":"error","error":"Set OPENAI_API_KEY to delegate investigations."}\n']))
  await assert.rejects(delegateInvestigation('Why?'), /OPENAI_API_KEY/)
})

test('a stream that ends without a result is an error, not silence', async t => {
  t.mock.method(globalThis, 'fetch', async () => streamResponse(['{"type":"step","name":"get_dag_status","args":{}}\n']))
  await assert.rejects(delegateInvestigation('Why?'), /without a result/)
})
