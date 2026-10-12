import assert from 'node:assert/strict'
import test from 'node:test'
import { formatEvidenceText, tokenizeLine, lineTone } from '../src/evidence-format.ts'

test('JSON is indented and keys, scalar values, and escaped strings have distinct tokens', () => {
  const text = formatEvidenceText({ task_id: 'ingest_supabase', attempt: 2, active: false, error: null }, 'json')
  assert.equal(text, '{\n  "task_id": "ingest_supabase",\n  "attempt": 2,\n  "active": false,\n  "error": null\n}')
  const tokens = tokenizeLine('  "message": "quote: \\"value\\"", "count": 2, "active": false', 'json')
  assert.ok(tokens.some(token => token.kind === 'key' && token.text === '"message"'))
  assert.ok(tokens.some(token => token.kind === 'string' && token.text.includes('value')))
  assert.ok(tokens.some(token => token.kind === 'number' && token.text === '2'))
  assert.ok(tokens.some(token => token.kind === 'boolean' && token.text === 'false'))
})

test('Airflow structured log envelopes become readable lines without losing metadata', () => {
  const payload = JSON.stringify({ content: [
    { timestamp: '2026-09-28T10:12:00Z', level: 'error', event: 'column "customer_segment" does not exist', task_id: 'ingest_supabase', try_number: 1 },
    { timestamp: '2026-09-28T10:12:01Z', level: 'info', event: 'Task finished', state: 'failed' },
  ], continuation_token: null })
  const text = formatEvidenceText(payload, 'log')
  assert.match(text, /2026-09-28T10:12:00Z\s+ERROR\s+column "customer_segment" does not exist/)
  assert.match(text, /task_id="ingest_supabase"/)
  assert.match(text, /try_number=1/)
  assert.match(text, /\n2026-09-28T10:12:01Z/)
  assert.equal(lineTone(text.split('\n')[0], 'log'), 'error')
})

test('JSONL logs are decoded while multiline tracebacks and ordinary text are preserved', () => {
  const input = '{"level":"warning","event":"Schema mismatch"}\nTraceback (most recent call last):\n  File "ingest.py", line 12\nValueError: missing field'
  const text = formatEvidenceText(input, 'log')
  assert.match(text, /^WARNING\s+Schema mismatch/)
  assert.match(text, /\n  File "ingest.py", line 12\nValueError: missing field$/)
  assert.equal(lineTone(text.split('\n')[0], 'log'), 'warning')
})

test('unknown JSON envelopes retain all fields and terminal colours are removed', () => {
  assert.equal(formatEvidenceText('{"custom":{"count":3}}', 'log'), '{\n  "custom": {\n    "count": 3\n  }\n}')
  assert.equal(formatEvidenceText('\u001b[31mERROR\u001b[0m task_id=ingest_supabase\r\n', 'log'), 'ERROR task_id=ingest_supabase\n')
})

test('highlighting preserves every character including HTML-like log content', () => {
  const line = '<script>alert("log only")</script> task_id=ingest_supabase attempt=2'
  const tokens = tokenizeLine(line, 'log')
  assert.equal(tokens.map(token => token.text).join(''), line)
  assert.ok(tokens.some(token => token.kind === 'key' && token.text === 'task_id'))
})

test('diff additions and removals have separate tones and SQL keywords are highlighted', () => {
  assert.equal(lineTone('+    customer_type', 'diff'), 'addition')
  assert.equal(lineTone('-    customer_segment', 'diff'), 'deletion')
  assert.notEqual(lineTone('+++ staged/model.sql', 'diff'), 'addition')
  assert.ok(tokenizeLine('SELECT customer_type FROM raw_customers', 'sql').some(token => token.kind === 'keyword' && token.text === 'SELECT'))
})

test('JSON view expands serialized log envelopes without changing ordinary strings', () => {
  const logs = JSON.stringify({ content: [{ level: 'error', event: 'Missing column' }], continuation_token: null })
  const formatted = formatEvidenceText({ logs, query: '{"keep":"literal"}', message: 'hello' }, 'json')
  const decoded = JSON.parse(formatted)
  assert.deepEqual(decoded.logs, JSON.parse(logs))
  assert.equal(decoded.query, '{"keep":"literal"}')
  assert.equal(decoded.message, 'hello')
  assert.match(formatted, /\n    "content": \[\n      \{/)
})
