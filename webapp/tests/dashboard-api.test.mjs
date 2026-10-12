import assert from 'node:assert/strict'
import test from 'node:test'
import { getDashboard } from '../src/api.ts'

test('dashboard reports an out-of-date API when the route returns HTML', async () => {
  const originalFetch = globalThis.fetch
  globalThis.fetch = async () => new Response('<!doctype html><title>Schema Therapy</title>', {
    headers: { 'content-type': 'text/html' },
  })
  try {
    await assert.rejects(getDashboard('30'), /API server.*restart/i)
  } finally {
    globalThis.fetch = originalFetch
  }
})
