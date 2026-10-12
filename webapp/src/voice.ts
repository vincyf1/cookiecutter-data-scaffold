export type VoiceEvent =
  | { type: 'status'; status: string }
  | { type: 'phase'; phase: 'listening' | 'hearing' | 'speaking' }
  | { type: 'activity'; text: string; kind: 'agent' | 'user' | 'error' }
  | { type: 'delegate'; request: string; delegationId: string }
  | { type: 'ended'; reason: string }

type HistoryMessage = { role: 'user' | 'assistant'; content: string }
type PendingResult = { delegationId: string | null; content: string; epoch: number; speak: boolean }
// Backend work GPT-Live is waiting on. Spoken requests carry a Live delegation ID; typed requests use null.
type Watch = { delegationId: string | null; epoch: number; startedAt: number; step: string; nudges: number }

const INPUT_GAP_MS = 1200
const PLAYBACK_QUIET_MS = 300
const QUIET_MS = 850
const DISCONNECT_GRACE_MS = 5000
const STARTUP_TIMEOUT_MS = 20000
const MAX_RECONNECTS = 5
// While Terra works, speak a step update once the room has been quiet this long...
const PROGRESS_QUIET_MS = 4000
// ...and if nothing has been said for this long, nudge GPT-Live to engage the audience.
const SILENCE_NUDGE_MS = 9000
const MAX_NUDGES = 6
const WATCH_TICK_MS = 1000

export class VoiceSession {
  private peer?: RTCPeerConnection
  private channel?: RTCDataChannel
  private stream?: MediaStream
  private audio?: HTMLAudioElement
  private onEvent: (event: VoiceEvent) => void
  private inputText = ''
  private lastInputEndMs = -1
  private lastInputAt = 0
  private engaged = false
  private announced = false
  private delegatedThisTurn = false
  // Live delegation ID → its offset_ms (NaN when unknown). The event carries no request text.
  private pendingDelegations = new Map<string, number>()
  private handledDelegations = new Set<string>()
  private activeDelegations = new Map<string, { epoch: number; request: string }>()
  private quietTimer?: ReturnType<typeof setTimeout>
  private delegationTimer?: ReturnType<typeof setTimeout>
  private playbackTimer?: ReturnType<typeof setTimeout>
  private outputTimer?: ReturnType<typeof setTimeout>
  private outputText = ''
  private closed = false
  private running = false
  private ready = false
  private epoch = 0
  private sessionId = ''
  private connectedAt = 0
  private retries = 0
  private reconnectTimer?: ReturnType<typeof setTimeout>
  private disconnectTimer?: ReturnType<typeof setTimeout>
  private startupTimer?: ReturnType<typeof setTimeout>
  private connectAbort?: AbortController
  private history: HistoryMessage[] = []
  private inputHistoryEntry?: HistoryMessage
  private pendingResults: PendingResult[] = []
  private phase: 'listening' | 'hearing' | 'speaking' = 'listening'
  private watches = new Map<string, Watch>()
  private watchTimer?: ReturnType<typeof setTimeout>
  private lastSoundAt = 0

  constructor(onEvent: (event: VoiceEvent) => void) {
    this.onEvent = onEvent
  }

  private setPhase(phase: 'listening' | 'hearing' | 'speaking') {
    if (this.phase === phase) return
    this.phase = phase
    this.onEvent({ type: 'phase', phase })
  }

  async start() {
    if (this.running) return
    this.closed = false
    this.running = true
    const epoch = ++this.epoch
    this.onEvent({ type: 'status', status: 'Connecting voice…' })
    try {
      this.audio = new Audio()
      this.audio.autoplay = true
      this.audio.muted = true
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      if (this.closed || epoch !== this.epoch) {
        stream.getTracks().forEach(track => track.stop())
        return
      }
      this.stream = stream
      stream.getTracks().forEach(track => track.addEventListener('ended', () => {
        if (!this.closed) this.end('Microphone capture ended. Check microphone permission, then start voice again.')
      }))
      await this.connect(false)
    } catch (error) {
      if (this.closed) return
      const message = error instanceof Error ? error.message : String(error)
      this.end(message)
      throw error
    }
  }

  private async connect(resuming: boolean) {
    const epoch = this.epoch
    const peer = new RTCPeerConnection()
    this.peer = peer
    this.ready = false
    this.connectedAt = Date.now()
    const current = () => !this.closed && this.peer === peer && this.epoch === epoch
    peer.ontrack = event => {
      if (!current() || !this.audio) return
      this.audio.srcObject = event.streams[0]
      this.audio.play().catch(error => {
        if (current()) this.onEvent({ type: 'activity', kind: 'error', text: `Voice playback failed: ${String(error)}` })
      })
    }
    peer.onconnectionstatechange = () => {
      if (!current()) return
      const state = peer.connectionState
      if (state === 'failed' || state === 'closed') this.recover(`WebRTC connection ${state}`)
      else if (state === 'disconnected') this.waitForTransport(peer, 'WebRTC connection disconnected')
      else if (state === 'connected') {
        clearTimeout(this.disconnectTimer)
        this.disconnectTimer = undefined
        if (this.ready) this.onEvent({ type: 'status', status: 'Listening for requests' })
      }
    }
    peer.oniceconnectionstatechange = () => {
      if (!current()) return
      if (peer.iceConnectionState === 'failed') this.recover('WebRTC ICE failed')
      else if (peer.iceConnectionState === 'disconnected') this.waitForTransport(peer, 'WebRTC ICE disconnected')
      else if (['connected', 'completed'].includes(peer.iceConnectionState)) {
        clearTimeout(this.disconnectTimer)
        this.disconnectTimer = undefined
      }
    }
    try {
      this.stream!.getTracks().forEach(track => peer.addTrack(track, this.stream!))
      const channel = peer.createDataChannel('oai-events')
      this.channel = channel
      channel.addEventListener('message', event => { if (current()) this.handleMessage(event.data) })
      channel.addEventListener('close', () => { if (current()) this.recover('Voice data channel closed; session finalization was not received') })
      channel.addEventListener('error', () => { if (current()) this.recover('Voice data channel error') })
      const offer = await peer.createOffer()
      await peer.setLocalDescription(offer)
      await new Promise<void>(resolve => {
        if (peer.iceGatheringState === 'complete') return resolve()
        const finish = () => { clearTimeout(timer); peer.removeEventListener('icegatheringstatechange', done); resolve() }
        const done = () => { if (peer.iceGatheringState === 'complete') finish() }
        const timer = setTimeout(finish, 2500)
        peer.addEventListener('icegatheringstatechange', done)
      })
      if (!current()) return
      const controller = new AbortController()
      this.connectAbort = controller
      const requestTimer = setTimeout(() => controller.abort(), 30000)
      this.rememberInput()
      const history = [...this.history]
      if (this.activeDelegations.size) history.push({
        role: 'assistant', content: 'Application task state: backend work is already in progress. Do not restart or repeat it. Await its result. Requests: ' + [...this.activeDelegations.values()].map(item => item.request).join('; ').slice(0, 800),
      })
      while (history.reduce((length, item) => length + item.content.length, 0) > 6000) history.shift()
      let response: Response
      try {
        response = await fetch('/api/live/session', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sdp: peer.localDescription?.sdp, history: resuming ? history : [], resuming }),
          signal: controller.signal,
        })
      } finally { clearTimeout(requestTimer) }
      const body = await response.json()
      if (!current()) return
      if (!response.ok) throw new Error(body.error || `Voice connection failed (${response.status})`)
      if (typeof body.sdp !== 'string') throw new Error('OpenAI did not return a WebRTC answer.')
      this.sessionId = body.sessionId || ''
      await peer.setRemoteDescription({ type: 'answer', sdp: body.sdp })
      if (current() && !this.ready) this.startupTimer = setTimeout(() => {
        if (current() && !this.ready) this.recover('Voice session did not emit session.started within 20 seconds')
      }, STARTUP_TIMEOUT_MS)
    } catch (error) {
      if (!current()) return
      if (!resuming) throw error
      this.recover(error instanceof Error ? error.message : String(error))
    }
  }

  private waitForTransport(peer: RTCPeerConnection, reason: string) {
    if (this.disconnectTimer) return
    this.onEvent({ type: 'status', status: 'Voice connection interrupted…' })
    this.disconnectTimer = setTimeout(() => {
      this.disconnectTimer = undefined
      if (!this.closed && this.peer === peer && (peer.connectionState === 'disconnected' || peer.iceConnectionState === 'disconnected')) this.recover(reason)
    }, DISCONNECT_GRACE_MS)
  }

  private recover(reason: string) {
    if (this.closed || this.reconnectTimer) return
    this.onEvent({ type: 'activity', kind: 'error', text: `${reason} · session ${this.sessionId || 'unknown'} · ${Math.round((Date.now() - this.connectedAt) / 1000)}s since connection setup` })
    this.rememberInput()
    this.flushOutput()
    this.settleInput()
    this.releaseTransport()
    this.inputText = ''
    this.inputHistoryEntry = undefined
    this.lastInputEndMs = -1
    this.lastInputAt = 0
    this.announced = false
    this.delegatedThisTurn = false
    this.pendingDelegations.clear()
    this.handledDelegations.clear()
    if (++this.retries > MAX_RECONNECTS) {
      this.end('Voice reconnection failed after five attempts. Check the connection and start voice again.')
      return
    }
    const delay = Math.min(1000 * 2 ** (this.retries - 1), 10000)
    this.onEvent({ type: 'status', status: `Reconnecting voice… (${this.retries}/${MAX_RECONNECTS})` })
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined
      if (!this.closed) void this.connect(true)
    }, delay)
  }

  private remember(role: HistoryMessage['role'], content: string) {
    const entry = { role, content: content.slice(0, 1200) }
    this.history.push(entry)
    this.trimHistory()
    return entry
  }

  private trimHistory() {
    while (this.history.length > 24 || this.history.reduce((length, item) => length + item.content.length, 0) > 6000) this.history.shift()
  }

  private rememberInput() {
    if (!this.inputText.trim()) return
    if (this.inputHistoryEntry && this.history.includes(this.inputHistoryEntry)) {
      this.inputHistoryEntry.content = this.inputText.trim().slice(0, 1200)
      this.trimHistory()
    } else this.inputHistoryEntry = this.remember('user', this.inputText.trim())
  }

  private flushOutput() {
    if (this.outputText.trim()) {
      this.remember('assistant', this.outputText.trim())
      this.onEvent({ type: 'activity', kind: 'agent', text: this.outputText.trim() })
    }
    this.outputText = ''
  }

  private setAudible(audible: boolean) {
    if (this.audio) this.audio.muted = !audible
  }

  private settleInput() {
    this.rememberInput()
    if (this.phase === 'hearing') this.setPhase('listening')
    if (!this.inputText.trim() || (!this.engaged && !this.pendingDelegations.size)) return
    this.engaged = true
    this.setAudible(true)
    if (!this.announced) this.onEvent({ type: 'activity', kind: 'user', text: this.inputText.trim() })
    this.announced = true
    this.startDelegations()
  }

  // GPT-Live has already chosen to delegate, so start the worker without waiting for the room to go quiet.
  // Waiting for silence held Terra (and the evidence pane) back for as long as the presenter kept talking.
  private startDelegations() {
    if (!this.inputText.trim() || !this.pendingDelegations.size) return
    this.engaged = true
    if (!this.announced) this.onEvent({ type: 'activity', kind: 'user', text: this.inputText.trim() })
    this.announced = true
    for (const delegationId of this.pendingDelegations.keys()) {
      if (this.delegatedThisTurn) break
      if (this.handledDelegations.has(delegationId)) continue
      this.handledDelegations.add(delegationId)
      this.delegatedThisTurn = true
      this.activeDelegations.set(delegationId, { epoch: this.epoch, request: this.inputText.trim() })
      this.watch(delegationId)
      this.onEvent({ type: 'delegate', request: this.inputText.trim(), delegationId })
    }
    this.pendingDelegations.clear()
  }

  // Give the request transcript a moment to arrive, then start. Later speech does not reset this timer.
  private scheduleDelegation() {
    if (this.delegationTimer || !this.pendingDelegations.size) return
    this.delegationTimer = setTimeout(() => { this.delegationTimer = undefined; this.startDelegations() }, QUIET_MS)
  }

  private handleMessage(raw: string) {
    let event: Record<string, any>
    try { event = JSON.parse(raw) } catch { return }
    if (event.type === 'session.started') {
      this.ready = true
      this.retries = 0
      clearTimeout(this.startupTimer)
      this.sessionId = event.session?.id || this.sessionId
      this.onEvent({ type: 'status', status: 'Listening for requests' })
      this.setPhase('listening')
      this.onEvent({ type: 'activity', kind: 'agent', text: `Voice session connected · ${this.sessionId || 'session ID unavailable'}` })
      for (const result of this.pendingResults.splice(0)) this.deliverResult(result)
    }
    if (event.type === 'session.closed') {
      const reason = typeof event.reason === 'string' ? event.reason : 'unknown'
      const usage = Number.isFinite(event.usage?.seconds) ? ` · ${event.usage.seconds}s voice usage` : ''
      if (['content', 'close_requested'].includes(reason)) this.end(`GPT-Live session ended: ${reason}${usage}`)
      else this.recover(`GPT-Live session ended: ${reason}${usage}`)
      return
    }
    if (event.type === 'session.input_transcript.delta' && typeof event.delta === 'string') {
      this.setPhase('hearing')
      const now = Date.now()
      const startMs = Number(event.start_ms)
      if (this.inputText && ((Number.isFinite(startMs) && startMs - this.lastInputEndMs > INPUT_GAP_MS) || now - this.lastInputAt > INPUT_GAP_MS)) {
        // Speech that started after a pending delegation belongs to a new turn, so the delegation serves the earlier text.
        // Otherwise keep it: its request transcript can arrive after session.delegation.created.
        if ([...this.pendingDelegations.values()].some(offset => Number.isFinite(offset) && Number.isFinite(startMs) && startMs >= offset)) this.startDelegations()
        this.rememberInput()
        this.inputText = ''
        this.inputHistoryEntry = undefined
        this.engaged = false
        this.announced = false
        this.delegatedThisTurn = false
        this.flushOutput()
      }
      this.lastInputAt = now
      this.lastSoundAt = now
      if (Number.isFinite(Number(event.end_ms))) this.lastInputEndMs = Number(event.end_ms)
      this.setAudible(false)
      this.inputText += event.delta
      clearTimeout(this.playbackTimer)
      this.playbackTimer = setTimeout(() => this.setAudible(true), PLAYBACK_QUIET_MS)
      clearTimeout(this.quietTimer)
      this.quietTimer = setTimeout(() => this.settleInput(), QUIET_MS)
      this.scheduleDelegation()
    }
    if (event.type === 'session.delegation.created' && event.delegation?.target === 'client' && typeof event.delegation.id === 'string') {
      this.engaged = true
      this.pendingDelegations.set(event.delegation.id, Number(event.offset_ms ?? NaN))
      clearTimeout(this.quietTimer)
      this.quietTimer = setTimeout(() => this.settleInput(), QUIET_MS)
      this.scheduleDelegation()
    }
    if (event.type === 'session.output_transcript.delta' && typeof event.delta === 'string') {
      this.setPhase('speaking')
      this.engaged = true
      if (Date.now() - this.lastInputAt >= PLAYBACK_QUIET_MS) this.setAudible(true)
      this.outputText += event.delta
      this.lastSoundAt = Date.now()
      clearTimeout(this.outputTimer)
      this.outputTimer = setTimeout(() => { this.flushOutput(); this.setPhase('listening') }, 900)
    }
    if (event.type === 'error') this.onEvent({ type: 'activity', kind: 'error', text: `GPT-Live error${event.error?.code ? ` (${event.error.code})` : ''}: ${event.error?.message || 'Unknown session error'} · session ${this.sessionId || 'unknown'}` })
  }

  private send(payload: unknown) { if (this.channel?.readyState === 'open') this.channel.send(JSON.stringify(payload)) }

  /** Start keeping the audience engaged while backend work runs. Idempotent per delegation. */
  watch(delegationId: string | null) {
    if (this.closed) return
    const key = delegationId ?? 'direct'
    if (!this.watches.has(key)) {
      this.watches.set(key, { delegationId, epoch: this.epoch, startedAt: Date.now(), step: '', nudges: 0 })
      this.lastSoundAt = Math.max(this.lastSoundAt, Date.now())
    }
    if (!this.watchTimer) this.watchTimer = setTimeout(() => this.tickWatches(), WATCH_TICK_MS)
  }

  /** A step the delegate just started. Spoken when the room is quiet, otherwise kept as silent context. */
  progress(delegationId: string | null, step: string) {
    const watch = this.watches.get(delegationId ?? 'direct')
    if (this.closed || !watch) return
    watch.step = step
    const speak = this.canFillSilence(PROGRESS_QUIET_MS)
    if (speak) { this.lastSoundAt = Date.now(); this.setAudible(true) }
    this.send({ type: speak ? 'session.commentary.append' : 'session.thinking.append', event_id: crypto.randomUUID(),
      delegation_id: this.liveDelegationId(watch), content: speak ? `Progress update, no result yet: ${step}` : `Backend progress: ${step}` })
  }

  private tickWatches() {
    this.watchTimer = undefined
    if (this.closed || !this.watches.size) return
    const watch = [...this.watches.values()][0]
    if (watch.nudges < MAX_NUDGES && this.canFillSilence(SILENCE_NUDGE_MS)) {
      watch.nudges++
      this.lastSoundAt = Date.now()
      this.setAudible(true)
      const seconds = Math.round((Date.now() - watch.startedAt) / 1000)
      this.send({ type: 'session.instructions.append', event_id: crypto.randomUUID(), delegation_id: this.liveDelegationId(watch),
        content: `The backend investigation is still running (${seconds}s so far${watch.step ? `; latest step: ${watch.step}` : ''}). ` +
          'Say one short, lively sentence to the audience so the room is not silent: what is being checked and why it matters, or a light remark about data pipelines. ' +
          'Vary it from anything you said earlier. Do not state, guess, or imply any result. Then stop and wait.' })
    }
    this.watchTimer = setTimeout(() => this.tickWatches(), WATCH_TICK_MS)
  }

  // Never talk over the presenter or over GPT-Live itself, and only on a live, started session.
  private canFillSilence(quietMs: number) {
    return this.ready && this.channel?.readyState === 'open' && this.phase === 'listening' && Date.now() - this.lastSoundAt >= quietMs
  }

  // After a reconnect the old session's delegation ID is unknown to the new session.
  private liveDelegationId(watch: Watch) {
    return watch.epoch === this.epoch ? watch.delegationId : null
  }

  commentary(delegationId: string | null, content: string) {
    if (this.closed) return
    this.watches.delete(delegationId ?? 'direct')
    const origin = delegationId ? this.activeDelegations.get(delegationId) : undefined
    if (delegationId) this.activeDelegations.delete(delegationId)
    this.remember('assistant', `Verified backend update: ${content}`)
    const result = { delegationId, content: content.slice(0, 1800), epoch: origin?.epoch ?? this.epoch, speak: !delegationId || this.engaged }
    if (this.channel?.readyState !== 'open' || !this.ready) { this.pendingResults.push(result); return }
    this.deliverResult(result)
  }

  private deliverResult(result: PendingResult) {
    const speak = result.speak && (this.engaged || !result.delegationId)
    if (speak) { this.engaged = true; this.setAudible(true) }
    this.send({ type: speak ? 'session.commentary.append' : 'session.thinking.append', event_id: crypto.randomUUID(),
      delegation_id: result.epoch === this.epoch ? result.delegationId : null, content: result.content })
  }

  private releaseTransport() {
    ++this.epoch
    this.ready = false
    clearTimeout(this.quietTimer)
    clearTimeout(this.delegationTimer)
    this.delegationTimer = undefined
    clearTimeout(this.playbackTimer)
    clearTimeout(this.outputTimer)
    clearTimeout(this.disconnectTimer)
    clearTimeout(this.startupTimer)
    this.disconnectTimer = undefined
    this.connectAbort?.abort()
    this.connectAbort = undefined
    const channel = this.channel
    const peer = this.peer
    this.channel = undefined
    this.peer = undefined
    channel?.close()
    peer?.close()
    if (this.audio) { this.audio.muted = true; this.audio.srcObject = null }
  }

  private end(reason: string) {
    this.onEvent({ type: 'activity', kind: 'error', text: reason })
    this.stop()
    this.onEvent({ type: 'status', status: 'Voice off' })
    this.onEvent({ type: 'ended', reason })
  }

  stop() {
    this.closed = true
    this.running = false
    clearTimeout(this.reconnectTimer)
    this.reconnectTimer = undefined
    this.setAudible(false)
    this.send({ type: 'session.close', event_id: crypto.randomUUID() })
    this.releaseTransport()
    this.stream?.getTracks().forEach(track => track.stop())
    if (this.audio) this.audio.srcObject = null
    this.stream = undefined
    this.activeDelegations.clear()
    this.watches.clear()
    clearTimeout(this.watchTimer)
    this.watchTimer = undefined
    this.pendingResults = []
  }
}
