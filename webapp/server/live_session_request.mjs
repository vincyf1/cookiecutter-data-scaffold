export function parseLiveOffer(body) {
  if (typeof body === 'string') {
    if (!body.startsWith('v=0')) throw new Error('A WebRTC SDP offer is required.')
    return { sdp: body, input: [], resuming: false }
  }
  if (!body || typeof body !== 'object' || Array.isArray(body) ||
      Object.keys(body).some(key => !['sdp', 'history', 'resuming'].includes(key)) ||
      typeof body.sdp !== 'string' || !body.sdp.startsWith('v=0') || body.sdp.length > 128000 ||
      (body.resuming !== undefined && typeof body.resuming !== 'boolean')) {
    throw new Error('Invalid voice session request. A WebRTC SDP offer is required.')
  }
  const history = body.history ?? []
  if (!Array.isArray(history) || history.length > 25 || history.some(item =>
    !item || typeof item !== 'object' || Object.keys(item).some(key => !['role', 'content'].includes(key)) ||
    !['user', 'assistant'].includes(item.role) || typeof item.content !== 'string' || item.content.length > 1200
  ) || history.reduce((length, item) => length + item.content.length, 0) > 6000) {
    throw new Error('Invalid voice history. Supply bounded user/assistant text messages.')
  }
  return {
    sdp: body.sdp, resuming: body.resuming === true,
    input: history.map(item => ({ type: 'message', role: item.role,
      content: [{ type: item.role === 'assistant' ? 'output_text' : 'input_text', text: item.content }] })),
  }
}
