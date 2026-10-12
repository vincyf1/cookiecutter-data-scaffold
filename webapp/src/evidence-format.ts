export type CodeLanguage = 'json' | 'log' | 'sql' | 'python' | 'yaml' | 'diff' | 'text'
export type CodeToken = { text: string; kind: string }

const SQL_WORDS = new Set('select from where order by group having join left right inner outer on as with union all distinct case when then else end and or not null is in create table if exists alter rename column to insert into values delete update set begin commit rollback count sum coalesce cast desc asc limit true false'.split(' '))
const PYTHON_WORDS = new Set('from import def class return if elif else try except finally raise with as for in while and or not is none true false async await pass lambda yield'.split(' '))
const ERROR_WORDS = /^(?:error|critical|fatal|failed|failure|upstream_failed|traceback|\w+Error|\w+Exception)$/i
const WARNING_WORDS = /^(?:warn|warning|deprecated)$/i
const SUCCESS_WORDS = /^(?:success|succeeded|completed|passed)$/i

function cleanText(text: string) {
  return text.replace(/\u001b\[[0-9;]*m/g, '').replace(/\r\n?/g, '\n')
}

function jsonText(value: unknown) {
  return JSON.stringify(value, null, 2) ?? String(value)
}

function logRecord(value: unknown): string {
  if (typeof value === 'string') return formatLog(value)
  if (!value || typeof value !== 'object' || Array.isArray(value)) return jsonText(value)
  const row = value as Record<string, unknown>
  const messageKey = ['event', 'message', 'msg'].find(key => typeof row[key] === 'string')
  if (!messageKey) return jsonText(row)
  const timeKey = ['timestamp', 'time', 'ts'].find(key => row[key] !== undefined)
  const levelKey = ['level', 'levelname', 'severity'].find(key => row[key] !== undefined)
  const header = [timeKey ? String(row[timeKey]) : '', levelKey ? String(row[levelKey]).toUpperCase() : ''].filter(Boolean).join('  ')
  const used = new Set([messageKey, timeKey, levelKey])
  const message = String(row[messageKey]).split('\n').map(line => `${header ? '  ' : ''}${line}`).join('\n')
  const metadata = Object.entries(row).filter(([key]) => !used.has(key)).map(([key, item]) => `  ${key}=${JSON.stringify(item)}`).join('\n')
  return [header, message, metadata].filter(Boolean).join('\n')
}

function formatLog(input: string, depth = 0): string {
  if (depth > 3) return input
  let parsed: unknown
  try { parsed = JSON.parse(input) }
  catch {
    return input.split('\n').map(line => {
      try {
        const item: unknown = JSON.parse(line)
        return item && typeof item === 'object' ? logRecord(item) : line
      } catch { return line }
    }).join('\n')
  }
  if (typeof parsed === 'string') return formatLog(parsed, depth + 1)
  if (Array.isArray(parsed)) return parsed.map(logRecord).join('\n\n')
  if (parsed && typeof parsed === 'object') {
    const payload = parsed as Record<string, unknown>
    const contentKey = ['content', 'logs'].find(key => typeof payload[key] === 'string' || Array.isArray(payload[key]))
    if (contentKey) {
      const content = payload[contentKey]
      const logs = Array.isArray(content) ? content.map(logRecord).join('\n\n') : formatLog(String(content), depth + 1)
      const metadata = Object.entries(payload).filter(([key, item]) => key !== contentKey && item != null).map(([key, item]) => `${key}=${JSON.stringify(item)}`).join('\n')
      return [logs, metadata].filter(Boolean).join('\n\n')
    }
    if (['event', 'message', 'msg'].some(key => typeof payload[key] === 'string')) return logRecord(payload)
  }
  return jsonText(parsed)
}

export function formatEvidenceText(value: unknown, language: CodeLanguage): string {
  if (language === 'json') {
    // Airflow encodes its log envelope as a string; expand it for the display only.
    return JSON.stringify(value, (key, item: unknown) => {
      if ((key === 'logs' || key === 'content') && typeof item === 'string') {
        try {
          const decoded: unknown = JSON.parse(item)
          if (decoded && typeof decoded === 'object') return decoded
        } catch { /* Plain log text remains a string. */ }
      }
      return item
    }, 2) ?? String(value)
  }
  const text = cleanText(typeof value === 'string' ? value : jsonText(value))
  return language === 'log' ? formatLog(text) : text
}

export function lineTone(line: string, language: CodeLanguage): string {
  if (language === 'diff') {
    if (line.startsWith('+') && !line.startsWith('+++')) return 'addition'
    if (line.startsWith('-') && !line.startsWith('---')) return 'deletion'
    return ''
  }
  if (language !== 'log') return ''
  if (/\b(?:ERROR|CRITICAL|FATAL|Traceback|\w+Error|\w+Exception)\b/i.test(line) || /\bstate\s*[:=]\s*"?failed\b/i.test(line)) return 'error'
  if (/\bWARN(?:ING)?\b/i.test(line)) return 'warning'
  return ''
}

export function tokenizeLine(line: string, language: CodeLanguage): CodeToken[] {
  const comment = language === 'python' ? '#.*$' : language === 'sql' ? '--.*$' : language === 'yaml' ? '#.*$' : '(?!)'
  const pattern = new RegExp(`${comment}|"(?:\\\\.|[^"\\\\])*"|'(?:\\\\.|[^'\\\\])*'|\\b\\d{4}-\\d{2}-\\d{2}T[\\d:.]+(?:Z|[+-][\\d:]+)?|[A-Za-z_][\\w.-]*(?=\\s*[:=])|\\b-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?\\b|\\b[A-Za-z_][\\w.]*\\b|[{}\\[\\]:,=()]`, 'g')
  const tokens: CodeToken[] = []
  let offset = 0
  for (const match of line.matchAll(pattern)) {
    const start = match.index!
    if (start > offset) tokens.push({ text: line.slice(offset, start), kind: '' })
    const text = match[0]
    const next = line.slice(start + text.length)
    const word = text.replace(/^['"]|['"]$/g, '')
    let kind = ''
    if ((language === 'python' || language === 'yaml') && text.startsWith('#') || language === 'sql' && text.startsWith('--')) kind = 'comment'
    else if (/^\d{4}-\d{2}-\d{2}T/.test(text)) kind = 'timestamp'
    else if (/^["']/.test(text) && /^\s*:/.test(next) || /^[A-Za-z_][\w.-]*$/.test(text) && /^\s*[:=]/.test(next)) kind = 'key'
    else if (ERROR_WORDS.test(word)) kind = 'error'
    else if (WARNING_WORDS.test(word)) kind = 'warning'
    else if (SUCCESS_WORDS.test(word)) kind = 'success'
    else if (/^["']/.test(text)) kind = 'string'
    else if (/^-?\d/.test(text)) kind = 'number'
    else if (/^(true|false)$/i.test(text)) kind = 'boolean'
    else if (/^(null|none)$/i.test(text)) kind = 'null'
    else if (language === 'sql' && SQL_WORDS.has(text.toLowerCase()) || language === 'python' && PYTHON_WORDS.has(text.toLowerCase()) || /^(INFO|DEBUG)$/.test(text)) kind = 'keyword'
    else if (/^[{}\[\]:,=()]$/.test(text)) kind = 'punctuation'
    tokens.push({ text, kind })
    offset = start + text.length
  }
  if (offset < line.length) tokens.push({ text: line.slice(offset), kind: '' })
  return tokens
}

export function fileLanguage(path: string): CodeLanguage {
  if (path.endsWith('.py')) return 'python'
  if (path.endsWith('.sql')) return 'sql'
  if (/\.ya?ml$/.test(path)) return 'yaml'
  return 'text'
}
