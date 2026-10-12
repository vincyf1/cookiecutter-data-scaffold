import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react'
import { Check, Copy, WrapText } from 'lucide-react'
import { formatEvidenceText, lineTone, tokenizeLine, type CodeLanguage } from './evidence-format'

export default function EvidenceCode({ value, language = 'json', title }: { value: unknown; language?: CodeLanguage; title?: string }) {
  const [wrap, setWrap] = useState(false)
  const [size, setSize] = useState(18)
  const [copyStatus, setCopyStatus] = useState('')
  const copyTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const text = useMemo(() => formatEvidenceText(value, language), [value, language])
  const lines = useMemo(() => text.split('\n').map(line => ({ tokens: tokenizeLine(line, language), tone: lineTone(line, language) })), [text, language])
  const errorCount = lines.filter(line => line.tone === 'error').length
  useEffect(() => () => clearTimeout(copyTimer.current), [])

  async function copy() {
    try { await navigator.clipboard.writeText(text); setCopyStatus('Copied') }
    catch { setCopyStatus('Copy failed') }
    clearTimeout(copyTimer.current)
    copyTimer.current = setTimeout(() => setCopyStatus(''), 2500)
  }

  return <section className={`evidence-code${wrap ? ' is-wrapped' : ''}`} data-language={language} style={{ '--code-font-size': `${size}px` } as CSSProperties} aria-label={title || `${language} evidence`}>
    <div className="code-toolbar">
      <div className="code-file-label"><strong>{title || language.toUpperCase()}</strong><span>{lines.length} lines</span>{errorCount > 0 && <span className="code-error-count">{errorCount} error {errorCount === 1 ? 'line' : 'lines'}</span>}</div>
      <div className="code-controls">
        <button onClick={() => setSize(current => Math.max(14, current - 2))} disabled={size <= 14} aria-label="Decrease code text size" title="Smaller text">A−</button>
        <button onClick={() => setSize(current => Math.min(24, current + 2))} disabled={size >= 24} aria-label="Increase code text size" title="Larger text for projection">A+</button>
        <button onClick={() => setWrap(current => !current)} aria-label="Wrap long lines" aria-pressed={wrap} title="Toggle line wrapping"><WrapText size={15}/><span>Wrap</span></button>
        <button onClick={copy} aria-label="Copy formatted evidence" title="Copy without line numbers">{copyStatus === 'Copied' ? <Check size={15}/> : <Copy size={15}/>}<span>{copyStatus || 'Copy'}</span></button>
      </div>
    </div>
    <div className="code-legend" aria-label="Syntax colour legend"><span className="syntax-key">key</span><span className="syntax-string">string</span><span className="syntax-number">number</span><span className="syntax-boolean">boolean</span>{language === 'log' && <><span className="syntax-warning">WARN</span><span className="syntax-error">ERROR</span></>}<span className="copy-status" aria-live="polite">{copyStatus}</span></div>
    <pre className="code-scroll" tabIndex={0} aria-label={`${title || language} content. ${wrap ? 'Long lines wrap.' : 'Scroll horizontally for long lines.'}`}><code className="code-source">{lines.map((line, index) => <span className={`code-line${line.tone ? ` code-line-${line.tone}` : ''}`} key={index}><span className="code-line-number" aria-hidden="true">{index + 1}</span><span className="code-text">{line.tokens.length ? line.tokens.map((token, tokenIndex) => <span className={token.kind ? `syntax-${token.kind}` : undefined} key={tokenIndex}>{token.text}</span>) : '\u00a0'}</span></span>)}</code></pre>
  </section>
}
