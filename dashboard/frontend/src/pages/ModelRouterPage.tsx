import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ConfigPageManagerLayout from './ConfigPageManagerLayout'
import styles from './ModelRouterPage.module.css'

interface ModelRouterUpstream {
  id: string
  name: string
  baseUrl: string
  apiKeySet: boolean
  authType: string
  models: string[]
  enabled: boolean
  priority: number
  timeoutSeconds: number
}

interface ModelRouterConfig {
  proxyApiKeys: string[]
  compactFallbackModel: string
  proxiedPathPrefixes: string[]
  failoverStatusCodes: number[]
  upstreams: ModelRouterUpstream[]
}

interface ModelRouterFailure {
  at: string
  detail: string
}

interface ModelRouterState {
  activeByModel: Record<string, string>
  lastFailures: Record<string, ModelRouterFailure>
}

interface ModelRouterStatus {
  state: ModelRouterState
  log: string[]
  configPath: string
  statePath: string
  logFile: string
}

async function fetchJSON<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  return response.json() as Promise<T>
}

const formatList = (values: string[] | number[]) => (values.length ? values.join(', ') : 'None')

const ModelRouterPage: React.FC = () => {
  const [config, setConfig] = useState<ModelRouterConfig | null>(null)
  const [status, setStatus] = useState<ModelRouterStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const logRef = useRef<HTMLDivElement>(null)

  const load = useCallback(async () => {
    try {
      const [nextConfig, nextStatus] = await Promise.all([
        fetchJSON<ModelRouterConfig>('/api/model-router/config'),
        fetchJSON<ModelRouterStatus>('/api/model-router/status'),
      ])
      setConfig(nextConfig)
      setStatus(nextStatus)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
    if (!autoRefresh) {
      return
    }
    const interval = window.setInterval(() => {
      void load()
    }, 5000)
    return () => window.clearInterval(interval)
  }, [autoRefresh, load])

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight
    }
  }, [status?.log])

  const activeEntries = useMemo(() => Object.entries(status?.state.activeByModel ?? {}), [status])
  const failureEntries = useMemo(() => Object.entries(status?.state.lastFailures ?? {}), [status])
  const enabledCount = config?.upstreams.filter((upstream) => upstream.enabled).length ?? 0
  const modelCount = new Set(
    (config?.upstreams ?? [])
      .flatMap((upstream) => upstream.models)
      .filter((model) => model !== '*'),
  ).size

  return (
    <ConfigPageManagerLayout
      eyebrow="Operations"
      title="Model Router"
      description="Manage the lightweight OpenAI-compatible proxy, failover targets, compaction fallback routing, and request logs."
      configArea="Proxy"
      scope="Live model traffic"
      panelEyebrow="Proxy surface"
      panelTitle="Multi-endpoint model routing"
      panelDescription="Route Codex, Paseo, Claude-style, Hermes-style, and OpenAI-compatible clients through one authenticated proxy."
      pills={[
        { label: '/v1', active: true },
        { label: 'Failover', active: true },
        { label: 'Compaction', active: true },
      ]}
    >
      <div className={styles.toolbar}>
        <div>
          <span className={styles.kicker}>Runtime</span>
          <strong>{loading ? 'Loading' : error ? 'Attention needed' : 'Ready'}</strong>
        </div>
        <label className={styles.toggle}>
          <input
            type="checkbox"
            checked={autoRefresh}
            onChange={(event) => setAutoRefresh(event.target.checked)}
          />
          <span>Auto-refresh</span>
        </label>
        <button type="button" className={styles.button} onClick={() => void load()}>
          Refresh
        </button>
      </div>

      {error && <div className={styles.error}>Model router API error: {error}</div>}

      <section className={styles.summaryGrid}>
        <article className={styles.summaryCard}>
          <span className={styles.label}>Enabled upstreams</span>
          <strong>{enabledCount}</strong>
          <span>{config?.upstreams.length ?? 0} configured</span>
        </article>
        <article className={styles.summaryCard}>
          <span className={styles.label}>Known models</span>
          <strong>{modelCount}</strong>
          <span>Wildcard routes are allowed</span>
        </article>
        <article className={styles.summaryCard}>
          <span className={styles.label}>Compaction model</span>
          <strong>{config?.compactFallbackModel || 'gpt-5.4-mini'}</strong>
          <span>Used for compact-like responses requests</span>
        </article>
        <article className={styles.summaryCard}>
          <span className={styles.label}>Proxy keys</span>
          <strong>{config?.proxyApiKeys.length ?? 0}</strong>
          <span>Client API keys accepted by this proxy</span>
        </article>
      </section>

      <section className={styles.panel}>
        <div className={styles.panelHeader}>
          <div>
            <span className={styles.kicker}>Configuration</span>
            <h2>Proxy routes</h2>
          </div>
        </div>
        <div className={styles.metaGrid}>
          <div>
            <span className={styles.label}>Prefixes</span>
            <strong>{formatList(config?.proxiedPathPrefixes ?? [])}</strong>
          </div>
          <div>
            <span className={styles.label}>Failover status</span>
            <strong>{formatList(config?.failoverStatusCodes ?? [])}</strong>
          </div>
          <div>
            <span className={styles.label}>Config file</span>
            <strong className={styles.path}>{status?.configPath ?? '-'}</strong>
          </div>
          <div>
            <span className={styles.label}>Log file</span>
            <strong className={styles.path}>{status?.logFile ?? '-'}</strong>
          </div>
        </div>
      </section>

      <section className={styles.panel}>
        <div className={styles.panelHeader}>
          <div>
            <span className={styles.kicker}>Upstreams</span>
            <h2>Target inventory</h2>
          </div>
        </div>
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Name</th>
                <th>Base URL</th>
                <th>Models</th>
                <th>Priority</th>
                <th>Auth</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {(config?.upstreams ?? []).map((upstream) => (
                <tr key={upstream.id}>
                  <td>
                    <strong>{upstream.name}</strong>
                    <span>{upstream.id}</span>
                  </td>
                  <td className={styles.path}>{upstream.baseUrl}</td>
                  <td>{formatList(upstream.models)}</td>
                  <td>{upstream.priority}</td>
                  <td>{upstream.authType}{upstream.apiKeySet ? ' / key set' : ''}</td>
                  <td>
                    <span className={upstream.enabled ? styles.goodBadge : styles.neutralBadge}>
                      {upstream.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                  </td>
                </tr>
              ))}
              {!config?.upstreams.length && (
                <tr>
                  <td colSpan={6} className={styles.empty}>No upstreams configured.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className={styles.twoColumn}>
        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <span className={styles.kicker}>State</span>
              <h2>Active routes</h2>
            </div>
          </div>
          <div className={styles.stack}>
            {activeEntries.map(([model, upstreamId]) => (
              <div className={styles.row} key={model}>
                <span>{model}</span>
                <strong>{upstreamId}</strong>
              </div>
            ))}
            {!activeEntries.length && <div className={styles.empty}>No active route has been promoted yet.</div>}
          </div>
        </div>

        <div className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <span className={styles.kicker}>Health</span>
              <h2>Recent failures</h2>
            </div>
          </div>
          <div className={styles.stack}>
            {failureEntries.slice(-8).map(([key, failure]) => (
              <div className={styles.failure} key={key}>
                <span>{key}</span>
                <strong>{failure.detail}</strong>
                <small>{failure.at}</small>
              </div>
            ))}
            {!failureEntries.length && <div className={styles.empty}>No recent failover records.</div>}
          </div>
        </div>
      </section>

      <section className={styles.panel}>
        <div className={styles.panelHeader}>
          <div>
            <span className={styles.kicker}>Logs</span>
            <h2>Model router tail</h2>
          </div>
        </div>
        <div className={styles.logBox} ref={logRef}>
          {(status?.log ?? []).map((line, index) => (
            <div key={`${line}-${index}`}>{line}</div>
          ))}
          {!status?.log.length && <div>No model router logs yet.</div>}
        </div>
      </section>
    </ConfigPageManagerLayout>
  )
}

export default ModelRouterPage
