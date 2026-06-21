import { useMemo, useState } from 'react'
import type { Command, KVResultEvent, StateEvent } from '../types/protocol'

interface Props {
  state: StateEvent | null
  results: KVResultEvent[]
  send: (cmd: Command) => void
}

let reqCounter = 0
const nextReq = () => `q${++reqCounter}`

export function KVInspector({ state, results, send }: Props) {
  const [key, setKey] = useState('x')
  const [value, setValue] = useState('1')
  const [target, setTarget] = useState('leader')
  const [linearizable, setLinearizable] = useState(true)

  const nodes = state?.nodes ?? []

  const submit = (op: string) =>
    send({ type: 'kv', op, key, value, target, linearizable, reqId: nextReq() })

  // Build a key x node matrix to visualize divergence during partitions.
  const { keys, valueOf } = useMemo(() => {
    const keySet = new Set<string>()
    const map = new Map<string, Map<number, string>>()
    for (const n of nodes) {
      for (const p of n.kv ?? []) {
        keySet.add(p.key)
        if (!map.has(p.key)) map.set(p.key, new Map())
        map.get(p.key)!.set(n.id, p.value)
      }
    }
    return {
      keys: [...keySet].sort(),
      valueOf: (k: string, id: number) => map.get(k)?.get(id) ?? '',
    }
  }, [nodes])

  const diverges = (k: string) => {
    const vals = nodes.map((n) => valueOf(k, n.id))
    const nonEmpty = vals.filter((v) => v !== '')
    return new Set(nonEmpty).size > 1 || nonEmpty.length !== vals.length
  }

  return (
    <div className="panel">
      <h3>Key-Value Store</h3>

      <div className="kv-form">
        <input value={key} onChange={(e) => setKey(e.target.value)} placeholder="key" />
        <input value={value} onChange={(e) => setValue(e.target.value)} placeholder="value" />
        <select value={target} onChange={(e) => setTarget(e.target.value)}>
          <option value="leader">→ leader</option>
          {nodes.map((n) => (
            <option key={n.id} value={String(n.id)}>node {n.id}</option>
          ))}
        </select>
        <label className="inline">
          <input type="checkbox" checked={linearizable} onChange={(e) => setLinearizable(e.target.checked)} />
          linearizable
        </label>
      </div>
      <div className="row">
        <button className="btn go" onClick={() => submit('put')}>PUT</button>
        <button className="btn" onClick={() => submit('get')}>GET</button>
        <button className="btn warn" onClick={() => submit('delete')}>DEL</button>
      </div>

      {keys.length > 0 && (
        <div className="matrix-wrap">
          <table className="matrix">
            <thead>
              <tr>
                <th>key</th>
                {nodes.map((n) => (
                  <th key={n.id} className={n.role === 'leader' ? 'leader-col' : ''}>
                    N{n.id}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {keys.map((k) => (
                <tr key={k} className={diverges(k) ? 'diverge' : ''}>
                  <td className="kcell">{k}</td>
                  {nodes.map((n) => (
                    <td key={n.id}>{valueOf(k, n.id) || '·'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          <div className="muted">rows highlighted in red show divergence across nodes</div>
        </div>
      )}

      <div className="results">
        {results.slice(0, 6).map((r, i) => (
          <div key={i} className={`result ${r.ok ? '' : 'bad'}`}>
            <b>{r.op.toUpperCase()}</b> {r.key}
            {r.op === 'get' && r.ok && (
              <> = {r.found ? `"${r.value}"` : '(missing)'} {r.linearizable ? '🔒' : '⚠ stale'}</>
            )}
            <div className="muted">{r.note}</div>
          </div>
        ))}
      </div>
    </div>
  )
}
