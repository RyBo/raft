import type { Command, NodeView } from '../types/protocol'

interface Props {
  node: NodeView | null
  send: (cmd: Command) => void
}

export function NodePanel({ node, send }: Props) {
  if (!node) {
    return (
      <div className="panel">
        <h3>Node detail</h3>
        <div className="muted">Click a node in the graph to inspect it.</div>
      </div>
    )
  }

  const n = node
  const log = n.log ?? []
  const kv = n.kv ?? []
  return (
    <div className="panel">
      <h3>
        Node {n.id} <span className={`badge ${n.role}`}>{n.crashed ? 'crashed' : n.role}</span>
        {n.isLearner && <span className="badge learner">learner</span>}
      </h3>

      <div className="grid2">
        <div>term <b>{n.term}</b></div>
        <div>leader <b>{n.lead || '—'}</b></div>
        <div>voted for <b>{n.vote || '—'}</b></div>
        <div>lastIndex <b>{n.lastIndex}</b></div>
        <div>commit <b>{n.commit}</b></div>
        <div>applied <b>{n.applied}</b></div>
      </div>

      <div className="row">
        {n.crashed ? (
          <button className="btn go" onClick={() => send({ type: 'node', action: 'restart', id: n.id })}>
            ↻ Restart
          </button>
        ) : (
          <button className="btn warn" onClick={() => send({ type: 'node', action: 'crash', id: n.id })}>
            ✗ Crash
          </button>
        )}
        {n.isLearner && (
          <button className="btn" onClick={() => send({ type: 'node', action: 'promote', id: n.id })}>
            ⬆ Promote
          </button>
        )}
        <button className="btn warn" onClick={() => send({ type: 'node', action: 'remove', id: n.id })}>
          − Remove
        </button>
      </div>

      <h4>Log ({log.length})</h4>
      <div className="log-list">
        {log.length === 0 && <div className="muted">empty</div>}
        {log.map((e) => (
          <div key={e.index} className={`logentry ${e.committed ? 'committed' : 'uncommitted'} ${e.kind}`}>
            <span className="idx">{e.index}</span>
            <span className="trm">t{e.term}</span>
            <span className="sum">{e.summary}</span>
          </div>
        ))}
      </div>

      {n.progress && (
        <>
          <h4>Replication progress</h4>
          <table className="matrix">
            <thead>
              <tr><th>peer</th><th>match</th><th>next</th><th>state</th></tr>
            </thead>
            <tbody>
              {Object.entries(n.progress).map(([pid, pr]) => (
                <tr key={pid}>
                  <td>N{pid}</td><td>{pr.match}</td><td>{pr.next}</td><td>{pr.state}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      {kv.length > 0 && (
        <>
          <h4>Local KV</h4>
          <div className="kv-pairs">
            {kv.map((p) => (
              <span key={p.key} className="kvpair">{p.key}={p.value}</span>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
