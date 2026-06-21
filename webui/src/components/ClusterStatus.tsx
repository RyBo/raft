import type { StateEvent } from '../types/protocol'

type Kind = 'healthy' | 'electing' | 'partitioned' | 'noquorum' | 'degraded' | 'unknown'

interface Status {
  kind: Kind
  label: string
  leader: number | null
  term: number
  aliveVoters: number
  needed: number
  voters: number
  learners: number
}

function derive(state: StateEvent | null): Status {
  if (!state) {
    return { kind: 'unknown', label: 'Connecting…', leader: null, term: 0, aliveVoters: 0, needed: 0, voters: 0, learners: 0 }
  }
  const nodes = state.nodes
  const voters = state.config?.voters ?? []
  const learners = state.config?.learners ?? []
  const byId = new Map(nodes.map((n) => [n.id, n]))

  const needed = Math.floor(voters.length / 2) + 1
  const aliveVoters = voters.filter((id) => {
    const n = byId.get(id)
    return n && !n.crashed
  }).length

  const leaders = nodes.filter((n) => n.role === 'leader' && !n.crashed)
  const candidates = nodes.filter((n) => n.role === 'candidate' || n.role === 'precandidate')
  const crashed = nodes.filter((n) => n.crashed).length
  const partitioned = (state.net?.partitions ?? []).length > 0

  const leader = leaders[0]?.id ?? null
  const term = leaders[0]?.term ?? nodes.reduce((m, n) => Math.max(m, n.term), 0)

  let kind: Kind
  let label: string
  if (aliveVoters < needed) {
    kind = 'noquorum'
    label = 'No quorum'
  } else if (partitioned) {
    kind = 'partitioned'
    label = 'Partitioned'
  } else if (leaders.length !== 1 || candidates.length > 0) {
    kind = 'electing'
    label = 'Electing leader'
  } else if (crashed > 0) {
    kind = 'degraded'
    label = 'Degraded'
  } else {
    kind = 'healthy'
    label = 'Healthy'
  }

  return { kind, label, leader, term, aliveVoters, needed, voters: voters.length, learners: learners.length }
}

export function ClusterStatus({ state }: { state: StateEvent | null }) {
  const s = derive(state)
  return (
    <div className="cluster-status">
      <span className={`cluster-pill ${s.kind}`} title="Derived cluster health">
        {s.label}
      </span>
      <span className="fact">leader <b>{s.leader ? `N${s.leader}` : '—'}</b></span>
      <span className="fact">term <b>{s.term}</b></span>
      <span className="fact" title="voters reachable / needed for quorum">
        quorum <b>{s.aliveVoters}/{s.needed}</b>
      </span>
      <span className="fact" title="voters · learners">
        <b>{s.voters}</b>V·<b>{s.learners}</b>L
      </span>
    </div>
  )
}
