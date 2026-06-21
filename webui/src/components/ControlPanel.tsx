import { useState } from 'react'
import type { Command, StateEvent } from '../types/protocol'

interface Props {
  state: StateEvent | null
  send: (cmd: Command) => void
}

export function ControlPanel({ state, send }: Props) {
  const [resetN, setResetN] = useState(3)
  const [seed, setSeed] = useState(1)

  const running = state?.clock.running ?? false
  const ms = state?.clock.msPerTick ?? 220
  const ids = state ? state.nodes.map((n) => n.id) : []
  const leader = state?.nodes.find((n) => n.role === 'leader')?.id

  const isolateLeader = () => {
    if (!state || leader === undefined) return
    const rest = ids.filter((id) => id !== leader)
    send({ type: 'partition', groups: [[leader], rest] })
  }
  const splitHalf = () => {
    if (!state) return
    const mid = Math.ceil(ids.length / 2)
    send({ type: 'partition', groups: [ids.slice(0, mid), ids.slice(mid)] })
  }
  const heal = () => send({ type: 'partition', groups: [] })

  return (
    <div className="panel">
      <h3>Controls</h3>

      <div className="row">
        <button className={running ? 'btn warn' : 'btn go'} onClick={() => send({ type: 'clock', action: running ? 'pause' : 'run' })}>
          {running ? '⏸ Pause' : '▶ Run'}
        </button>
        <button className="btn" onClick={() => send({ type: 'clock', action: 'step' })} disabled={running}>
          ⏭ Step
        </button>
      </div>

      <label className="field">
        <span>Speed (ms/tick): {ms}</span>
        <input
          type="range"
          min={20}
          max={800}
          step={10}
          value={ms}
          // Lower ms = faster, so invert the slider visually by direct value.
          onChange={(e) => send({ type: 'clock', action: 'setSpeed', msPerTick: Number(e.target.value) })}
        />
      </label>

      <h4>Network · CAP</h4>
      <div className="row">
        <button className="btn" onClick={isolateLeader}>Isolate leader</button>
        <button className="btn" onClick={splitHalf}>Split in half</button>
        <button className="btn go" onClick={heal}>Heal</button>
      </div>
      <label className="field">
        <span>Base latency: {state?.net.baseLatency ?? 0} ticks</span>
        <input
          type="range"
          min={1}
          max={12}
          value={state?.net.baseLatency ?? 2}
          onChange={(e) => send({ type: 'net', latency: Number(e.target.value), drop: state?.net.dropRate ?? 0 })}
        />
      </label>
      <label className="field">
        <span>Drop rate: {((state?.net.dropRate ?? 0) * 100).toFixed(0)}%</span>
        <input
          type="range"
          min={0}
          max={0.9}
          step={0.05}
          value={state?.net.dropRate ?? 0}
          onChange={(e) => send({ type: 'net', latency: state?.net.baseLatency ?? 2, drop: Number(e.target.value) })}
        />
      </label>

      <h4>Membership</h4>
      <div className="row">
        <button className="btn" onClick={() => send({ type: 'addLearner' })}>+ Add node (learner)</button>
        <button className="btn" onClick={() => send({ type: 'addNode' })}>+ Add voter</button>
      </div>

      <h4>Reset</h4>
      <div className="row">
        <label className="inline">
          nodes
          <input type="number" min={1} max={9} value={resetN} onChange={(e) => setResetN(Number(e.target.value))} />
        </label>
        <label className="inline">
          seed
          <input type="number" value={seed} onChange={(e) => setSeed(Number(e.target.value))} />
        </label>
        <button className="btn warn" onClick={() => send({ type: 'reset', n: resetN, seed })}>Reset</button>
      </div>
      {state && <div className="muted">tick {state.tick} · seed {state.seed}</div>}
    </div>
  )
}
