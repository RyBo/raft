import { useState } from 'react'
import { ClusterGraph } from './components/ClusterGraph'
import { ControlPanel } from './components/ControlPanel'
import { EventLog } from './components/EventLog'
import { KVInspector } from './components/KVInspector'
import { NodePanel } from './components/NodePanel'
import { useCluster } from './ws/useCluster'

export default function App() {
  const { connected, state, stateRef, messagesRef, events, kvResults, send } = useCluster()
  const [selected, setSelected] = useState<number | null>(null)

  const selectedNode = state?.nodes.find((n) => n.id === selected) ?? null
  const leader = state?.nodes.find((n) => n.role === 'leader')

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">⛀</span> Raft Visualizer
        </div>
        <div className="topstats">
          {state && (
            <>
              <span>nodes <b>{state.nodes.length}</b></span>
              <span>leader <b>{leader ? `N${leader.id}` : '—'}</b></span>
              <span>term <b>{leader?.term ?? Math.max(0, ...(state.nodes.map((n) => n.term)))}</b></span>
              <span>tick <b>{state.tick}</b></span>
            </>
          )}
          <span className={`conn ${connected ? 'on' : 'off'}`}>{connected ? '● live' : '○ offline'}</span>
        </div>
      </header>

      <div className="layout">
        <main className="stage">
          <ClusterGraph stateRef={stateRef} messagesRef={messagesRef} selected={selected} onSelect={setSelected} />
          <div className="legend">
            <span><i className="dot leader" /> leader</span>
            <span><i className="dot candidate" /> candidate</span>
            <span><i className="dot follower" /> follower</span>
            <span><i className="dot crashed" /> crashed</span>
            <span><i className="dot mvote" /> vote</span>
            <span><i className="dot mapp" /> append</span>
            <span><i className="dot mhb" /> heartbeat</span>
          </div>
        </main>

        <aside className="sidebar">
          <ControlPanel state={state} send={send} />
          <KVInspector state={state} results={kvResults} send={send} />
          <NodePanel node={selectedNode} send={send} />
          <EventLog events={events} />
        </aside>
      </div>
    </div>
  )
}
