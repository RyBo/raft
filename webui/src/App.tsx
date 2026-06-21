import { useState } from 'react'
import { ClusterGraph } from './components/ClusterGraph'
import { ClusterStatus } from './components/ClusterStatus'
import { ControlPanel } from './components/ControlPanel'
import { EventLog } from './components/EventLog'
import { InfoModal } from './components/InfoModal'
import { KVInspector } from './components/KVInspector'
import { NodePanel } from './components/NodePanel'
import { useCluster } from './ws/useCluster'

export default function App() {
  const { connected, state, stateRef, messagesRef, events, kvResults, send } = useCluster()
  const [selected, setSelected] = useState<number | null>(null)
  const [showInfo, setShowInfo] = useState(false)

  const selectedNode = state?.nodes.find((n) => n.id === selected) ?? null

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">⛀</span> Raft Visualizer
          <button className="info-btn" onClick={() => setShowInfo(true)} title="About Raft">
            ⓘ
          </button>
        </div>
        <div className="topstats">
          <ClusterStatus state={state} />
          <span className="fact">tick <b>{state?.tick ?? 0}</b></span>
          <span className={`conn ${connected ? 'on' : 'off'}`}>{connected ? '● live' : '○ offline'}</span>
        </div>
      </header>

      <div className="layout">
        <aside className="sidebar left">
          <KVInspector state={state} results={kvResults} send={send} />
          <NodePanel node={selectedNode} send={send} />
        </aside>

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

        <aside className="sidebar right">
          <ControlPanel state={state} send={send} />
          <EventLog events={events} />
        </aside>
      </div>

      {showInfo && <InfoModal onClose={() => setShowInfo(false)} />}
    </div>
  )
}
