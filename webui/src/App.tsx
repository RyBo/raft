import { useEffect, useRef, useState } from 'react'
import { ClusterGraph } from './components/ClusterGraph'
import { ClusterStatus } from './components/ClusterStatus'
import { ControlPanel } from './components/ControlPanel'
import { EventLog } from './components/EventLog'
import { InfoModal } from './components/InfoModal'
import { KVInspector } from './components/KVInspector'
import { NodePanel } from './components/NodePanel'
import { PanelDivider } from './components/PanelDivider'
import { useMediaQuery } from './hooks/useMediaQuery'
import { useCluster } from './ws/useCluster'

const persist = (k: string, v: number) => {
  try {
    localStorage.setItem(k, String(v))
  } catch {
    /* ignore */
  }
}
const readNum = (k: string, fallback: number) => {
  const v = Number(localStorage.getItem(k))
  return Number.isFinite(v) && v > 0 ? v : fallback
}

function PanelIcon({ side }: { side: 'left' | 'right' }) {
  const x = side === 'left' ? 6 : 10
  return (
    <svg viewBox="0 0 16 16" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="1.5">
      <rect x="1.5" y="2.75" width="13" height="10.5" rx="1.5" />
      <line x1={x} y1="2.75" x2={x} y2="13.25" />
    </svg>
  )
}

export default function App() {
  const { connected, state, stateRef, messagesRef, events, kvResults, send } = useCluster()
  const [selected, setSelected] = useState<number | null>(null)
  const [showInfo, setShowInfo] = useState(false)

  const isMobile = useMediaQuery('(max-width: 820px)')
  const [leftOpen, setLeftOpen] = useState(!isMobile)
  const [rightOpen, setRightOpen] = useState(!isMobile)
  const [leftW, setLeftW] = useState(() => readNum('raft.leftW', 360))
  const [rightW, setRightW] = useState(() => readNum('raft.rightW', 380))

  // Reset panel visibility when crossing the mobile breakpoint: open on desktop, closed on mobile.
  useEffect(() => {
    setLeftOpen(!isMobile)
    setRightOpen(!isMobile)
  }, [isMobile])

  // Measure header height so mobile drawers / backdrop start below it.
  const headerRef = useRef<HTMLElement>(null)
  const [headerH, setHeaderH] = useState(0)
  useEffect(() => {
    const el = headerRef.current
    if (!el) return
    const update = () => setHeaderH(el.offsetHeight)
    update()
    const ro = new ResizeObserver(update)
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const selectedNode = state?.nodes.find((n) => n.id === selected) ?? null

  const layoutStyle = {
    '--left-w': !isMobile && leftOpen ? `${leftW}px` : '0px',
    '--right-w': !isMobile && rightOpen ? `${rightW}px` : '0px',
  } as React.CSSProperties

  return (
    <div className={`app${isMobile ? ' mobile' : ''}`} style={{ '--header-h': `${headerH}px` } as React.CSSProperties}>
      <header className="topbar" ref={headerRef}>
        <div className="tb-row tb-primary">
          <div className="brand">
            <span className="logo">⛀</span> Raft Visualizer
            <button className="info-btn" onClick={() => setShowInfo(true)} title="About Raft">
              ⓘ
            </button>
          </div>
          {isMobile && <ClusterStatus state={state} variant="primary" />}
        </div>
        <div className="tb-row tb-secondary">
          {isMobile && (
            <button
              className="panel-toggle"
              onClick={() => setLeftOpen((o) => !o)}
              aria-pressed={leftOpen}
              title="Toggle data panel"
            >
              <PanelIcon side="left" />
            </button>
          )}
          <div className="tb-stats">
            <ClusterStatus state={state} variant={isMobile ? 'secondary' : 'full'} />
            <span className="fact">tick <b>{state?.tick ?? 0}</b></span>
            <span className={`conn ${connected ? 'on' : 'off'}`}>{connected ? '● live' : '○ offline'}</span>
          </div>
          {isMobile && (
            <button
              className="panel-toggle"
              onClick={() => setRightOpen((o) => !o)}
              aria-pressed={rightOpen}
              title="Toggle controls panel"
            >
              <PanelIcon side="right" />
            </button>
          )}
        </div>
      </header>

      <div className="layout" style={layoutStyle}>
        <aside className={`sidebar left${leftOpen ? ' open' : ''}`}>
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

        <aside className={`sidebar right${rightOpen ? ' open' : ''}`}>
          <ControlPanel state={state} send={send} />
          <EventLog events={events} />
        </aside>

        {!isMobile && (
          <PanelDivider
            side="left"
            open={leftOpen}
            width={leftW}
            onToggle={() => setLeftOpen((o) => !o)}
            onResize={setLeftW}
            onCommit={(w) => persist('raft.leftW', w)}
          />
        )}
        {!isMobile && (
          <PanelDivider
            side="right"
            open={rightOpen}
            width={rightW}
            onToggle={() => setRightOpen((o) => !o)}
            onResize={setRightW}
            onCommit={(w) => persist('raft.rightW', w)}
          />
        )}
      </div>

      {isMobile && (leftOpen || rightOpen) && (
        <div
          className="drawer-backdrop"
          onClick={() => {
            setLeftOpen(false)
            setRightOpen(false)
          }}
        />
      )}

      {showInfo && <InfoModal onClose={() => setShowInfo(false)} />}
    </div>
  )
}
