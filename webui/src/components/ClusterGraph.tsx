import { useEffect, useRef, useState } from 'react'
import type { StateEvent } from '../types/protocol'
import type { InFlight } from '../ws/useCluster'

interface Props {
  stateRef: React.MutableRefObject<StateEvent | null>
  messagesRef: React.MutableRefObject<InFlight[]>
  selected: number | null
  onSelect: (id: number) => void
}

interface NodePos {
  id: number
  x: number
  y: number
  r: number
}

interface View {
  scale: number
  ox: number
  oy: number
}

const ROLE_COLOR: Record<string, string> = {
  leader: '#34d399',
  candidate: '#fbbf24',
  precandidate: '#f59e0b',
  follower: '#60a5fa',
}

function msgColor(t: string): string {
  if (t.includes('PreVote')) return '#c084fc'
  if (t.includes('Vote')) return '#a78bfa'
  if (t.includes('Heartbeat')) return '#94a3b8'
  if (t.includes('Snap')) return '#fb923c'
  if (t.includes('App')) return '#38bdf8'
  if (t.includes('ReadIndex')) return '#2dd4bf'
  return '#64748b'
}

const GROUP_TINT = ['#1e293b', '#3b1d2b', '#1d3b2b', '#33301d', '#2b1d3b']

const MIN_SCALE = 0.3
const MAX_SCALE = 4
const DRAG_THRESHOLD = 4 // CSS px before a press becomes a drag (vs a click)

function easeInOut(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2
}

function clampScale(s: number): number {
  return Math.min(MAX_SCALE, Math.max(MIN_SCALE, s))
}

// screen (CSS px) -> world px, inverse of `screen = world * scale + o`. DPR never enters here.
function screenToWorld(sx: number, sy: number, v: View): { x: number; y: number } {
  return { x: (sx - v.ox) / v.scale, y: (sy - v.oy) / v.scale }
}

// world px <-> normalized (center-relative, scaled by the short side) so dragged nodes
// track the responsive circular layout instead of drifting on resize.
function norm(x: number, y: number, w: number, h: number): { nx: number; ny: number } {
  const s = Math.min(w, h) || 1
  return { nx: (x - w / 2) / s, ny: (y - h / 2) / s }
}
function denorm(o: { nx: number; ny: number }, w: number, h: number): { x: number; y: number } {
  const s = Math.min(w, h) || 1
  return { x: w / 2 + o.nx * s, y: h / 2 + o.ny * s }
}

export function ClusterGraph({ stateRef, messagesRef, selected, onSelect }: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const posRef = useRef<NodePos[]>([])

  // Interaction state lives in refs so the rAF loop and pointer handlers read live values
  // without ever restarting their effects. zoomPct is the only state (drives the HUD).
  const viewRef = useRef<View>({ scale: 1, ox: 0, oy: 0 })
  const manualPosRef = useRef<Map<number, { nx: number; ny: number }>>(new Map())
  const pointersRef = useRef<Map<number, { x: number; y: number }>>(new Map())
  const wRef = useRef(0)
  const hRef = useRef(0)
  const selectedRef = useRef(selected)
  const onSelectRef = useRef(onSelect)
  selectedRef.current = selected
  onSelectRef.current = onSelect

  const dragRef = useRef<{
    mode: 'idle' | 'pending' | 'pan' | 'node'
    pointerId: number
    nodeId: number | null
    startScreen: { x: number; y: number }
    startView: { ox: number; oy: number }
    grabOffset: { x: number; y: number }
    moved: boolean
  }>({
    mode: 'idle',
    pointerId: -1,
    nodeId: null,
    startScreen: { x: 0, y: 0 },
    startView: { ox: 0, oy: 0 },
    grabOffset: { x: 0, y: 0 },
    moved: false,
  })
  const pinchRef = useRef<{
    startDist: number
    startScale: number
    startMid: { x: number; y: number }
    startView: { ox: number; oy: number }
  } | null>(null)

  const [zoomPct, setZoomPct] = useState(100)

  // --- draw loop ---
  useEffect(() => {
    const canvas = canvasRef.current!
    const ctx = canvas.getContext('2d')!
    let raf = 0
    let w = 0
    let h = 0
    let dprUsed = 0

    const groupOf = (net: StateEvent['net']): Map<number, number> => {
      const m = new Map<number, number>()
      ;(net.partitions ?? []).forEach((g, gi) => (g ?? []).forEach((id) => m.set(id, gi)))
      return m
    }

    const draw = () => {
      const dpr = window.devicePixelRatio || 1
      const parent = canvas.parentElement!
      const cw = parent.clientWidth
      const ch = parent.clientHeight
      if (cw !== w || ch !== h || dpr !== dprUsed) {
        w = cw
        h = ch
        dprUsed = dpr
        canvas.width = w * dpr
        canvas.height = h * dpr
        canvas.style.width = w + 'px'
        canvas.style.height = h + 'px'
      }
      wRef.current = w
      hRef.current = h

      // Clear in DPR-only space (covers the whole viewport regardless of pan/zoom),
      // then switch to the combined view+DPR transform for all world drawing.
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      ctx.clearRect(0, 0, w, h)
      const view = viewRef.current
      ctx.setTransform(dpr * view.scale, 0, 0, dpr * view.scale, dpr * view.ox, dpr * view.oy)

      const st = stateRef.current
      if (!st) {
        raf = requestAnimationFrame(draw)
        return
      }

      const nodes = st.nodes
      const cx = w / 2
      const cy = h / 2
      const radius = Math.min(w, h) * 0.36
      const nodeR = nodes.length > 6 ? 26 : 32

      // prune overrides for nodes that no longer exist (reset / remove)
      const liveIds = new Set(nodes.map((n) => n.id))
      for (const id of manualPosRef.current.keys()) {
        if (!liveIds.has(id)) manualPosRef.current.delete(id)
      }

      const positions: NodePos[] = nodes.map((n, i) => {
        const override = manualPosRef.current.get(n.id)
        if (override) {
          const p = denorm(override, w, h)
          return { id: n.id, x: p.x, y: p.y, r: nodeR }
        }
        const angle = -Math.PI / 2 + (i * 2 * Math.PI) / nodes.length
        return {
          id: n.id,
          x: cx + radius * Math.cos(angle),
          y: cy + radius * Math.sin(angle),
          r: nodeR,
        }
      })
      posRef.current = positions
      const posById = new Map(positions.map((p) => [p.id, p]))
      const grp = groupOf(st.net)
      const selectedId = selectedRef.current

      // --- edges ---
      for (let i = 0; i < positions.length; i++) {
        for (let j = i + 1; j < positions.length; j++) {
          const a = positions[i]
          const b = positions[j]
          const ga = grp.get(a.id)
          const gb = grp.get(b.id)
          const partitioned = ga !== undefined && gb !== undefined && ga !== gb
          ctx.beginPath()
          ctx.moveTo(a.x, a.y)
          ctx.lineTo(b.x, b.y)
          if (partitioned) {
            ctx.strokeStyle = 'rgba(239,68,68,0.5)'
            ctx.setLineDash([6, 6])
            ctx.lineWidth = 1.5
          } else {
            ctx.strokeStyle = 'rgba(148,163,184,0.12)'
            ctx.setLineDash([])
            ctx.lineWidth = 1
          }
          ctx.stroke()
        }
      }
      ctx.setLineDash([])

      // --- in-flight messages ---
      const now = performance.now()
      const msgs = messagesRef.current
      const keep: InFlight[] = []
      for (const m of msgs) {
        const t = (now - m.start) / m.duration
        if (t >= 1) continue
        const from = posById.get(m.from)
        const to = posById.get(m.to)
        if (!from || !to) continue
        keep.push(m)
        const dropped = m.fate === 'dropped'
        const tt = easeInOut(dropped ? Math.min(t, 0.55) : t)
        const x = from.x + (to.x - from.x) * tt
        const y = from.y + (to.y - from.y) * tt
        let alpha = 1
        if (dropped && t > 0.45) alpha = Math.max(0, 1 - (t - 0.45) / 0.3)
        ctx.globalAlpha = alpha
        ctx.beginPath()
        ctx.arc(x, y, m.fate === 'duplicated' ? 4 : 5, 0, Math.PI * 2)
        ctx.fillStyle = dropped ? '#ef4444' : msgColor(m.msgType)
        ctx.fill()
        if (dropped && t > 0.45) {
          ctx.strokeStyle = '#ef4444'
          ctx.lineWidth = 1.5
          ctx.beginPath()
          ctx.moveTo(x - 4, y - 4)
          ctx.lineTo(x + 4, y + 4)
          ctx.moveTo(x + 4, y - 4)
          ctx.lineTo(x - 4, y + 4)
          ctx.stroke()
        }
        ctx.globalAlpha = 1
      }
      messagesRef.current = keep

      // --- nodes ---
      for (const p of positions) {
        const n = nodes.find((x) => x.id === p.id)!
        const g = grp.get(p.id)
        if (g !== undefined) {
          ctx.beginPath()
          ctx.arc(p.x, p.y, p.r + 10, 0, Math.PI * 2)
          ctx.fillStyle = GROUP_TINT[g % GROUP_TINT.length]
          ctx.fill()
        }

        // leader glow
        if (n.role === 'leader' && !n.crashed) {
          ctx.beginPath()
          ctx.arc(p.x, p.y, p.r + 6, 0, Math.PI * 2)
          ctx.strokeStyle = 'rgba(52,211,153,0.5)'
          ctx.lineWidth = 3
          ctx.stroke()
        }

        ctx.beginPath()
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2)
        ctx.fillStyle = n.crashed ? '#374151' : ROLE_COLOR[n.role] || '#60a5fa'
        ctx.globalAlpha = n.crashed ? 0.55 : 1
        ctx.fill()
        ctx.globalAlpha = 1

        // outline (selected / learner)
        ctx.lineWidth = p.id === selectedId ? 3 : 2
        ctx.strokeStyle = p.id === selectedId ? '#f8fafc' : 'rgba(15,23,42,0.8)'
        if (n.isLearner) ctx.setLineDash([4, 3])
        ctx.beginPath()
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2)
        ctx.stroke()
        ctx.setLineDash([])

        // labels
        ctx.fillStyle = '#0f172a'
        ctx.font = `bold ${p.r > 28 ? 16 : 14}px ui-sans-serif, system-ui`
        ctx.textAlign = 'center'
        ctx.textBaseline = 'middle'
        ctx.fillText(`N${p.id}`, p.x, p.y - 6)
        ctx.font = '11px ui-sans-serif, system-ui'
        ctx.fillText(n.crashed ? 'down' : n.role, p.x, p.y + 9)

        // term + commit below node
        ctx.fillStyle = '#cbd5e1'
        ctx.font = '11px ui-sans-serif, system-ui'
        ctx.fillText(`t${n.term} · c${n.commit}`, p.x, p.y + p.r + 14)

        // crash X
        if (n.crashed) {
          ctx.strokeStyle = '#9ca3af'
          ctx.lineWidth = 2
          ctx.beginPath()
          ctx.moveTo(p.x - 10, p.y - 10)
          ctx.lineTo(p.x + 10, p.y + 10)
          ctx.moveTo(p.x + 10, p.y - 10)
          ctx.lineTo(p.x - 10, p.y + 10)
          ctx.stroke()
        }
      }

      raf = requestAnimationFrame(draw)
    }

    raf = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(raf)
  }, [stateRef, messagesRef])

  // --- unified pointer interaction (tap-select, node drag, pan, wheel/pinch zoom) ---
  useEffect(() => {
    const canvas = canvasRef.current!

    const screenOf = (e: { clientX: number; clientY: number }) => {
      const rect = canvas.getBoundingClientRect()
      return { x: e.clientX - rect.left, y: e.clientY - rect.top }
    }
    const hitTest = (world: { x: number; y: number }): NodePos | null => {
      for (const p of posRef.current) {
        if (Math.hypot(world.x - p.x, world.y - p.y) <= p.r) return p
      }
      return null
    }

    const onPointerDown = (e: PointerEvent) => {
      canvas.setPointerCapture(e.pointerId)
      const s = screenOf(e)
      pointersRef.current.set(e.pointerId, s)

      if (pointersRef.current.size === 2) {
        const [a, b] = [...pointersRef.current.values()]
        pinchRef.current = {
          startDist: Math.hypot(a.x - b.x, a.y - b.y),
          startScale: viewRef.current.scale,
          startMid: { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 },
          startView: { ox: viewRef.current.ox, oy: viewRef.current.oy },
        }
        dragRef.current.mode = 'idle'
        return
      }

      const world = screenToWorld(s.x, s.y, viewRef.current)
      const hit = hitTest(world)
      dragRef.current = {
        mode: 'pending',
        pointerId: e.pointerId,
        nodeId: hit ? hit.id : null,
        startScreen: s,
        startView: { ox: viewRef.current.ox, oy: viewRef.current.oy },
        grabOffset: hit ? { x: world.x - hit.x, y: world.y - hit.y } : { x: 0, y: 0 },
        moved: false,
      }
      canvas.style.cursor = 'grabbing'
    }

    const onPointerMove = (e: PointerEvent) => {
      const s = screenOf(e)
      if (pointersRef.current.has(e.pointerId)) pointersRef.current.set(e.pointerId, s)
      const view = viewRef.current

      // pinch takes priority over single-pointer gestures
      if (pinchRef.current && pointersRef.current.size >= 2) {
        const [a, b] = [...pointersRef.current.values()]
        const dist = Math.hypot(a.x - b.x, a.y - b.y)
        const mid = { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }
        const p = pinchRef.current
        const newScale = clampScale(p.startScale * (dist / (p.startDist || 1)))
        const worldAtMid = screenToWorld(p.startMid.x, p.startMid.y, {
          scale: p.startScale,
          ox: p.startView.ox,
          oy: p.startView.oy,
        })
        view.scale = newScale
        view.ox = mid.x - worldAtMid.x * newScale
        view.oy = mid.y - worldAtMid.y * newScale
        setZoomPct(Math.round(newScale * 100))
        return
      }

      const d = dragRef.current
      if (d.mode === 'idle' || e.pointerId !== d.pointerId) return
      const dx = s.x - d.startScreen.x
      const dy = s.y - d.startScreen.y

      if (d.mode === 'pending') {
        if (Math.hypot(dx, dy) < DRAG_THRESHOLD) return
        d.moved = true
        d.mode = d.nodeId !== null ? 'node' : 'pan'
      }

      if (d.mode === 'pan') {
        view.ox = d.startView.ox + dx
        view.oy = d.startView.oy + dy
      } else if (d.mode === 'node' && d.nodeId !== null) {
        const world = screenToWorld(s.x, s.y, view)
        const nx = world.x - d.grabOffset.x
        const ny = world.y - d.grabOffset.y
        manualPosRef.current.set(d.nodeId, norm(nx, ny, wRef.current, hRef.current))
      }
    }

    const endPointer = (e: PointerEvent) => {
      pointersRef.current.delete(e.pointerId)
      if (canvas.hasPointerCapture(e.pointerId)) canvas.releasePointerCapture(e.pointerId)

      if (pinchRef.current && pointersRef.current.size < 2) {
        pinchRef.current = null
        // re-arm a single-finger pan from the remaining pointer so the gesture doesn't dead-end
        if (pointersRef.current.size === 1) {
          const [id] = [...pointersRef.current.keys()]
          const s = pointersRef.current.get(id)!
          dragRef.current = {
            mode: 'pan',
            pointerId: id,
            nodeId: null,
            startScreen: s,
            startView: { ox: viewRef.current.ox, oy: viewRef.current.oy },
            grabOffset: { x: 0, y: 0 },
            moved: true,
          }
        }
        return
      }

      const d = dragRef.current
      if (e.pointerId === d.pointerId) {
        if (!d.moved && d.nodeId !== null) onSelectRef.current(d.nodeId)
        d.mode = 'idle'
        d.pointerId = -1
      }
      canvas.style.cursor = 'grab'
    }

    const onWheel = (e: WheelEvent) => {
      e.preventDefault()
      const s = screenOf(e)
      const view = viewRef.current
      const worldUnder = screenToWorld(s.x, s.y, view)
      const newScale = clampScale(view.scale * Math.exp(-e.deltaY * 0.0015))
      view.ox = s.x - worldUnder.x * newScale
      view.oy = s.y - worldUnder.y * newScale
      view.scale = newScale
      setZoomPct(Math.round(newScale * 100))
    }

    canvas.addEventListener('pointerdown', onPointerDown)
    canvas.addEventListener('pointermove', onPointerMove)
    canvas.addEventListener('pointerup', endPointer)
    canvas.addEventListener('pointercancel', endPointer)
    canvas.addEventListener('lostpointercapture', endPointer)
    canvas.addEventListener('wheel', onWheel, { passive: false })
    return () => {
      canvas.removeEventListener('pointerdown', onPointerDown)
      canvas.removeEventListener('pointermove', onPointerMove)
      canvas.removeEventListener('pointerup', endPointer)
      canvas.removeEventListener('pointercancel', endPointer)
      canvas.removeEventListener('lostpointercapture', endPointer)
      canvas.removeEventListener('wheel', onWheel)
    }
  }, [])

  // --- HUD control handlers ---
  const zoomAtCenter = (factor: number) => {
    const view = viewRef.current
    const cx = wRef.current / 2
    const cy = hRef.current / 2
    const worldCenter = screenToWorld(cx, cy, view)
    const newScale = clampScale(view.scale * factor)
    view.ox = cx - worldCenter.x * newScale
    view.oy = cy - worldCenter.y * newScale
    view.scale = newScale
    setZoomPct(Math.round(newScale * 100))
  }
  const resetView = () => {
    viewRef.current = { scale: 1, ox: 0, oy: 0 }
    setZoomPct(100)
  }
  const resetLayout = () => {
    manualPosRef.current.clear()
  }

  return (
    <>
      <canvas ref={canvasRef} className="cluster-canvas" />
      <div className="graph-controls">
        <button className="gc-btn" onClick={() => zoomAtCenter(1.2)} disabled={zoomPct >= MAX_SCALE * 100} title="Zoom in">
          +
        </button>
        <span className="gc-readout">{zoomPct}%</span>
        <button
          className="gc-btn"
          onClick={() => zoomAtCenter(1 / 1.2)}
          disabled={zoomPct <= MIN_SCALE * 100}
          title="Zoom out"
        >
          −
        </button>
        <button className="gc-btn" onClick={resetView} title="Reset view (zoom & pan)">
          ⤢
        </button>
        <button className="gc-btn" onClick={resetLayout} title="Reset node layout">
          ⟳
        </button>
      </div>
    </>
  )
}
