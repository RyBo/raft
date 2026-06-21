import { useEffect, useRef } from 'react'
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

function easeInOut(t: number): number {
  return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2
}

export function ClusterGraph({ stateRef, messagesRef, selected, onSelect }: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const posRef = useRef<NodePos[]>([])

  useEffect(() => {
    const canvas = canvasRef.current!
    const ctx = canvas.getContext('2d')!
    let raf = 0
    let w = 0
    let h = 0

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
      if (cw !== w || ch !== h) {
        w = cw
        h = ch
        canvas.width = w * dpr
        canvas.height = h * dpr
        canvas.style.width = w + 'px'
        canvas.style.height = h + 'px'
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      }
      ctx.clearRect(0, 0, w, h)

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

      const positions: NodePos[] = nodes.map((n, i) => {
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
        ctx.lineWidth = p.id === selected ? 3 : 2
        ctx.strokeStyle = p.id === selected ? '#f8fafc' : 'rgba(15,23,42,0.8)'
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
  }, [stateRef, messagesRef, selected])

  useEffect(() => {
    const canvas = canvasRef.current!
    const onClick = (e: MouseEvent) => {
      const rect = canvas.getBoundingClientRect()
      const mx = e.clientX - rect.left
      const my = e.clientY - rect.top
      for (const p of posRef.current) {
        if (Math.hypot(mx - p.x, my - p.y) <= p.r) {
          onSelect(p.id)
          return
        }
      }
    }
    canvas.addEventListener('click', onClick)
    return () => canvas.removeEventListener('click', onClick)
  }, [onSelect])

  return <canvas ref={canvasRef} className="cluster-canvas" />
}
