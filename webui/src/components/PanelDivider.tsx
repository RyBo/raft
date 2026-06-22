import { useRef } from 'react'

const MIN_W = 240
const maxW = () => Math.round(window.innerWidth * 0.4)

interface Props {
  side: 'left' | 'right'
  open: boolean
  /** Current width of the adjacent sidebar in px (read at drag start). */
  width: number
  onToggle: () => void
  /** Called continuously during a resize drag with the new clamped width. */
  onResize: (width: number) => void
  /** Called when a resize drag begins. */
  onResizeStart?: () => void
  /** Called once when a resize drag ends, for persistence. */
  onCommit?: (width: number) => void
}

/**
 * The control that lives on the boundary between a sidebar and the stage:
 * a draggable grip to resize the panel (when open) plus an always-visible
 * tab to collapse/expand it. Positioned against the stage edge, so it tracks
 * the boundary when open and the screen edge when the panel is collapsed.
 */
export function PanelDivider({ side, open, width, onToggle, onResize, onResizeStart, onCommit }: Props) {
  const startRef = useRef({ x: 0, w: 0, last: 0 })

  const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.currentTarget.setPointerCapture(e.pointerId)
    startRef.current = { x: e.clientX, w: width, last: width }
    onResizeStart?.()
  }
  const onPointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    if (!e.currentTarget.hasPointerCapture(e.pointerId)) return
    const dx = e.clientX - startRef.current.x
    const raw = side === 'left' ? startRef.current.w + dx : startRef.current.w - dx
    const w = Math.max(MIN_W, Math.min(maxW(), raw))
    startRef.current.last = w
    onResize(w)
  }
  const end = (e: React.PointerEvent<HTMLDivElement>) => {
    if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId)
    onCommit?.(startRef.current.last)
  }

  // Chevron points toward the panel when open (= collapse it), away when closed (= expand it).
  const pointsLeft = side === 'left' ? open : !open

  return (
    <div className={`panel-divider ${side}${open ? ' open' : ''}`}>
      {open && (
        <div
          className="pd-grip"
          onPointerDown={onPointerDown}
          onPointerMove={onPointerMove}
          onPointerUp={end}
          onPointerCancel={end}
          title="Drag to resize"
        />
      )}
      <button
        className="pd-toggle"
        onClick={onToggle}
        title={open ? 'Collapse panel' : 'Expand panel'}
        aria-expanded={open}
      >
        <Chevron left={pointsLeft} />
      </button>
    </div>
  )
}

function Chevron({ left }: { left: boolean }) {
  return (
    <svg
      viewBox="0 0 16 16"
      width="13"
      height="13"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      style={left ? { transform: 'scaleX(-1)' } : undefined}
    >
      <path d="M6 3l5 5-5 5" />
    </svg>
  )
}
