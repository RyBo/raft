import { useCallback, useEffect, useRef, useState } from 'react'
import type {
  Command,
  KVResultEvent,
  LogEvent,
  ServerEvent,
  StateEvent,
} from '../types/protocol'

// InFlight is a message being animated across the cluster graph.
export interface InFlight {
  id: string
  from: number
  to: number
  msgType: string
  fate: string
  start: number // performance.now() when it appeared
  duration: number // ms to traverse
}

const MAX_INFLIGHT = 400

export interface ClusterAPI {
  connected: boolean
  state: StateEvent | null
  stateRef: React.MutableRefObject<StateEvent | null>
  messagesRef: React.MutableRefObject<InFlight[]>
  events: LogEvent[]
  kvResults: KVResultEvent[]
  send: (cmd: Command) => void
}

export function useCluster(): ClusterAPI {
  const [connected, setConnected] = useState(false)
  const [state, setState] = useState<StateEvent | null>(null)
  const [events, setEvents] = useState<LogEvent[]>([])
  const [kvResults, setKvResults] = useState<KVResultEvent[]>([])

  const stateRef = useRef<StateEvent | null>(null)
  const messagesRef = useRef<InFlight[]>([])
  const wsRef = useRef<WebSocket | null>(null)

  const handle = useCallback((ev: ServerEvent) => {
    switch (ev.type) {
      case 'state':
        stateRef.current = ev
        setState(ev)
        break
      case 'message': {
        const ms = stateRef.current?.clock.msPerTick ?? 200
        const ticks = Math.max(1, ev.deliverTick - ev.sentTick)
        const duration = Math.min(3000, Math.max(160, ticks * ms))
        const arr = messagesRef.current
        arr.push({
          id: ev.id,
          from: ev.from,
          to: ev.to,
          msgType: ev.msgType,
          fate: ev.fate,
          start: performance.now(),
          duration,
        })
        if (arr.length > MAX_INFLIGHT) arr.splice(0, arr.length - MAX_INFLIGHT)
        break
      }
      case 'event':
        setEvents((prev) => [ev, ...prev].slice(0, 250))
        break
      case 'kvResult':
        setKvResults((prev) => [ev, ...prev].slice(0, 40))
        break
    }
  }, [])

  useEffect(() => {
    let stopped = false
    let retry: number | undefined

    const connect = () => {
      if (stopped) return
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      const ws = new WebSocket(`${proto}://${location.host}/ws`)
      wsRef.current = ws
      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        if (!stopped) retry = window.setTimeout(connect, 1000)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (e) => {
        try {
          handle(JSON.parse(e.data) as ServerEvent)
        } catch {
          /* ignore malformed */
        }
      }
    }
    connect()
    return () => {
      stopped = true
      if (retry) clearTimeout(retry)
      wsRef.current?.close()
    }
  }, [handle])

  const send = useCallback((cmd: Command) => {
    const ws = wsRef.current
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(cmd))
    }
  }, [])

  return { connected, state, stateRef, messagesRef, events, kvResults, send }
}
