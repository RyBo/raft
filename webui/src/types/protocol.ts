// Mirror of the Go wire protocol in sim/protocol.go.

export interface ClockView {
  running: boolean
  msPerTick: number
}

export interface ProgressView {
  match: number
  next: number
  state: string
}

export interface LogEntryView {
  index: number
  term: number
  committed: boolean
  kind: string
  summary: string
}

export interface Pair {
  key: string
  value: string
}

export interface NodeView {
  id: number
  role: string
  term: number
  vote: number
  lead: number
  commit: number
  applied: number
  lastIndex: number
  crashed: boolean
  isLearner: boolean
  log: LogEntryView[]
  kv: Pair[]
  progress?: Record<number, ProgressView>
}

export interface ConfigView {
  voters: number[]
  learners: number[]
}

export interface LinkView {
  from: number
  to: number
  latency: number
  drop: number
}

export interface NetView {
  partitions: number[][]
  baseLatency: number
  dropRate: number
  links: LinkView[]
}

export interface StateEvent {
  type: 'state'
  tick: number
  seed: number
  clock: ClockView
  nodes: NodeView[]
  config: ConfigView
  net: NetView
}

export interface MessageEvent {
  type: 'message'
  id: string
  from: number
  to: number
  msgType: string
  sentTick: number
  deliverTick: number
  fate: string
  entries: number
}

export interface LogEvent {
  type: 'event'
  tick: number
  kind: string
  node: number
  term: number
  text: string
}

export interface KVResultEvent {
  type: 'kvResult'
  reqId: string
  ok: boolean
  op: string
  key: string
  value: string
  found: boolean
  servedBy: number
  linearizable: boolean
  note: string
}

export type ServerEvent = StateEvent | MessageEvent | LogEvent | KVResultEvent

export interface Command {
  type: string
  [k: string]: unknown
}
