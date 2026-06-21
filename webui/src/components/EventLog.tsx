import type { LogEvent } from '../types/protocol'

interface Props {
  events: LogEvent[]
}

const KIND_CLASS: Record<string, string> = {
  leader_elected: 'ev-leader',
  election_started: 'ev-election',
  prevote_started: 'ev-election',
  node_crashed: 'ev-bad',
  node_removed: 'ev-bad',
  node_restarted: 'ev-good',
  node_added: 'ev-good',
  partition: 'ev-warn',
  net: 'ev-warn',
  stale_read: 'ev-warn',
  conf_change_applied: 'ev-info',
  conf_change_proposed: 'ev-info',
  snapshot_created: 'ev-info',
  snapshot_installed: 'ev-good',
}

export function EventLog({ events }: Props) {
  return (
    <div className="panel">
      <h3>Event timeline</h3>
      <div className="event-list">
        {events.length === 0 && <div className="muted">no events yet</div>}
        {events.map((e, i) => (
          <div key={i} className={`event ${KIND_CLASS[e.kind] || 'ev-info'}`}>
            <span className="etick">t{e.tick}</span>
            <span className="etext">{e.text}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
