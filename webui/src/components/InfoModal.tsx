import { useEffect } from 'react'

interface Props {
  onClose: () => void
}

interface Link {
  href: string
  title: string
  note: string
}

const PAPER: Link[] = [
  { href: 'https://raft.github.io/', title: 'raft.github.io', note: 'The official Raft homepage' },
  {
    href: 'https://raft.github.io/raft.pdf',
    title: 'In Search of an Understandable Consensus Algorithm (Extended)',
    note: 'Ongaro & Ousterhout, 2014 — the Raft paper',
  },
  {
    href: 'https://github.com/ongardie/dissertation',
    title: 'Consensus: Bridging Theory and Practice',
    note: "Diego Ongaro's PhD dissertation — membership changes, ReadIndex, more",
  },
  {
    href: 'https://thesecretlivesofdata.com/raft/',
    title: 'The Secret Lives of Data',
    note: 'A friendly animated walkthrough of Raft',
  },
]

const IMPLS: Link[] = [
  { href: 'https://github.com/etcd-io/raft', title: 'etcd-io/raft', note: 'Go — powers etcd & Kubernetes; the Ready/Advance design this core follows' },
  { href: 'https://github.com/hashicorp/raft', title: 'hashicorp/raft', note: 'Go — used by Consul, Nomad, Vault' },
  { href: 'https://github.com/tikv/raft-rs', title: 'tikv/raft-rs', note: 'Rust — used by TiKV / TiDB' },
]

function LinkList({ links }: { links: Link[] }) {
  return (
    <ul className="modal-links">
      {links.map((l) => (
        <li key={l.href}>
          <a href={l.href} target="_blank" rel="noreferrer">
            {l.title} ↗
          </a>
          <span className="muted">{l.note}</span>
        </li>
      ))}
    </ul>
  )
}

export function InfoModal({ onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <header className="modal-head">
          <h2>What is Raft?</h2>
          <button className="modal-close" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </header>
        <div className="modal-body">
          <p>
            <b>Raft</b> is a consensus algorithm: it keeps a <i>replicated log</i> identical across a
            cluster of servers, so they behave like a single reliable machine even when some of them
            fail. It was designed to be <i>understandable</i> — an approachable alternative to Paxos.
          </p>
          <p>It works in three parts, all visible in this demo:</p>
          <ul className="modal-pillars">
            <li>
              <b>Leader election</b> — one server is elected leader for a numbered <i>term</i>;
              followers grant votes and a randomized timeout breaks ties.
            </li>
            <li>
              <b>Log replication</b> — clients send writes to the leader, which appends them and
              replicates to followers; an entry <i>commits</i> once a majority store it.
            </li>
            <li>
              <b>Safety</b> — rules on voting and commitment guarantee that a committed entry is never
              lost or overwritten, even across leader changes and network partitions.
            </li>
          </ul>
          <p className="muted">
            Because consensus needs a majority, a partitioned minority cannot make progress — this is
            the CAP trade-off you can trigger with the network controls (Raft chooses consistency).
          </p>

          <h3>Paper &amp; background</h3>
          <LinkList links={PAPER} />

          <h3>Production implementations</h3>
          <LinkList links={IMPLS} />
        </div>
      </div>
    </div>
  )
}
