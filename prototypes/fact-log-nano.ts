// fact-log-nano.ts
//
// ═══════════════════════════════════════════════════════════════════════════
// What this is
// ═══════════════════════════════════════════════════════════════════════════
//
// A toy demonstration of ONE primitive — a signed, append-only FACT LOG —
// and how every other primitive in an agent harness (skills, agents,
// dispatches, approvals, middleware, RBAC) can be expressed as
// APPEND-A-FACT or PROJECT-THE-LOG over it. No node ever calls a method
// on another node. No shared mutable state. No RPC.
//
// Why this shape. The engine-nano prototype (sibling worktree) used
// one in-process World with local maps, closures, and direct mutations
// of other agents' queues. That falls apart the moment there are two
// machines: closures can't ship, dispatcher-side selection can't find
// remote agents, approvals have no provenance. The fact-log primitive
// solves all three in one move: every state change is a fact anyone
// can append, verify, subscribe to, and project — so collaboration
// reduces to "share the log."
//
// The toy. Four participants ("nodes"), each with its own local policy,
// all pointing at one in-memory FactLog that lives in this process.
// Swap the FactLog for a WebSocket stream, a Convex table, or a
// gossip-replicated Merkle log and this exact same code collaborates
// across machines.
//
//   detector-1        Observes a noisy signup-rate sensor. When a
//                     4-tick moving average drops below threshold,
//                     appends DispatchProposed for rollback-signup.
//
//   alice-laptop      Appends AgentOffered{kind:'human'}. When she
//                     sees a DispatchProposed for a skill she offers,
//                     appends DispatchClaimed. Three ticks later,
//                     appends DispatchConfirmed.
//
//   rollback-vps      Joins in phase 2. Offers itself as a 'script'
//                     agent for the same skill. Claims + confirms
//                     same tick. Alice defers to it by local policy.
//
//   owner-dashboard   Holds an attestation that it has the 'owner'
//                     role. When it sees a DispatchProposed whose
//                     skill requires 'owner' approval, appends
//                     DispatchApproved.
//
// All communication between these four happens through log facts.
// The demo runs them in one process because that's enough to show
// the shape; nothing about the code assumes co-location.
//
// ═══════════════════════════════════════════════════════════════════════════
// Why this is "trivial to expand"
// ═══════════════════════════════════════════════════════════════════════════
//
//  + Add a new skill                  → append SkillRegistered fact
//  + Add a new node on a new machine  → point it at the log; write its policy
//  + Add a new role                   → append RoleAttested fact (signed)
//  + Add a new middleware             → write a pure projection; everyone runs it
//  + Add a new agent kind             → add a string to the AgentKind union
//  + Distribute across machines       → replace FactLog with a replicated store
//  + Audit what happened              → dump the log; every decision is there
//  + Replay to a past state           → project the log up to lamport N
//
// None of these require changing the Node class or the FactLog class.
// The primitive absorbs the extension.

// ═══════════════════════════════════════════════════════════════════════════
// Facts — the universe of things that can happen
// ═══════════════════════════════════════════════════════════════════════════

type AgentKind = 'human' | 'ai' | 'script'

type FactPayload =
  | { kind: 'SkillRegistered'; skillId: string; description: string; requiresApprovalFromRole?: string }
  | { kind: 'RoleAttested'; subject: string; role: string }
  | { kind: 'AgentOffered'; agentId: string; agentKind: AgentKind; skills: string[] }
  | { kind: 'SensorEmitted'; sensorId: string; reading: Record<string, unknown> }
  | { kind: 'DispatchProposed'; dispatchId: string; skillId: string; payload: unknown }
  | { kind: 'DispatchApproved'; dispatchId: string }
  | { kind: 'DispatchClaimed'; dispatchId: string; byAgent: string }
  | { kind: 'DispatchConfirmed'; dispatchId: string }
  | { kind: 'DispatchBlocked'; dispatchId: string; reason: string }

type Fact = {
  id: string          // content-address stub; real system: hash(prev, payload, signer)
  lamport: number     // monotonic across the log; replaces global tick
  signer: string      // id of the node that appended this fact
  payload: FactPayload
}

// ═══════════════════════════════════════════════════════════════════════════
// FactLog — the ONE primitive. Append-only, subscribable, projectable.
// ═══════════════════════════════════════════════════════════════════════════

class FactLog {
  entries: Fact[] = []
  subscribers: ((fact: Fact) => void)[] = []
  private nextLamport = 1

  append(signer: string, payload: FactPayload): Fact {
    const fact: Fact = {
      id: `f${this.entries.length + 1}`,
      lamport: this.nextLamport++,
      signer,
      payload,
    }
    this.entries.push(fact)
    for (const notify of this.subscribers) notify(fact)
    return fact
  }

  subscribe(callback: (fact: Fact) => void): () => void {
    this.subscribers.push(callback)
    return () => { this.subscribers = this.subscribers.filter(cb => cb !== callback) }
  }

  // Pure projection — any reducer, optionally "as of" a lamport cut (time-travel).
  project<T>(seed: T, reducer: (acc: T, fact: Fact) => T, asOfLamport?: number): T {
    const relevant = asOfLamport === undefined
      ? this.entries
      : this.entries.filter(f => f.lamport <= asOfLamport)
    return relevant.reduce(reducer, seed)
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// View — the projection shape the demo uses. Derived from the log.
// ═══════════════════════════════════════════════════════════════════════════

type DispatchStatus =
  | 'awaiting-approval'   // requires a role's approval; none recorded yet
  | 'claimable'           // approved (or no approval needed); any capable agent can claim
  | 'working'             // claimed by some agent; they're on it
  | 'confirmed'           // claimer appended DispatchConfirmed
  | 'blocked'             // explicit DispatchBlocked

type DispatchView = {
  id: string
  skillId: string
  payload: unknown
  status: DispatchStatus
  proposedAt: number
  approvedAt?: number
  claimedBy?: string
  claimedAt?: number
  confirmedAt?: number
  blockedReason?: string
}

type View = {
  skills: Map<string, { description: string; requiresApprovalFromRole?: string }>
  rolesHeldBy: Map<string, Set<string>>         // subject → set of roles attested about them
  agents: Map<string, { kind: AgentKind; skills: string[] }>
  dispatches: Map<string, DispatchView>
  sensorHistory: Map<string, Record<string, unknown>[]>
}

// This reducer IS the entire semantic model of the system. It encodes
// middleware (RBAC approval check), selection policy (first-to-claim
// wins, only capable agents), and lifecycle transitions. Every node
// computes it locally from the same log, so they all agree.
function project(log: FactLog, asOfLamport?: number): View {
  return log.project<View>(
    {
      skills: new Map(),
      rolesHeldBy: new Map(),
      agents: new Map(),
      dispatches: new Map(),
      sensorHistory: new Map(),
    },
    (view, fact) => {
      const payload = fact.payload

      if (payload.kind === 'SkillRegistered') {
        view.skills.set(payload.skillId, {
          description: payload.description,
          requiresApprovalFromRole: payload.requiresApprovalFromRole,
        })
      }

      else if (payload.kind === 'RoleAttested') {
        const existing = view.rolesHeldBy.get(payload.subject) ?? new Set<string>()
        existing.add(payload.role)
        view.rolesHeldBy.set(payload.subject, existing)
      }

      else if (payload.kind === 'AgentOffered') {
        view.agents.set(payload.agentId, { kind: payload.agentKind, skills: payload.skills })
      }

      else if (payload.kind === 'SensorEmitted') {
        const history = view.sensorHistory.get(payload.sensorId) ?? []
        history.push(payload.reading)
        view.sensorHistory.set(payload.sensorId, history)
      }

      else if (payload.kind === 'DispatchProposed') {
        const skill = view.skills.get(payload.skillId)
        const needsApproval = !!skill?.requiresApprovalFromRole
        view.dispatches.set(payload.dispatchId, {
          id: payload.dispatchId,
          skillId: payload.skillId,
          payload: payload.payload,
          status: needsApproval ? 'awaiting-approval' : 'claimable',
          proposedAt: fact.lamport,
        })
      }

      else if (payload.kind === 'DispatchApproved') {
        const dispatch = view.dispatches.get(payload.dispatchId)
        if (!dispatch) return view
        const skill = view.skills.get(dispatch.skillId)
        const requiredRole = skill?.requiresApprovalFromRole
        if (!requiredRole) return view
        // RBAC: approval is ONLY valid if signer holds the required role
        const signerRoles = view.rolesHeldBy.get(fact.signer) ?? new Set()
        if (!signerRoles.has(requiredRole)) return view
        dispatch.approvedAt = fact.lamport
        if (dispatch.status === 'awaiting-approval') dispatch.status = 'claimable'
      }

      else if (payload.kind === 'DispatchClaimed') {
        const dispatch = view.dispatches.get(payload.dispatchId)
        if (!dispatch) return view
        if (dispatch.status !== 'claimable') return view    // first-claim-wins
        const agent = view.agents.get(payload.byAgent)
        if (!agent) return view                              // unknown agent
        if (!agent.skills.includes(dispatch.skillId)) return view  // agent doesn't offer this skill
        if (fact.signer !== payload.byAgent) return view     // claims must be self-signed
        dispatch.claimedBy = payload.byAgent
        dispatch.claimedAt = fact.lamport
        dispatch.status = 'working'
      }

      else if (payload.kind === 'DispatchConfirmed') {
        const dispatch = view.dispatches.get(payload.dispatchId)
        if (!dispatch) return view
        if (dispatch.claimedBy && fact.signer !== dispatch.claimedBy) return view  // only claimer confirms
        dispatch.confirmedAt = fact.lamport
        dispatch.status = 'confirmed'
      }

      else if (payload.kind === 'DispatchBlocked') {
        const dispatch = view.dispatches.get(payload.dispatchId)
        if (!dispatch) return view
        dispatch.blockedReason = payload.reason
        dispatch.status = 'blocked'
      }

      return view
    },
    asOfLamport,
  )
}

// ═══════════════════════════════════════════════════════════════════════════
// Node — a participant with an id, a local policy, and private local state.
// Nodes never see each other. They only see the log.
// ═══════════════════════════════════════════════════════════════════════════

type NodeLocalState = Record<string, unknown>
type NodePolicy = (view: View, log: FactLog, self: string, local: NodeLocalState) => void

class Node {
  local: NodeLocalState = {}
  constructor(
    public id: string,
    public log: FactLog,
    public policy: NodePolicy,
  ) {}

  tick() {
    const view = project(this.log)
    this.policy(view, this.log, this.id, this.local)
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario
// ═══════════════════════════════════════════════════════════════════════════

const log = new FactLog()

// Bootstrap facts. In a real system these could come from a config node,
// a network join handshake, or the first node to come online. Here: inline.
log.append('bootstrap', {
  kind: 'SkillRegistered',
  skillId: 'rollback-signup',
  description: 'Revert the last deploy to restore signup flow',
  requiresApprovalFromRole: 'owner',
})
log.append('bootstrap', {
  kind: 'RoleAttested',
  subject: 'owner-dashboard',
  role: 'owner',
})

// ── detector-1: observes a noisy sensor, proposes dispatch when sustained drop ──
const detector = new Node('detector-1', log, (view, log, self, local) => {
  const tick = ((local.tick as number) ?? 0) + 1
  local.tick = tick

  // Emit sensor reading
  const inIncidentWindow = (tick >= 6 && tick <= 11) || (tick >= 22 && tick <= 26)
  const realRate = inIncidentWindow ? 20 : 100
  const observedRate = Math.max(0, realRate + Math.random() * 30 - 15)
  log.append(self, { kind: 'SensorEmitted', sensorId: 'signup-rate', reading: { signupsPerMinute: observedRate, atTick: tick } })

  // Compute 4-tick moving average
  const history = view.sensorHistory.get('signup-rate') ?? []
  const recentFour = history.slice(-4) as { signupsPerMinute: number }[]
  if (recentFour.length < 4) return
  const movingAverage = recentFour.reduce((a, b) => a + b.signupsPerMinute, 0) / recentFour.length
  if (movingAverage >= 50) return

  // Don't propose if there's already an outstanding dispatch
  const outstanding = [...view.dispatches.values()].some(d =>
    d.status !== 'confirmed' && d.status !== 'blocked'
  )
  if (outstanding) return

  const dispatchId = `d${log.entries.length + 1}`
  log.append(self, {
    kind: 'DispatchProposed',
    dispatchId,
    skillId: 'rollback-signup',
    payload: { reason: `4-tick avg ${movingAverage.toFixed(1)} < 50` },
  })
})

// ── owner-dashboard: approves anything whose required role matches its attested role ──
const ownerDashboard = new Node('owner-dashboard', log, (view, log, self) => {
  const myRoles = view.rolesHeldBy.get(self) ?? new Set<string>()
  for (const dispatch of view.dispatches.values()) {
    if (dispatch.status !== 'awaiting-approval') continue
    const skill = view.skills.get(dispatch.skillId)
    const requiredRole = skill?.requiresApprovalFromRole
    if (!requiredRole) continue
    if (!myRoles.has(requiredRole)) continue
    // Don't double-approve
    const alreadyApproved = log.entries.some(f =>
      f.payload.kind === 'DispatchApproved' && f.payload.dispatchId === dispatch.id && f.signer === self
    )
    if (alreadyApproved) continue
    log.append(self, { kind: 'DispatchApproved', dispatchId: dispatch.id })
  }
  return
})

// ── alice-laptop: human agent. Offers herself. Claims what she can.
//    Local policy: defer to any script agent offering the same skill.
const aliceLaptop = new Node('alice-laptop', log, (view, log, self, local) => {
  const localTick = ((local.tick as number) ?? 0) + 1
  local.tick = localTick

  // Offer once
  if (!local.offered) {
    log.append(self, { kind: 'AgentOffered', agentId: self, agentKind: 'human', skills: ['rollback-signup'] })
    local.offered = true
    local.claimedAtTick = {} as Record<string, number>
    return
  }

  const claimedAtTick = local.claimedAtTick as Record<string, number>

  // Claim anything claimable I can do — but defer to script agents offering the same skill
  for (const dispatch of view.dispatches.values()) {
    if (dispatch.status !== 'claimable') continue
    const me = view.agents.get(self)
    if (!me?.skills.includes(dispatch.skillId)) continue
    const aScriptExistsForSkill = [...view.agents.values()].some(a =>
      a.kind === 'script' && a.skills.includes(dispatch.skillId)
    )
    if (aScriptExistsForSkill && me.kind === 'human') continue     // defer to script
    log.append(self, { kind: 'DispatchClaimed', dispatchId: dispatch.id, byAgent: self })
    claimedAtTick[dispatch.id] = localTick
  }

  // Confirm anything I claimed, 3 local ticks after claiming
  for (const dispatch of view.dispatches.values()) {
    if (dispatch.status !== 'working') continue
    if (dispatch.claimedBy !== self) continue
    const mine = claimedAtTick[dispatch.id]
    if (mine !== undefined && localTick - mine >= 3) {
      log.append(self, { kind: 'DispatchConfirmed', dispatchId: dispatch.id })
      delete claimedAtTick[dispatch.id]
    }
  }
})

// ═══════════════════════════════════════════════════════════════════════════
// Observer — prints facts as they land so you can read the collaboration
// ═══════════════════════════════════════════════════════════════════════════

function summarizeFact(fact: Fact): string | null {
  const p = fact.payload
  if (p.kind === 'SensorEmitted') return null   // too chatty
  const prefix = `  [L${String(fact.lamport).padStart(3, ' ')}] ${fact.signer.padEnd(16, ' ')} `
  if (p.kind === 'SkillRegistered')      return prefix + `📘 SkillRegistered ${p.skillId}${p.requiresApprovalFromRole ? ` (requires ${p.requiresApprovalFromRole})` : ''}`
  if (p.kind === 'RoleAttested')         return prefix + `📜 RoleAttested ${p.subject} has role '${p.role}'`
  if (p.kind === 'AgentOffered')         return prefix + `🤝 AgentOffered ${p.agentId} (${p.agentKind}) for [${p.skills.join(', ')}]`
  if (p.kind === 'DispatchProposed')     return prefix + `📮 DispatchProposed ${p.dispatchId} skill=${p.skillId} ${JSON.stringify(p.payload)}`
  if (p.kind === 'DispatchApproved')     return prefix + `✅ DispatchApproved ${p.dispatchId}`
  if (p.kind === 'DispatchClaimed')      return prefix + `🙋 DispatchClaimed ${p.dispatchId} by ${p.byAgent}`
  if (p.kind === 'DispatchConfirmed')    return prefix + `🟢 DispatchConfirmed ${p.dispatchId}`
  if (p.kind === 'DispatchBlocked')      return prefix + `🔒 DispatchBlocked ${p.dispatchId}: ${p.reason}`
  return null
}

log.subscribe(fact => {
  const line = summarizeFact(fact)
  if (line) console.log(line)
})

// ═══════════════════════════════════════════════════════════════════════════
// Run
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  const nodes: Node[] = [detector, ownerDashboard, aliceLaptop]

  console.log('\n── phase 1: three nodes online (detector, owner, alice) ──')
  for (let i = 1; i <= 16; i++) {
    for (const node of nodes) node.tick()
    const outstanding = [...project(log).dispatches.values()].find(d =>
      d.status !== 'confirmed' && d.status !== 'blocked'
    )
    const label = outstanding ? `${outstanding.id}:${outstanding.status}` : '(nothing outstanding)'
    console.log(`  tick ${String(i).padStart(2)} │ ${label}`)
  }

  console.log('\n── phase 2: rollback-vps joins (a proven script agent) ──')
  const rollbackVps = new Node('rollback-vps', log, (view, log, self, local) => {
    if (!local.offered) {
      log.append(self, { kind: 'AgentOffered', agentId: self, agentKind: 'script', skills: ['rollback-signup'] })
      local.offered = true
      return
    }
    // Claim anything claimable I can do
    for (const dispatch of view.dispatches.values()) {
      if (dispatch.status !== 'claimable') continue
      const me = view.agents.get(self)
      if (!me?.skills.includes(dispatch.skillId)) continue
      log.append(self, { kind: 'DispatchClaimed', dispatchId: dispatch.id, byAgent: self })
    }
    // Scripts confirm same tick
    for (const dispatch of view.dispatches.values()) {
      if (dispatch.status !== 'working') continue
      if (dispatch.claimedBy !== self) continue
      log.append(self, { kind: 'DispatchConfirmed', dispatchId: dispatch.id })
    }
  })
  // Run script BEFORE alice each tick so it claims first (alice would defer anyway).
  nodes.push(rollbackVps)
  // Move rollbackVps earlier than alice in the tick order
  const aliceIndex = nodes.indexOf(aliceLaptop)
  nodes.splice(aliceIndex, 0, nodes.pop()!)

  for (let i = 1; i <= 14; i++) {
    for (const node of nodes) node.tick()
    const outstanding = [...project(log).dispatches.values()].find(d =>
      d.status !== 'confirmed' && d.status !== 'blocked'
    )
    const label = outstanding ? `${outstanding.id}:${outstanding.status}` : '(nothing outstanding)'
    console.log(`  tick ${String(i).padStart(2)} │ ${label}`)
  }

  console.log(`\n── log has ${log.entries.length} facts. Audit everything by replaying. ──`)
  console.log(`   Dispatches in final view:`)
  for (const d of project(log).dispatches.values()) {
    console.log(`     ${d.id}  skill=${d.skillId}  status=${d.status}  claimedBy=${d.claimedBy ?? '—'}  L${d.proposedAt}→${d.confirmedAt ?? '?'}`)
  }
}

main()
