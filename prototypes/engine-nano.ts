// engine-nano.ts
//
// ═══════════════════════════════════════════════════════════════════════════
// What this is
// ═══════════════════════════════════════════════════════════════════════════
//
// The minimum viable shape of an agent harness — the same primitive
// category as Claude Code, a CI/CD runner, or a workflow engine.
//
// An agent harness is a tick loop that does three things repeatedly:
//   (1) observes the world (sensors, state, signals),
//   (2) decides some work needs doing,
//   (3) dispatches that work to an agent who can fulfill it.
//
// Claude Code is an instance of this pattern. Its "agent" is Claude,
// its "skills" are tool-use blocks, its "middleware" is the permission
// system and hooks, its "queue" is the pending tool-calls list, and its
// "confirmation" is the tool-call result returning. Claude Code then
// adds reliability (streaming, retries, schema validation, UX polish)
// ON TOP OF this loop — those are not new primitives in the core model.
//
// This file implements the core loop with nothing on top. It fits in
// one file so the shape is inspectable. Every primitive you'd need to
// build a real harness is named, but there's no reliability layer yet.
//
// ═══════════════════════════════════════════════════════════════════════════
// The seven primitives
// ═══════════════════════════════════════════════════════════════════════════
//
//   Skill       A declarative unit of work. Has an id, a description, and
//               an optional approval requirement. Does NOT know who will
//               fulfill it. Skills are the "action buttons" — the only
//               surface where trust/policy is enforced.
//
//   Agent       A worker that claims one or more skills and processes a
//               queue of assigned work. Has a kind (human | ai | script)
//               which influences selection but doesn't change the
//               protocol. A human agent and a script agent look
//               identical to the dispatcher.
//
//   Engine      A reactive loop: (world, self) => state. Reads other
//               engines' state via the tick ledger. May dispatch skills.
//               May hold roles (for approval).
//
//   Middleware  A composable pipeline applied at every dispatch.
//               Returns allow | pause (for approval) | block.
//               RBAC is the one middleware implemented here.
//
//   Role        A string token. Engines hold roles; skills may require
//               a role for approval. Replaces org-chart trees —
//               scoping is expressed through role names.
//
//   Dispatch    One instance of "this skill, with this payload, by this
//               engine, for this agent" tracked through its lifecycle:
//               awaiting-approval → queued → working → confirmed.
//
//   Tick ledger Per-engine state history. Every engine's return value
//               is appended each tick. Everything reactive reads from it.
//
// ═══════════════════════════════════════════════════════════════════════════
// The scenario below
// ═══════════════════════════════════════════════════════════════════════════
//
// A customer signup rate sensor is noisy. An anomaly detector takes
// a 4-tick moving average and flags sustained drops. A responder
// dispatches a `rollback-signup` skill when it sees an anomaly.
// The skill requires approval from an engine holding the 'owner' role.
// An 'owner' engine auto-approves matching dispatches.
//
// Phase 1: only a human agent (on-call-alice) claims the skill.
//   Dispatches are queued to alice; alice takes 3 ticks to finish.
//
// Phase 2: a proven script agent registers for the same skill.
//   Selection prefers script > ai > human, so the script agent now
//   gets the work. The skill still requires owner approval — the
//   approval gate is orthogonal to who fulfills. The script finishes
//   in one tick.
//
// Run: bun engine-nano.ts

// ═══════════════════════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════════════════════

type AgentKind = 'human' | 'ai' | 'script'
type Role = string

type Skill = {
  id: string
  description: string
  requires_approval?: Role
}

type DispatchStatus =
  | 'awaiting-approval'  // middleware paused it; a role-holder needs to approve
  | 'queued'             // in the assigned agent's queue, not started yet
  | 'working'            // the agent is processing it
  | 'confirmed'          // the agent reported done
  | 'blocked'            // middleware denied, or no capable agent

type Dispatch = {
  id: string
  skill: string
  dispatcher: string    // id of the engine that asked for the work
  agent: string         // id of the agent assigned (or '∅' if blocked early)
  agentKind: AgentKind
  payload: unknown
  dispatchedAtTick: number
  status: DispatchStatus
  blockedReason?: string
}

type Agent = {
  id: string
  kind: AgentKind
  skills: string[]          // skill ids this agent claims to fulfill
  queue: string[]           // dispatch ids assigned to this agent (FIFO)
  // Called by the runtime each tick with the head of this agent's queue.
  // Returns 'working' to keep the item queued, 'done' to confirm and pop.
  work: (dispatch: Dispatch, world: World) => 'working' | 'done'
}

type Engine = {
  id: string
  roles?: Role[]
  run: (world: World, self: string) => Record<string, unknown>
}

type MiddlewareResult =
  | { kind: 'allow' }
  | { kind: 'pause'; needRole: Role }
  | { kind: 'block'; reason: string }

type Middleware = (dispatch: Dispatch, world: World) => MiddlewareResult

const STATUS_EMOJI: Record<DispatchStatus, string> = {
  'awaiting-approval': '🟡',
  queued: '📥',
  working: '🟠',
  confirmed: '🟢',
  blocked: '🔒',
}

// ═══════════════════════════════════════════════════════════════════════════
// Runtime
// ═══════════════════════════════════════════════════════════════════════════

class World {
  skills = new Map<string, Skill>()
  agents = new Map<string, Agent>()
  engines = new Map<string, Engine>()
  ledger = new Map<string, Record<string, unknown>[]>()  // per-engine tick history
  dispatches = new Map<string, Dispatch>()
  approvals: { dispatchId: string; approver: string; atTick: number }[] = []
  middleware: Middleware[] = []
  currentTick = 0

  registerSkill(skill: Skill) {
    this.skills.set(skill.id, skill)
    return this
  }

  registerAgent(agent: Agent) {
    this.agents.set(agent.id, agent)
    return this
  }

  registerEngine(engine: Engine) {
    this.engines.set(engine.id, engine)
    this.ledger.set(engine.id, [])
    return this
  }

  useMiddleware(middleware: Middleware) {
    this.middleware.push(middleware)
    return this
  }

  enginesHoldingRole(role: Role): string[] {
    return [...this.engines.values()]
      .filter(engine => engine.roles?.includes(role))
      .map(engine => engine.id)
  }

  // Policy: among agents claiming a skill, prefer script > ai > human.
  // "Progressive automation" = adding a more-trusted agent over time.
  pickAgentForSkill(skillId: string): Agent | undefined {
    const kindRank: Record<AgentKind, number> = { script: 3, ai: 2, human: 1 }
    const capableAgents = [...this.agents.values()].filter(agent =>
      agent.skills.includes(skillId)
    )
    return capableAgents.sort((a, b) => kindRank[b.kind] - kindRank[a.kind])[0]
  }

  recordApproval(dispatchId: string, approverId: string) {
    const alreadyApproved = this.approvals.some(
      a => a.dispatchId === dispatchId && a.approver === approverId
    )
    if (alreadyApproved) return
    this.approvals.push({ dispatchId, approver: approverId, atTick: this.currentTick })
    console.log(`  ✅ ${approverId} approved ${dispatchId}`)
  }

  private applyMiddleware(dispatch: Dispatch): MiddlewareResult {
    for (const middleware of this.middleware) {
      const result = middleware(dispatch, this)
      if (result.kind !== 'allow') return result
    }
    return { kind: 'allow' }
  }

  // Engines call this to ask for work. The runtime picks an agent,
  // runs middleware, and either queues the work or records a pause/block.
  dispatch(requesterId: string, skillId: string, payload: unknown): string {
    const dispatchId = `${requesterId}→${skillId}#${this.currentTick}`
    const chosenAgent = this.pickAgentForSkill(skillId)

    if (!chosenAgent) {
      const noAgentDispatch: Dispatch = {
        id: dispatchId, skill: skillId, dispatcher: requesterId,
        agent: '∅', agentKind: 'script', payload,
        dispatchedAtTick: this.currentTick,
        status: 'blocked', blockedReason: 'no capable agent',
      }
      this.dispatches.set(dispatchId, noAgentDispatch)
      console.log(`  🔒 ${dispatchId} — no capable agent`)
      return dispatchId
    }

    const newDispatch: Dispatch = {
      id: dispatchId, skill: skillId, dispatcher: requesterId,
      agent: chosenAgent.id, agentKind: chosenAgent.kind, payload,
      dispatchedAtTick: this.currentTick,
      status: 'queued',
    }

    const middlewareResult = this.applyMiddleware(newDispatch)
    if (middlewareResult.kind === 'block') {
      newDispatch.status = 'blocked'
      newDispatch.blockedReason = middlewareResult.reason
    } else if (middlewareResult.kind === 'pause') {
      newDispatch.status = 'awaiting-approval'
      newDispatch.blockedReason = `needs ${middlewareResult.needRole}`
    } else {
      // Allowed: assign to the agent's queue
      chosenAgent.queue.push(dispatchId)
    }

    this.dispatches.set(dispatchId, newDispatch)
    const detail = newDispatch.blockedReason ?? JSON.stringify(payload)
    console.log(
      `  ${STATUS_EMOJI[newDispatch.status]} ${dispatchId} → ${chosenAgent.id} (${chosenAgent.kind}) — ${detail}`
    )
    return dispatchId
  }

  async tick() {
    this.currentTick++

    // 1. Re-check paused dispatches: approvals may have arrived last tick
    for (const dispatch of this.dispatches.values()) {
      if (dispatch.status !== 'awaiting-approval') continue
      const recheck = this.applyMiddleware(dispatch)
      if (recheck.kind === 'allow') {
        const agent = this.agents.get(dispatch.agent)!
        agent.queue.push(dispatch.id)
        dispatch.status = 'queued'
        console.log(`  ▶ ${dispatch.id} approved — queued to ${agent.id}`)
      }
    }

    // 2. Each agent processes the head of its queue
    for (const agent of this.agents.values()) {
      if (agent.queue.length === 0) continue
      const headDispatchId = agent.queue[0]
      const dispatch = this.dispatches.get(headDispatchId)!
      if (dispatch.status === 'queued') dispatch.status = 'working'
      const workResult = agent.work(dispatch, this)
      if (workResult === 'done') {
        dispatch.status = 'confirmed'
        agent.queue.shift()
        console.log(`  🟢 ${headDispatchId} confirmed by ${agent.id} (tick ${this.currentTick})`)
      }
    }

    // 3. Run engines — they read the updated world and emit new state
    for (const [engineId, engine] of this.engines) {
      const newState = await engine.run(this, engineId)
      this.ledger.get(engineId)!.push(newState)
    }
  }

  // Convenience accessor: the most recent state emitted by an engine.
  latest(engineId: string): Record<string, unknown> {
    return this.ledger.get(engineId)?.at(-1) ?? {}
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Middleware: RBAC
// ═══════════════════════════════════════════════════════════════════════════

// If the skill requires a role, pause until some engine holding that
// role has recorded an approval for this dispatch.
const rbacMiddleware: Middleware = (dispatch, world) => {
  const skill = world.skills.get(dispatch.skill)!
  if (!skill.requires_approval) return { kind: 'allow' }

  const requiredRole = skill.requires_approval
  const validApprovers = new Set(world.enginesHoldingRole(requiredRole))
  const hasValidApproval = world.approvals.some(
    approval => approval.dispatchId === dispatch.id && validApprovers.has(approval.approver)
  )

  if (hasValidApproval) return { kind: 'allow' }
  return { kind: 'pause', needRole: requiredRole }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario
// ═══════════════════════════════════════════════════════════════════════════

const world = new World().useMiddleware(rbacMiddleware)

// The skill: what work could be done, and what it needs before it happens.
world.registerSkill({
  id: 'rollback-signup',
  description: 'Revert the last deploy to restore the customer signup flow',
  requires_approval: 'owner',
})

// Human agent: claims the skill. "Does work" for 3 ticks then marks done.
world.registerAgent({
  id: 'on-call-alice',
  kind: 'human',
  skills: ['rollback-signup'],
  queue: [],
  work: (dispatch, world) => {
    const ticksSpentOnThisTask = world.currentTick - dispatch.dispatchedAtTick
    if (ticksSpentOnThisTask < 3) return 'working'
    console.log(`    👤 on-call-alice: finished ${dispatch.id}`)
    return 'done'
  },
})

// Signup rate sensor. Real rate drops to 20 during incident windows
// (ticks 6-11 and 22-26); noise of ±15 is always present.
world.registerEngine({
  id: 'signup-rate',
  run: (world) => {
    const tick = world.currentTick
    const inIncidentWindow = (tick >= 6 && tick <= 11) || (tick >= 22 && tick <= 26)
    const realRate = inIncidentWindow ? 20 : 100
    const noise = Math.random() * 30 - 15
    const observedRate = Math.max(0, realRate + noise)
    return { signupsPerMinute: observedRate }
  },
})

// Anomaly detector. A 4-tick moving average rejects single-tick spikes
// and only fires when the signal has been degraded for a sustained window.
// This is the "PID signal attenuation" primitive.
world.registerEngine({
  id: 'anomaly-detector',
  run: (world) => {
    const sensorHistory = world.ledger.get('signup-rate') ?? []
    const windowSize = 4
    const recentSamples = sensorHistory.slice(-windowSize) as { signupsPerMinute: number }[]
    if (recentSamples.length < windowSize) {
      return { isAnomalous: false, movingAverage: null }
    }
    const sum = recentSamples.reduce((acc, s) => acc + s.signupsPerMinute, 0)
    const movingAverage = sum / recentSamples.length
    return {
      isAnomalous: movingAverage < 50,
      movingAverage: +movingAverage.toFixed(1),
    }
  },
})

// Responder. When the anomaly flag turns true, dispatch the rollback skill.
// Remember the dispatch id so we don't fire again while one is in flight.
// Clear memory once the prior is confirmed AND the world has recovered.
world.registerEngine({
  id: 'responder',
  run: (world, self) => {
    const anomalyState = world.latest('anomaly-detector') as { isAnomalous: boolean }
    const previousState = world.latest(self) as { dispatchId?: string }
    const priorDispatchId = previousState.dispatchId

    if (priorDispatchId) {
      const priorDispatch = world.dispatches.get(priorDispatchId)!
      const canForgetPrior = priorDispatch.status === 'confirmed' && !anomalyState.isAnomalous
      if (canForgetPrior) return { status: '🟡' }
      return { status: STATUS_EMOJI[priorDispatch.status], dispatchId: priorDispatchId }
    }

    if (anomalyState.isAnomalous) {
      const newDispatchId = world.dispatch(self, 'rollback-signup', {
        reason: '4-tick moving average dropped below 50',
      })
      return { status: STATUS_EMOJI.working, dispatchId: newDispatchId }
    }

    return { status: '🟡' }
  },
})

// Owner. Holds the 'owner' role. Approves any awaiting-approval dispatch
// whose required role is 'owner'. This is the RBAC counterparty to the
// rbacMiddleware: middleware checks for approval; owner engine records it.
world.registerEngine({
  id: 'owner',
  roles: ['owner'],
  run: (world, self) => {
    for (const dispatch of world.dispatches.values()) {
      if (dispatch.status !== 'awaiting-approval') continue
      const skill = world.skills.get(dispatch.skill)!
      if (skill.requires_approval !== 'owner') continue
      world.recordApproval(dispatch.id, self)
    }
    return { approved: true }
  },
})

// ═══════════════════════════════════════════════════════════════════════════
// Run
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  const printRow = (tickNumber: number) => {
    const rate = (world.latest('signup-rate') as any).signupsPerMinute?.toFixed(1) ?? '—'
    const average = (world.latest('anomaly-detector') as any).movingAverage ?? '—'
    const isAnomalous = (world.latest('anomaly-detector') as any).isAnomalous ?? false
    const responderStatus = (world.latest('responder') as any).status ?? '—'
    const aliceQueueDepth = world.agents.get('on-call-alice')?.queue.length ?? 0
    const scriptQueueDepth = world.agents.get('rollback-script-v1')?.queue.length ?? 0
    console.log(
      `${String(tickNumber).padStart(4)} | ${String(rate).padStart(7)} | ` +
      `${String(average).padStart(5)} | ${String(isAnomalous).padStart(7)} | ` +
      `${responderStatus}  | alice:${aliceQueueDepth} script:${scriptQueueDepth}`
    )
  }

  console.log('tick | signups | avg   | anomaly | resp | queues')
  console.log('── phase 1: only on-call-alice (human) claims rollback-signup ──')
  for (let i = 1; i <= 16; i++) {
    await world.tick()
    printRow(i)
  }

  console.log('\n── phase 2: rollback-script-v1 (script) now also claims it ──\n')
  world.registerAgent({
    id: 'rollback-script-v1',
    kind: 'script',
    skills: ['rollback-signup'],
    queue: [],
    work: (dispatch) => {
      console.log(`    🤖 rollback-script-v1 executing: ${JSON.stringify(dispatch.payload)}`)
      return 'done'  // deterministic scripts finish in one tick
    },
  })
  for (let i = 1; i <= 14; i++) {
    await world.tick()
    printRow(i)
  }
}

main()
