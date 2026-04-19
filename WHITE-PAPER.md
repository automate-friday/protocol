# Automate Friday — A Fact-Log Protocol for Agent Coordination

**Author:** Jacob Haugen
**Version:** 0.2 (draft, for discussion)
**Date:** 2026-04-19
**Status:** Pre-release. Prototypes exist; production implementation does not.
**Repo:** github.com/automate-friday/protocol

---

## Abstract

This paper describes a minimal protocol for letting humans, AI agents, and deterministic scripts collaborate on sequences of skills across machines and organizations, without any party imperatively commanding another.

The protocol is built on a single primitive: a signed, append-only **fact log** with domain-aware projection. Every coordination action — a skill being registered, an agent offering capability, a unit of work being dispatched, an approval being granted, a job being claimed or confirmed — is a signed fact appended to the log. Every party computes system state by projecting the log through a deterministic reducer. The log is the coordination fabric; the reducer is the semantics.

The mental model is **Git for agent coordination**. Where Git lets distributed parties collaborate on source code, this protocol lets distributed parties collaborate on behavior in the world.

---

## Why this matters

Three things are true about where automation is today:

1. **AI agents can do the work. You just can't trust them to always do it right.** Claude and peers are capable enough to handle real business tasks end-to-end — scheduling, triage, drafting, research, routine operations. They are not reliable enough to be handed the keys. Every serious AI deployment today is a trust negotiation in disguise.

2. **Real automation is a gradient. Today, the gradient is a cliff.** Automation maturity is a ladder: human → human-with-AI-assist → AI-with-approval → AI-unattended → deterministic script. Each rung should be the same work, done by a different party, with the same interface. Instead, every tool assumes a single rung. Moving up the ladder means rebuilding the integration from scratch. The gradient is impossible to express, so nobody expresses it.

3. **Any real workflow involves multiple parties, often across organizations, always across time.** Your agent on your laptop and your client's agent on their VPS should be able to collaborate on a shared outcome without one hosting the other's code. The party mix will also change over time: the human approving today is the AI approving next quarter is the script running unattended next year. No tool treats this as first-class.

These three problems have one root: **the absence of a shared coordination substrate for work getting done.** Every existing tool — Zapier, Temporal, CrewAI, Kubernetes, bespoke internal pipelines — assumes one runtime, one moment, one authority. That assumption IS the problem.

**The solution reduces to one observation.** Git solved distributed collaboration on *code* by inventing a primitive: the signed append-only log with content addressing. The same primitive, applied to coordinating *behavior in the world*, dissolves every layer of the problem above:

- **Trust gap** → approvals are signed facts; audit is the log itself; every action has provenance.
- **Ladder problem** → reputation is a projection over confirmation facts; trust graduation is a reducer rule that relaxes approval requirements once track record accumulates. Same skill, same interface; fulfiller changes silently over time.
- **Coordination gap** → cross-party collaboration is subscribing to a shared log topic. No integration project. Agents from different organizations coordinate by appending facts with their own signatures.

**The product's name earns itself.** *Automate Friday* means your system takes over enough of the week that you can take Friday off. Progressive automation — the namesake — IS the product. The Git-style protocol is *how* you get there.

---

## 1. The problem

Existing automation tooling divides cleanly along two axes, and the useful cell is empty.

| | Single party | Multi-party |
|---|---|---|
| **Imperative** | Bash scripts, cron, Zapier, n8n, Make | Procurement systems, ERP, custom integrations |
| **Declarative** | Kubernetes operators, Terraform, reactive state systems | **← empty** |

Single-party imperative automation (Zapier, n8n) is solved, and the 200-line "Claude Code clone" tutorial demonstrates that a smart agent with one general tool can do remarkable amounts of work alone.

The gap is **multi-party declarative**: how do a human on one machine, an AI agent on another machine, and a deterministic script on a third collaborate on sequences of skills, where:

- No one imperatively commands another (agents agree to work; they aren't assigned it)
- Approval workflows are first-class, not bolt-ons
- Trust graduates over time (work that required human approval last month can run unattended this month as track record accumulates)
- Every decision is auditable with provenance
- Agents can belong to different organizations without per-pair integration work
- Adding a new participant, skill, role, or policy is trivial, not a rewrite

No existing framework hits all six. Workflow engines (Temporal, Inngest, Prefect) are single-runtime imperative DAGs. Agent frameworks (CrewAI, LangGraph, AutoGen) are single-runtime conversational pipelines. RPA tools are single-machine scripted UI automation. Blockchain smart contracts get distributed coordination right but impose consensus and cost that's inappropriate for most business workflows.

What's needed is a coordination primitive that's **distributed, append-only, signed, and reducer-driven**, without the consensus overhead of a blockchain.

---

## 2. The primitive: a signed, append-only fact log

The entire protocol rests on one data structure:

```typescript
type Fact = {
  id: string           // content-addressed: hash(prev, payload, signer)
  lamport: number      // causal order; no global clock required
  signer: string       // identity appending this fact (cryptographically signed in production)
  payload: FactPayload // typed coordination event
}

interface FactLog {
  append(signer: string, payload: FactPayload): Fact
  subscribe(filter?: Filter): AsyncIterable<Fact>
  project<T>(reducer: (acc: T, fact: Fact) => T, seed: T, asOf?: number): T
}
```

The log has three operations: **append a fact**, **subscribe to facts**, **project the log to derive any view**. That is the entire storage API.

A fact's payload is one of a small set of typed events:

- `SkillRegistered` — a unit of work has been declared
- `RoleAttested` — a subject holds a role (attested by a signer)
- `AgentOffered` — an agent advertises it can fulfill certain skills at a given kind (human / ai / script)
- `DispatchProposed` — a unit of work is requested
- `DispatchApproved` — a role-holder authorizes a dispatch to proceed
- `DispatchClaimed` — an agent has taken on a dispatch
- `DispatchConfirmed` — the claimer has completed the work
- `DispatchBlocked` — the dispatch is denied or cannot proceed
- Additional payloads (observations, reputation signals, economic events) extend the set without protocol change

Every participant — every human, every AI agent, every script — is a **node** with an identity and a local policy. Nodes interact *only* by appending facts and projecting views. No node calls a method on another node. No node holds a callback or closure over another node.

---

## 3. Architecture in one page

Four kinds of participants, all implemented as nodes with different policies:

- **Sensors** — append observations (webhook bridges, schedulers, monitoring streams)
- **Engines** — reactive policies that observe projections and propose dispatches
- **Agents** — advertise capabilities, claim dispatches, execute, confirm
- **Approvers** — hold roles, record approvals that unblock gated dispatches

These are not separate primitives. They are **roles a node can play**. One node can simultaneously sense, propose, claim, and approve. The separation is purely behavioral.

Coordination flows through one pattern — the **dispatch lifecycle** — which maps directly onto a pull request:

```
DispatchProposed    ← "someone opens a PR"
  → (if skill requires approval)
DispatchApproved    ← "a reviewer approves"
DispatchClaimed     ← "someone self-assigns"
DispatchConfirmed   ← "the PR merges"
  (or DispatchBlocked ← "closed without merging")
```

Middleware is expressed as **rules inside the reducer**. Role-based access control, rate limiting, budget caps, content filtering — each is a predicate that either accepts or rejects a state transition when a fact is projected. Because every node runs the same reducer over the same log, every node agrees on what state the system is in.

Extending the protocol means adding a new payload kind and a corresponding rule in the reducer. It does not require touching the log implementation or the node implementation.

---

## 4. The Git mental model

The cleanest way to understand the protocol is by analogy to Git.

| Git | Automate Friday |
|---|---|
| Repo | A fact log (one per workflow) |
| Commit | A fact |
| Author / committer | Node signer identity |
| **Pull request** | **Dispatch lifecycle (Proposed → Approved → Claimed → Confirmed)** |
| Review | Approval fact |
| Merge | Confirmation fact |
| Open issues | Outstanding DispatchProposed |
| GitHub Actions | Engine (reactive node policy) |
| Pre-receive hooks | Middleware / reducer rule |
| Branch protection rules | RBAC enforced by reducer |
| CODEOWNERS | Role attestations + approval requirements |
| Collaborators | Nodes (humans, AI agents, scripts) |
| Permissions | Roles, enforced via `RoleAttested` facts |
| Fork | Organizational split with shared history |
| Submodule | Cross-workflow skill reference |
| Project board | Projection of dispatches by status |

The analogy runs deep. **Dispatches are pull requests.** A DispatchProposed is a PR being opened. A DispatchApproved is a reviewer sign-off. A DispatchClaimed is an assignee picking up the PR. A DispatchConfirmed is the merge. The approval ritual is identical.

This matters practically for two reasons:

1. **The onboarding cost is near zero.** Every developer and every modern AI agent already knows how PR workflows function. The training data is a decade of GitHub interactions.
2. **The implementation substrate can literally be Git.** A production deployment could use a Git repository as the underlying log, with facts stored as commits to a `/facts/` directory. Nodes pull, project, decide, append, push. The ecosystem of Git hosting, replication, authentication, and tooling transfers directly.

One place the analogy cracks honestly: Git's history is linear-per-branch, while our log is a causal partial order. When two agents concurrently claim the same dispatch, there is no conflict resolution — the reducer silently picks one (by lamport order, then signer tiebreak) and ignores the other. No merge strategy, no merge conflict. This is strictly an improvement over Git semantics for this use case.

---

## 5. What this unlocks

Capabilities that fall out of the primitive without additional infrastructure:

- **Multi-party, multi-organization collaboration without integration plumbing.** Two companies share a log topic. Their agents collaborate. Neither hosts the other's code.
- **Full audit trail as a free byproduct.** Every decision is a signed fact with a timestamp and a causal chain. Compliance review becomes "dump the log."
- **Time travel and replay.** `project(log, asOfLamport)` reconstructs the exact system state at any historical moment. Debugging, training data generation, and policy experimentation all become projection operations.
- **Progressive automation via reputation.** Track record is a projection: count confirmations signed by an agent for a given skill. Trust graduation is a reducer rule: "if agent has >N confirmations at >R success rate in last period, this skill no longer requires owner approval when claimed by this agent." Same agent, same skill, same log — gradually less friction.
- **Policy as pure function.** New middleware is a reducer rule anyone can write and distribute. Experimentation requires no production deployment — run new rules against the historical log.
- **Offline-first operation.** A participant can append facts to their local log while disconnected; replication on reconnect is deterministic because lamport ordering and reducer determinism combine to guarantee convergence.
- **Skill / agent marketplaces.** Offers, bids, prices, reputations, attestations — all are fact kinds. A marketplace emerges from projection, not from a central platform.
- **Cross-trust-boundary delegation.** Node A can weight attestations from Node B at a specific confidence. "I trust the SRE team's endorsements at 1.0, community endorsements at 0.1." Trust is local policy over public facts.

---

## 6. What this deliberately doesn't try to be

To keep the protocol minimal and prevent scope creep:

- **Not a consensus protocol.** Lamport ordering is sufficient for deterministic projection; no Byzantine fault tolerance, no proof-of-work, no proof-of-stake. Participants agree because they project the same log through the same rules.
- **Not a low-latency RPC replacement.** Sub-100ms synchronous request-reply is not the design center. Within a node, use normal function calls. Across nodes, use dispatch + confirm pairs with deadlines.
- **Not a high-frequency data bus.** Logs with 10,000+ events per second become expensive to replicate and project. Use local aggregator engines to window raw streams into summary facts at 1/sec or 1/min granularity.
- **Not a UI or workflow builder.** The protocol is substrate. Visual builders, dashboards, and ergonomic DSLs are product-layer concerns that slot in above the protocol.
- **Not a payment rail.** Economic facts (`OfferPricedFor`, `DispatchPaid`, `CollateralSlashed`) can be modeled, but settlement integrates with external systems; the log records, it does not custody funds.

These limitations have well-known adapter patterns. The protocol does not try to solve them natively because they are orthogonal concerns with mature external solutions.

---

## 7. Current status

Two working prototypes exist as single TypeScript files in the reference repository:

- **`engine-nano.ts`** (~300 lines) — a single-runtime, in-memory implementation of the protocol's vocabulary (skills, agents, engines, middleware, RBAC). Useful for understanding the primitives but does not survive distribution.

- **`fact-log-nano.ts`** (~300 lines) — a log-first implementation where all the primitives reduce to append-fact and project-log. Multiple nodes collaborate through a single in-memory log. Swapping the log implementation for a WebSocket stream, a Convex table, or a Git repository distributes the system without changing node code.

Both are throwaway prototypes for vocabulary clarification, not production. A production implementation would integrate cryptographic signatures, replication transport, persistent storage, and a trust layer for sensitive skill classes.

---

## 8. Open source intent

This protocol is intended to be open source. The primitive itself — signed append-only logs with reducer-driven projection — is not novel in isolation; it draws on decades of work in distributed systems, event sourcing, Promise Theory (Mark Burgess), capability-based security, and Git's architecture. The synthesis and the vocabulary for agent coordination are the contribution.

We believe the right way to ship this is as an open protocol, with paid services and tools layered above — following the same pattern as Git itself, HTTP, Kafka, or React. The substrate belongs to everyone; value accrues to the builders who construct useful things on top.

### Licensing plan

Three layers, three licenses, each chosen to maximize the right kind of freedom:

| Layer | License | Why |
|---|---|---|
| **Protocol specification + fact schemas** | MIT or CC0 | Standards need to be maximally permissive. HTTP, RSS, ActivityPub, Git's own protocol all follow this. |
| **Agent skills + reducer rule libraries + reference middleware** | MIT or CC-BY | Shareable artifacts benefit from reusability. Agents and skills should flow between orgs with minimal friction. |
| **Production framework + hosted platform** | FSL or BSL, converting to Apache 2.0 after 2–4 years | Protects commercial surfaces from hyperscaler capture while keeping personal, internal, and non-competing commercial use free. Following the Terraform / HashiCorp / Sentry precedent. |

The reference prototypes in this repository are MIT. The production framework, when it exists, will be released under FSL (Functional Source License) or BSL (Business Source License) with an automatic conversion to Apache 2.0 at a defined change date.

### Specific artifacts we intend to publish openly

- The protocol specification (this paper, expanded over time)
- Reference node implementation in TypeScript
- Fact schema definitions (content-addressed, versioned)
- Reference reducer rules for common policies (RBAC, rate limiting, reputation)
- Integration guides for common log transports (Git, Convex, Postgres, Redis Streams, S3)

### Commercial surfaces we will build on top

- Hosted log-as-a-service with reliability guarantees
- The trust layer for governed AI tool access
- Visual editors, dashboards, and non-developer interfaces
- Skill and agent marketplaces
- Managed compliance reporting and audit tooling

---

## 9. Open problems and invitation

These are the questions we are actively working through and welcome collaboration on:

- **Skill schema interoperability.** Two parties' logs calling a skill `rollback-signup` may mean different things. Content-addressed skill definitions solve naming; semantic equivalence across domains is harder. Related work: RDF, schema.org, JSON-LD, OpenAPI.
- **Log truncation and snapshotting.** Long-running systems need to compact. A periodic `SnapshotAgreed{asOfLamport, stateHash}` fact may suffice, but concurrent appends that would have referenced older facts need handling.
- **Privacy and selective disclosure.** Some facts should be visible only to specific participants. Per-recipient encryption plus manifest facts is one approach; content-addressed commitments is another.
- **Economic primitives for marketplaces.** How to express pricing, slashing, collateral, dispute resolution in a small, well-typed fact vocabulary.
- **Failure modes and recovery.** What happens when an agent claims a dispatch and then disappears? Time-bounded claims? Forfeit facts? Re-claiming after timeout?

We are building this in public. The reference implementations, white paper revisions, and ongoing design discussion live at github.com/automate-friday/automate. Issues, pull requests, and adversarial critique are welcome.

---

## Appendix: Minimal example

A complete fact-log interaction illustrating propose → approve → claim → confirm (taken from the `fact-log-nano.ts` reference implementation output):

```
[L  1] bootstrap        📘 SkillRegistered rollback-signup (requires owner)
[L  2] bootstrap        📜 RoleAttested owner-dashboard has role 'owner'
[L  4] alice-laptop     🤝 AgentOffered alice-laptop (human) for [rollback-signup]
[L 13] detector-1       📮 DispatchProposed d13 skill=rollback-signup
[L 14] owner-dashboard  ✅ DispatchApproved d13
[L 15] alice-laptop     🙋 DispatchClaimed d13 by alice-laptop
[L 19] alice-laptop     🟢 DispatchConfirmed d13
```

Seven facts. Four participants (three online at dispatch time, one bootstrap). One complete unit of work, fully audited, replayable, and verifiable from the log alone. No RPC, no central coordinator, no shared mutable state.

---

*This paper is version 0.1. Feedback shapes v0.2. Author contact: github.com/automate-friday/automate.*
