# Automate Friday — Fact-Log Protocol for Agent Coordination

> An open protocol for letting humans, AI agents, and deterministic scripts collaborate on sequences of skills across machines and organizations — without any party imperatively commanding another.

**Status:** Pre-release. This repository contains the v0.1 white paper and two reference prototypes used to clarify the protocol's vocabulary. The full framework is not yet implemented. We are publishing early to gather feedback and establish an open-source authorship timestamp.

**Author:** Jacob Haugen
**First published:** 2026-04-19

## Companion repositories

- **[automate-friday/protocol](https://github.com/automate-friday/protocol)** (this repo, MIT) — protocol specification, fact schemas, white paper, minimal reference prototypes.
- **[automate-friday/automate](https://github.com/automate-friday/automate)** (FSL-1.1) — the framework: DSL, runtime, CLI, and reference agents. Converts to Apache 2.0 after 2 years.

The protocol is where the standard lives. The framework is where the implementation lives. Both are open; they differ in licensing terms for competing-SaaS clauses.

---

## What this is

A minimal coordination protocol built on one primitive — a **signed, append-only fact log with domain-aware projection**. Every coordination action (a skill being declared, an agent offering capability, a unit of work being dispatched, an approval being granted, a job being claimed or confirmed) is a signed fact appended to the log. Every party derives system state by projecting the log through a deterministic reducer.

The mental model is **Git for agent coordination**. Where Git lets distributed parties collaborate on source code, this protocol lets distributed parties collaborate on behavior in the world.

## Start here

- **[WHITE-PAPER.md](WHITE-PAPER.md)** — the v0.1 paper (~10 min read). Motivation, primitives, architecture, Git mental model, what this unlocks, open problems.
- **[prototypes/engine-nano.ts](prototypes/engine-nano.ts)** — single-runtime reference. Useful for understanding the vocabulary (Skill, Agent, Engine, Middleware, Role, Dispatch, Tick Ledger) but does not survive distribution. Documented in-file.
- **[prototypes/fact-log-nano.ts](prototypes/fact-log-nano.ts)** — log-first reference. All primitives reduce to `append(fact)` and `project(log)`. Four nodes collaborate through one in-memory log. Swap the log implementation for a WebSocket stream, a Convex table, or a Git repository and the same code distributes across machines.

## Running the prototypes

Both are single TypeScript files with no dependencies beyond [Bun](https://bun.sh). Clone the repo and:

```bash
bun prototypes/engine-nano.ts      # ~300 LOC — single-runtime reference
bun prototypes/fact-log-nano.ts    # ~300 LOC — log-based reference
```

Each prints a tape of what happens tick by tick. The log-based prototype prints every fact as it lands so you can read the collaboration.

## Where we are

Honest state:

- **Done:** Vocabulary is clarified. Two prototypes demonstrate the primitives. The Git analogy has been stress-tested and holds.
- **In progress:** Trust-layer specification, skill schema interoperability, log-transport integrations (Git, Convex, Postgres).
- **Not started:** Production reference implementation, cryptographic signature integration, replication transport, reliability layer, ergonomic DSL.
- **Intentionally deferred:** Visual editors, marketplaces, hosted services — these are product-layer concerns that belong above the protocol.

Treat the prototypes as sketches, not reference implementations. They exist to make the protocol's shape concrete, not to be depended on.

## Why open source

The primitive itself — signed append-only logs with reducer-driven projection — is not novel in isolation. It draws on decades of work in distributed systems (event sourcing, CRDTs), Promise Theory (Mark Burgess), capability-based security, and Git's architecture. The contribution is the synthesis, the vocabulary for agent coordination, and the deliberate packaging.

We believe the right way to ship this is as an open protocol, with paid services and tools layered above — the same pattern as Git itself, HTTP, Kafka, or React. The substrate belongs to everyone; value accrues to the builders who construct useful things on top.

See the white paper's section on [Open source intent](WHITE-PAPER.md#8-open-source-intent) for the detailed split between what will always be open (protocol spec, reference implementation, fact schemas, reducer rules, transport integrations) and what the author's company will build commercially on top (hosted log-as-a-service, trust layer, visual editors, marketplaces).

## Feedback

- Issues and pull requests on this repository are welcome
- Adversarial critique is especially welcome — the v0.1 paper is meant to be challenged
- Specific open problems are listed [in the paper](WHITE-PAPER.md#9-open-problems-and-invitation)

## License

MIT — see [LICENSE](LICENSE). The protocol is free to use, implement, and build on.

---

*This is v0.1. The paper, the prototypes, and this README will all change as conversation sharpens the ideas. Your reading of an early draft is appreciated.*
