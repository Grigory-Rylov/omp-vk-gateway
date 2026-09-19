# AGENTS.md

Operational rules for AI agents working in this repo. These are hard constraints, not suggestions.

## NEVER restart processes yourself

The gateway and its coprocesses are kept alive by external restarter processes. **You must not start, stop, kill, restart, or otherwise manage any of these processes:**

- `omp-agent` — the VK gateway (the main binary in this repo)
- `omp-agent-restarter` — Go restarter binary that respawns the gateway on `.agent-restart`
- `agent-restarter` — companion restarter process
- `gateway-restarter.py` — Python restarter
- The `omp` coprocess children spawned by the gateway (the oh-my-pi RPC processes)

**If the gateway or any coprocess needs to be restarted, do not do it. Ask the user to do it.** The user controls the restarter processes; an agent restart can race the restarter, kill in-flight work, or leave a half-recovered state.

The only supported gateway respawn mechanism is `touch .agent-restart`, which the restarter consumes. Even that — **do not trigger it yourself without the user's explicit go-ahead.** It kills the live coprocess and drops any in-flight `@agent` runs (they are re-issued on the next start, but a running turn is lost).

## Other hard constraints

- **Never run `omp agents unpack`** — it can clobber agent definitions.
- **Do not modify `config.json`** (tokens, agent command, agent names, subscription).
- **`debug/gateway.log`** is a large append-only log. Never read it in full. Grep a narrow time window, e.g. `grep -a "2026-09-18T09:2" debug/gateway.log`.
- **Do not run the full oh-my-pi test suite** from this repo. Verify this repo's own packages (`go test ./pkg/...`) only.

## Code style

- **Prefer self-documenting names over comments.** Name methods, types, and variables so their intent is obvious from the name; do not add a comment that merely restates what the code already says. Add a comment only for the non-obvious *why* — a constraint, a workaround, a subtle invariant — that the code cannot express on its own.

## VK message delivery

`messages.send` requires an integer `random_id`. `messages.get` does not work with this token — verify delivery via `debug/gateway.log`, not the VK API.

