# pi-matrix-relay

`pi-matrix-relay` connects Matrix rooms to live Pi coding-agent sessions. It is
Matrix-only, supports encrypted rooms from the start, and lets one Matrix bot
account serve multiple concurrent Pi sessions.

It consists of a broker and an extension.

The broker owns Matrix connectivity. In normal use it runs as a systemd user service.
During `pi-matrix-relay` development/debugging, do not use the systemd user service; stop it and run the broker manually from the JVM nREPL so broker state is inspectable with eval.

The extension runs in process with pi instances and connects to the broker over local UDS socket.

The broker is written in JVM Clojure. It uses donut.system for lifecycle
management, http-kit for the Ring web server, reitit for routing/middleware,
charred for JSON, ol.dirs for XDG paths, and trixnity-clj for Matrix.

The extension runs in process with Pi and is written in ClojureScript with
shadow-cljs.

## Development rules

- Always acknowledge Matrix/user messages before long development or research stretches so the operator knows the relay is alive and work has started.
- Do not ship best-effort placeholders, fake implementations, or degraded fallbacks as if they are complete. If a feature cannot be implemented properly with the available Pi/broker APIs, stop and raise the limitation for discussion before proceeding.

## Local dev

Run from the repo root:

- `bb lint` — lint broker and extension.
- `bb test` — test broker and extension.
- `bb qa` — run `lint`, then `test`.

For subproject tasks, see `broker/AGENTS.md` and `extension/AGENTS.md`.

## Resources

- trixnity-clj src: ~/src/github.com/outskirtslabs/trixnity-clj
- trixnity example usage: ~/src/github.com/ramblurr/thingstead/components/matrix/deps.edn
- ol.dirs src: ~/src/github.com/outskirtslabs/dirs
- reitit src: ~/src/github.com/metosin/reitit
- shadow-cljs src ~/src/github.com/thheller/shadow-cljs
- shadow-cljs docs ~/src/github.com/shadow-cljs/shadow-cljs.github.io/docs

See local-git-reference skill if you need to reference more codebases, do not munge in jars or caches.


## Dev workflow

Pre-reqs:

- read tmux skill
- read clojure-eval still

Start/Stop

- use tmux session name 'pi-matrix-relay', the first tab is the broker repl, started with `cd broker && bb dev` INSIDE the tmux session. ONLY start and stop the broker repl inside there.
- if this session doesnt exist, then create it with `tmuxb new pi-matrix-relay`
  use tmuxb list to see if it exists
- broker repl entrypoint is the `dev` ns: broker/dev/dev.clj
