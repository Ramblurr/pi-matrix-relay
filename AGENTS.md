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
- At this early stage: absolutely no backwards compatibility. If possible to an inplace migration of state, but if it is too much work just blow it away and start fresh.

## Local dev

Run from the repo root:

- `bb lint` — lint broker and extension.
- `bb test` — test broker and extension.
- `bb qa` — run `lint`, then `test`.

### Local extension install for live Pi/Matrix testing

Use exactly one project-local relay extension package directory during development. The install task writes a cache-busted bundle inside it like:

```text
.pi/extensions/pi-matrix-relay/pi-matrix-relay-<epoch-ms>.js
```

Install or replace it only with:

```bash
cd extension && bb install-local-extension
```

That task must remove any existing `.pi/extensions/pi-matrix-relay*` files or directories before copying the new package directory. The package `package.json` must point at exactly one cache-busted bundle via `pi.extensions`. Do not leave multiple timestamped, hashed, or otherwise unique relay bundle names discoverable in `.pi/extensions/`; Pi loads every discovered extension entry, so duplicates start duplicate relay clients and can lease multiple Matrix slots.

When adding npm dependencies to the extension, they must be bundled into the copied extension file. Pi loads `.pi/extensions/pi-matrix-relay/package.json`, then the package's cache-busted bundle, and does not have `extension/node_modules` beside it. The app build uses shadow-cljs `:js-provider :shadow` with `:keep-native-requires true` so npm deps are bundled while Node built-ins stay native. After adding a dependency, run `cd extension && bb shadow-release` and verify the released bundle does not contain external `require("<dep>")` for the new package. Do not fix missing runtime deps by copying `node_modules` into `.pi/extensions/`.

Before live testing after extension work:

1. Run `cd extension && bb install-local-extension`.
2. Ensure `.pi/extensions/` contains only one `pi-matrix-relay` package directory for this relay and no stale flat `pi-matrix-relay*.js` files.
3. Reload Pi (`/reload` or `reload_runtime`).
4. Run `matrix_relay_diagnostics` and verify this Pi process has exactly one active relay client/slot before testing Matrix behavior.

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
- read clojure-eval skill

Start/Stop

- use tmux session name 'pi-matrix-relay', the first tab is the broker repl, started with `cd broker && bb dev` INSIDE the tmux session. ONLY start and stop the broker repl inside there.
- if this session doesnt exist, then create it with `tmuxb new pi-matrix-relay`
  use tmuxb list to see if it exists
- broker repl entrypoint is the `dev` ns: broker/dev/dev.clj

Live Clojure evaluation

- Before evaluating Clojure, read and follow the clojure-eval skill.
- Use the project `brepl` workflow with heredocs for live broker evaluation, for example:
  ```bash
  cd broker
  brepl "$(cat <<'EOF'
  (require '[dev :as dev])
  ;; inspect or mutate the running broker JVM state here
  EOF
  )"
  ```
- Do not hand-roll nREPL/bencode clients, ad-hoc socket clients, or Python wrappers for REPL evaluation.
- Do not use a fresh `clojure -e` process when debugging live broker/Matrix state; it will not share the running broker's Matrix client, sync loop, encryption store, or in-memory state.
