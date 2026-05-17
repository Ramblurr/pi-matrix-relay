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

## Clojure and broker resources

- trixnity-clj src: ~/src/github.com/outskirtslabs/trixnity-clj
- trixnity example usage: ~/src/github.com/ramblurr/thingstead/components/matrix/deps.edn
- ol.dirs src: ~/src/github.com/outskirtslabs/dirs
- reitit src: ~/src/github.com/metosin/reitit
- Broker commands:
  - `cd broker && bb dev` starts the JVM nREPL from the broker directory.
  - `cd broker && bb test` runs JVM Clojure tests.

## Clojurescript and shadow-cljs resources

- shadow-cljs src ~/src/github.com/thheller/shadow-cljs
- shadow-cljs docs ~/src/github.com/shadow-cljs/shadow-cljs.github.io/docs
