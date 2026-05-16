const assert = require("node:assert/strict");

const extensionFactory = require("../dist/pi-matrix-relay.js");

assert.equal(typeof extensionFactory, "function", "compiled extension should export a Pi factory function");

const commands = new Map();
const pi = {
  registerCommand(name, options) {
    commands.set(name, options);
  },
};

extensionFactory(pi);

assert.deepEqual(Array.from(commands.keys()), ["hello"]);

const hello = commands.get("hello");
assert.equal(hello.description, "Say hello from the ClojureScript Pi extension");

const notifications = [];
hello.handler("Matrix", {
  ui: {
    notify(message, level) {
      notifications.push({ message, level });
    },
  },
});

assert.deepEqual(notifications, [
  { message: "Hello, Matrix, from ClojureScript!", level: "info" },
]);

console.log("hello extension behavior ok");
