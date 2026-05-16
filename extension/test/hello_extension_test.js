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

assert.deepEqual(Array.from(commands.keys()).sort(), ["matrix-relay", "mr"]);

for (const name of ["matrix-relay", "mr"]) {
  const command = commands.get(name);
  assert.equal(command.description, "Control the Pi Matrix relay");
  assert.equal(typeof command.handler, "function");
}

console.log("matrix relay extension behavior ok");
