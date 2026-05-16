const assert = require("node:assert/strict");

const extensionFactory = require("../dist/pi-matrix-relay.js");

assert.equal(typeof extensionFactory, "function", "compiled extension should export a Pi factory function");

const commands = new Map();
const tools = new Map();
const pi = {
  registerCommand(name, options) {
    commands.set(name, options);
  },
  registerTool(tool) {
    tools.set(tool.name, tool);
  },
};

extensionFactory(pi);

assert.deepEqual(Array.from(commands.keys()).sort(), ["matrix-relay", "mr"]);
assert.deepEqual(Array.from(tools.keys()).sort(), ["send_matrix_message", "send_matrix_reaction"]);

for (const name of ["matrix-relay", "mr"]) {
  const command = commands.get(name);
  assert.equal(command.description, "Control the Pi Matrix relay");
  assert.equal(typeof command.handler, "function");
}

const tool = tools.get("send_matrix_message");
assert.equal(tool.label, "Send Matrix Message");
assert.equal(typeof tool.execute, "function");

const reactionTool = tools.get("send_matrix_reaction");
assert.equal(reactionTool.label, "Send Matrix Reaction");
assert.equal(typeof reactionTool.execute, "function");

console.log("matrix relay extension behavior ok");
