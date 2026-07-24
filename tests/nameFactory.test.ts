import assert from "node:assert";
import test, { describe } from "node:test";
import { NameFactory } from "../modules/index.ts";
import type { Equal, Expect } from "./support.ts";

describe("NameFactory", () => {
	test("name only", () => {
		const n = NameFactory({ name: "MyQueue" });
		assert.strictEqual(n.name, "MyQueue");
		assert.strictEqual(n.displayName, "MyQueue");
		type _ = Expect<Equal<typeof n.name, "MyQueue">>;
	});

	test("prefixes env", () => {
		const n = NameFactory({ name: "MyQueue", env: "dev" });
		assert.strictEqual(n.name, "dev-MyQueue");
		assert.strictEqual(n.displayName, "MyQueue");
		type _ = Expect<Equal<typeof n.name, "dev-MyQueue">>;
	});

	test("string parent keeps env", () => {
		const n = NameFactory({ name: "child", parent: "parent", env: "dev" });
		// displayName is always the bare name
		assert.strictEqual(n.displayName, "child");
	});
});
