import assert from "node:assert"
import test, {describe} from 'node:test';

await describe("Test Queue Only", () => {
    test('synchronous passing test', (t) => {
        // This test passes because it does not throw an exception.
        assert.strictEqual(1, 1);
    });

    test('synchronous failing test', (t) => {
        // This test fails because it throws an exception.
        assert.strictEqual(1, 2);
    });

    test('asynchronous passing test', async (t) => {
        // This test passes because the Promise returned by the async
        // function is settled and not rejected.
        assert.strictEqual(1, 1);
    });
})

