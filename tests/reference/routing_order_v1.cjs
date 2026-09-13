// Independent Node reference for the v1 integer/HMAC order golden vector.
// Run explicitly with: node tests/reference/routing_order_v1.cjs
const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
const seed = Buffer.from('0123456789abcdef'.repeat(4), 'hex');

function field(value) {
  const bytes = Buffer.from(String(value), 'ascii');
  const length = Buffer.alloc(4);
  length.writeUInt32BE(bytes.length);
  return Buffer.concat([length, bytes]);
}

function block(level, priority, pick, counter) {
  const message = Buffer.concat([
    Buffer.from('gateway.routing-seed/v1/order', 'ascii'),
    ...[level, priority, pick, counter].map(field),
  ]);
  return createHmac('sha256', seed).update(message).digest('hex');
}

assert.equal(block('full', 0, 0, 0), 'adc35212dcfac7f6e66041c8427ff0f21510eec0824311d7d58956f87228d8a9');
assert.equal(block('reduced', 65535, 2, 1), '19b7f72d1d7142dc27778b32725854f3bf76228daf8e5a7fdef17051b860a3f3');

const remaining = [{ id: 'binding-c', weight: 3n }, { id: 'binding-a', weight: 1n }, { id: 'binding-b', weight: 7n }];
remaining.sort((a, b) => Buffer.compare(Buffer.from(a.id), Buffer.from(b.id)));
const order = [];
const space = 1n << 256n;
while (remaining.length) {
  const bound = remaining.reduce((sum, candidate) => sum + candidate.weight, 0n);
  const cutoff = space - space % bound;
  let value;
  for (let counter = 0; ; counter++) {
    value = BigInt('0x' + block('full', 0, order.length, counter));
    if (value < cutoff) break;
  }
  let draw = value % bound;
  const index = remaining.findIndex(candidate => {
    if (draw < candidate.weight) return true;
    draw -= candidate.weight;
    return false;
  });
  order.push(remaining.splice(index, 1)[0].id);
}
assert.deepEqual(order, ['binding-b', 'binding-c', 'binding-a']);
console.log('Routing order v1 Node golden vectors passed');
