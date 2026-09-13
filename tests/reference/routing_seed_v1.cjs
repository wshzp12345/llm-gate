// Independent raw-key HMAC seed vector. Node is not a Gateway dependency.
const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
const values = ['12345678-1234-4234-9234-123456789abc', 'routing.primary', '9223372036854775807'];
const fields = values.map(value => {
  const bytes = Buffer.from(value, 'utf8');
  const size = Buffer.alloc(4);
  size.writeUInt32BE(bytes.length);
  return Buffer.concat([size, bytes]);
});
const message = Buffer.concat([Buffer.from('gateway.routing-seed/v1', 'ascii'), ...fields]);
const rawKey = Buffer.from(Array.from({length: 32}, (_, index) => index));
const seed = createHmac('sha256', rawKey).update(message).digest('hex');
assert.equal(seed, 'baa5364da88093b83a64f41f324fe1ef9683c0e9859d10ada585a3c494716e90');
console.log('Routing seed v1 golden vector passed');
