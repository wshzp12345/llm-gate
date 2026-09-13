// Independent canonical JSON/HMAC vector for the supported text subset.
const assert = require('node:assert/strict');
const { createHmac } = require('node:crypto');
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value !== null && typeof value === 'object') {
    return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  }
  return JSON.stringify(value);
}
const projection = {
  route: 'POST /v1/chat/completions',
  authorization_scope: {tenant_id: 'tenant', subject: 'subject', issuer: 'issuer', audience: 'audience', scopes: ['model.invoke', 'model.read']},
  model: 'general', messages: [{role: 'user', content: 'private text é'}],
  generation: {max_output_tokens: null, temperature: null, top_p: null},
  stream: false, include_usage: false, opaque_tools: null, output_schema: null,
  semantic_extensions: {}, caller_invocation_deadline: null,
};
const message = 'gateway.request-fingerprint/text-v1' + canonical(projection);
const digest = createHmac('sha256', Buffer.alloc(32, 0x6b)).update(message, 'utf8').digest('hex');
assert.equal(digest, '2300a9c2de65da5bd216eb25e662c8c9fa3501d440053bff625a1d57c08fc269');
console.log('Text Request Fingerprint v1 golden vector passed');
