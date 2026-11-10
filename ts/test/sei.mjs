// The timing SEI as the relay writes it (§39.4), read as the console does:
// internal/relay/testdata/timing-au.hex is kept current by the relay's
// TestTimingSEIGolden, and holds an H.264 access unit with one whose fields
// need emulation prevention bytes.
//
//   node test/sei.mjs
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { readTiming } from '../src/sei.ts'

const hex = readFileSync(new URL('../../internal/relay/testdata/timing-au.hex', import.meta.url), 'utf8').trim()
const au = Uint8Array.from(hex.match(/../g).map((b) => parseInt(b, 16)))

const t0 = 1_800_000_000_123.456
const got = readTiming(au)
assert.ok(got, 'the SEI is found')
assert.equal(got.media, 3033.366)
assert.equal(got.t1, t0)
assert.equal(got.t2, t0 + 2)
assert.equal(got.t3, t0 + 140)
assert.equal(got.t4, t0 + 390)

// An access unit without one, and one in H.265 framing that is not SEI.
assert.equal(readTiming(Uint8Array.from([0, 0, 0, 1, 0x67, 0x42, 0, 0, 0, 1, 0x65, 0x88])), undefined)
assert.equal(readTiming(Uint8Array.from([0, 0, 0, 1, 0x26, 0x01, 0xaa])), undefined)

console.log('sei: ok')
