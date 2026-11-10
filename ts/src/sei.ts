/**
 * The timing SEI the relay puts in a timed session's video (§39.4), read
 * from an encoded frame. Apart from `timing.ts` so the worker that reads it
 * imports nothing that starts a worker.
 *
 * @module
 */

/** What a timing SEI says, in Unix milliseconds; 0 where unknown. */
export interface FrameTiming {
	/** The frame's decode time on the stream's clock, in milliseconds: its name. */
	media: number
	t1: number
	t2: number
	t3: number
	t4: number
}

/** The SEI's mark among others an encoder may write. */
const uuid = [0x8f, 0x0a, 0x5b, 0x63, 0x4e, 0x1c, 0x4d, 0x2f, 0x9a, 0x6e, 0x53, 0x68, 0x61, 0x6c, 0x65, 0x54]
const version = 1
const size = 16 + 1 + 5 * 8

/** The NAL units a timing SEI is looked for among: it comes before the first slice. */
const lookAhead = 8

/**
 * readTiming finds a timing SEI in an access unit in Annex B, of H.264 or
 * H.265 alike: the two SEI headers do not collide with each other.
 */
export function readTiming(au: Uint8Array): FrameTiming | undefined {
	let seen = 0
	for (let i = 0; i + 3 < au.length && seen < lookAhead; ) {
		if (au[i] !== 0 || au[i + 1] !== 0 || au[i + 2] !== 1) {
			i++
			continue
		}
		const u = i + 3
		seen++
		let end = u
		while (end + 2 < au.length && !(au[end] === 0 && au[end + 1] === 0 && (au[end + 2] === 1 || au[end + 2] === 0))) end++
		if (end + 2 >= au.length) end = au.length
		const h = au[u] ?? 0
		const got = (h & 0x1f) === 6 ? sei(au.subarray(u + 1, end)) : ((h >> 1) & 0x3f) === 39 ? sei(au.subarray(u + 2, end)) : undefined
		if (got !== undefined) return got
		i = end
	}

	return undefined
}

/** sei reads one SEI NAL unit's payload, after its header, for Shale's message. */
function sei(escaped: Uint8Array): FrameTiming | undefined {
	const b = unescape(escaped)
	if (b.length < 2 || b[0] !== 5) return undefined
	let n = 0
	let p = 1
	while (p < b.length && b[p] === 0xff) {
		n += 255
		p++
	}
	n += b[p] ?? 0
	p++
	if (n !== size || b.length < p + size) return undefined
	for (let k = 0; k < 16; k++) if (b[p + k] !== uuid[k]) return undefined
	if (b[p + 16] !== version) return undefined
	const v = new DataView(b.buffer, b.byteOffset + p + 17, 5 * 8)
	// Nanoseconds to milliseconds, through microseconds, which a double holds.
	const at = (k: number): number => Number(v.getBigInt64(8 * k) / 1000n) / 1000

	return { media: at(0), t1: at(1), t2: at(2), t3: at(3), t4: at(4) }
}

/** unescape takes out the emulation prevention bytes. */
function unescape(b: Uint8Array): Uint8Array {
	const out = new Uint8Array(b.length)
	let n = 0
	let zeros = 0
	for (const c of b) {
		if (zeros >= 2 && c === 3) {
			zeros = 0
			continue
		}
		out[n++] = c
		zeros = c === 0 ? zeros + 1 : 0
	}

	return out.subarray(0, n)
}

/** What the worker says of each timed frame. */
export interface Timed extends FrameTiming {
	/** The session's number on this page. */
	id: number
	rtp: number
}

