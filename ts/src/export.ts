/**
 * Export: a range of a camera's recordings as one MP4 (§40). The laminae of
 * a range are fragmented MP4 whose decode times are their encoder session's
 * own: a session that began again begins again at zero. Played, each lamina
 * is placed on the wall clock by `timestampOffset`; exported, the placement
 * has to be in the file, so every fragment's `tfdt` is moved to where its
 * lamina sits on the wall clock and the result has one init segment.
 *
 * A lamina that goes on from the one before it in the same session keeps
 * the session's own decode times, which are exact, rather than its
 * `date_started`, which is not. Anything else is placed by the wall clock,
 * and never before what came before it. A gap stays a gap: the next
 * fragment's `tfdt` is where the recording begins again, which costs no
 * bytes, and a player that reads `tfdt` shows it as one.
 *
 * Laminae of another track configuration (the encoder changed: another
 * profile, size or codec) cannot share one init segment; joining them is a
 * remux this does not do, so the export stops there and says where.
 *
 * @module
 */

import { boxes, concat, find, type Box, type Bytes } from './fmp4.js'

/** Piece is one lamina's part of an export. */
export interface Piece {
	/** The lamina's init segment. */
	init: Uint8Array
	/**
	 * Its fragments, whole `moof`s and `mdat`s from a keyframe on. Their
	 * `tfdt`s and `mfhd`s are rewritten in place.
	 */
	fragments: Uint8Array
	/** The wall clock, in ms, at decode time zero of the lamina's session. */
	zero: number
}

/** Changed is a piece whose track configuration is not the first piece's. */
export class Changed extends Error {
	constructor(readonly piece: number) {
		super('the encoder changed within the range')
	}
}

/**
 * How far a piece's wall clock placement may be from its predecessor's
 * session, in seconds, for the piece still to be taken as that session going
 * on: `date_started` is when the first fragment arrived, not when it began.
 */
const slack = 2

/** 1904-01-01, where an MP4's clock begins, in Unix seconds. */
const epoch1904 = 2082844800

/**
 * join is the pieces of a range as one MP4: the first piece's init segment,
 * then every piece's fragments placed on one timeline that begins at the
 * first piece's first sample. `start` is that sample's wall clock time, in
 * ms, which is also the file's `mvhd` creation time.
 */
export function join(pieces: Piece[]): { bytes: Bytes; start: number } {
	const head = pieces[0]
	if (head === undefined) throw new Error('nothing to export')
	const want = tracks(head.init)
	if (want === undefined) throw new Error('no init segment')

	// Per track: what is added to a decode time, and where the output so far ends.
	let shift = new Map<number, number>()
	const end = new Map<number, number>()
	// The current session's placement, in seconds: output time = session time + place.
	let place: number | undefined
	let origin = head.zero
	let seq = 0
	const out: Uint8Array[] = []
	for (const [i, p] of pieces.entries()) {
		const have = tracks(p.init)
		if (have === undefined || !compatible(want, have)) throw new Changed(i)
		const frags = scan(p.fragments, have)
		if (frags.length === 0) continue
		const first = new Map<number, number>()
		for (const f of frags) for (const t of f.trafs) if (!first.has(t.track)) first.set(t.track, t.time)
		const seconds = (track: number, ticks: number): number => ticks / have.get(track)!.timescale

		if (place === undefined) {
			// The output begins at the earliest first sample of any track.
			const lead = Math.min(...[...first].map(([track, t]) => seconds(track, t)))
			origin = p.zero + lead * 1000
			place = -lead
			shift = shifts(have, first, end, place)
		} else {
			const wall = (p.zero - origin) / 1000
			const goesOn = Math.abs(wall - place) <= slack && [...first].every(([track, t]) => !shift.has(track) || t + shift.get(track)! >= (end.get(track) ?? 0))
			if (!goesOn) {
				place = wall
				shift = shifts(have, first, end, place)
			} else {
				// A track the session had not sent yet joins at the session's placement.
				for (const [track, s] of shifts(have, first, end, place)) if (!shift.has(track)) shift.set(track, s)
			}
		}

		for (const f of frags) {
			setUint32(p.fragments, f.mfhd, ++seq)
			for (const t of f.trafs) {
				const s = shift.get(t.track)
				if (s === undefined) throw new Changed(i)
				const time = t.time + s
				writeTime(p.fragments, t.tfdt, time)
				end.set(t.track, Math.max(end.get(t.track) ?? 0, time + t.duration))
			}
		}
		out.push(p.fragments)
	}
	if (out.length === 0) throw new Error('nothing to export')

	return { bytes: concat([header(head.init, origin, end), ...out]), start: origin }
}

/**
 * shifts places a session's first samples at `place` seconds of the output,
 * each track no earlier than where it ends so far.
 */
function shifts(have: Map<number, Track>, first: Map<number, number>, end: Map<number, number>, place: number): Map<number, number> {
	const out = new Map<number, number>()
	for (const [track, t] of first) {
		const scale = have.get(track)!.timescale
		out.set(track, Math.max(Math.round(place * scale), (end.get(track) ?? 0) - t))
	}

	return out
}

/** Track is what an init segment says of one track. */
interface Track {
	timescale: number
	/** `trex`'s default sample duration. */
	duration: number
	/** What has to be the same for fragments to share the init segment. */
	config: Uint8Array[]
}

/**
 * tracks reads the tracks of an init segment by ID: their timescales, and
 * the boxes their fragments are read by (the handler, the sample entries,
 * the `trex` defaults).
 */
function tracks(init: Uint8Array): Map<number, Track> | undefined {
	const moov = find(init, 0, init.length, 'moov')
	if (moov === undefined) return undefined
	const v = view(init)
	const trex = new Map<number, Box>()
	const mvex = find(init, moov.off + moov.hdr, moov.end, 'mvex')
	if (mvex !== undefined) {
		for (const x of boxes(init, mvex.off + mvex.hdr, mvex.end)) if (x.type === 'trex') trex.set(v.getUint32(x.off + x.hdr + 4), x)
	}
	const out = new Map<number, Track>()
	for (const trak of boxes(init, moov.off + moov.hdr, moov.end)) {
		if (trak.type !== 'trak') continue
		const tkhd = find(init, trak.off + trak.hdr, trak.end, 'tkhd')
		const mdia = find(init, trak.off + trak.hdr, trak.end, 'mdia')
		if (tkhd === undefined || mdia === undefined) return undefined
		const mdhd = find(init, mdia.off + mdia.hdr, mdia.end, 'mdhd')
		const hdlr = find(init, mdia.off + mdia.hdr, mdia.end, 'hdlr')
		const minf = find(init, mdia.off + mdia.hdr, mdia.end, 'minf')
		const stbl = minf === undefined ? undefined : find(init, minf.off + minf.hdr, minf.end, 'stbl')
		const stsd = stbl === undefined ? undefined : find(init, stbl.off + stbl.hdr, stbl.end, 'stsd')
		if (mdhd === undefined || hdlr === undefined || stsd === undefined) return undefined
		const id = v.getUint32(tkhd.off + tkhd.hdr + (init[tkhd.off + tkhd.hdr] === 1 ? 20 : 12))
		const timescale = v.getUint32(mdhd.off + mdhd.hdr + (init[mdhd.off + mdhd.hdr] === 1 ? 20 : 12))
		const x = trex.get(id)
		out.set(id, {
			timescale,
			duration: x === undefined ? 0 : v.getUint32(x.off + x.hdr + 12),
			config: [
				init.subarray(hdlr.off + hdlr.hdr + 8, hdlr.off + hdlr.hdr + 12),
				init.subarray(stsd.off, stsd.end),
				x === undefined ? new Uint8Array() : init.subarray(x.off, x.end),
			],
		})
	}

	return out
}

/**
 * joins is whether fragments under init segment `b` can follow those under
 * `a` in one file, which is known before any fragment is fetched.
 */
export function joins(a: Uint8Array, b: Uint8Array): boolean {
	const x = tracks(a)
	const y = tracks(b)

	return x !== undefined && y !== undefined && compatible(x, y)
}

/** compatible is whether fragments of `b` read the same under the init segment of `a`. */
function compatible(a: Map<number, Track>, b: Map<number, Track>): boolean {
	if (a.size !== b.size) return false
	for (const [id, x] of a) {
		const y = b.get(id)
		if (y === undefined || x.timescale !== y.timescale) return false
		if (!x.config.every((c, i) => same(c, y.config[i]!))) return false
	}

	return true
}

/** Fragment is where a `moof`'s numbers are, and each `traf`'s time. */
interface Fragment {
	/** Where `mfhd`'s sequence number is. */
	mfhd: number
	trafs: {
		track: number
		/** The `tfdt` box, its decode time, and the duration of its samples. */
		tfdt: Box
		time: number
		duration: number
	}[]
}

/** scan reads every `moof` of a run of fragments. */
function scan(b: Uint8Array, have: Map<number, Track>): Fragment[] {
	const v = view(b)
	const out: Fragment[] = []
	for (const moof of boxes(b)) {
		if (moof.type !== 'moof') continue
		const mfhd = find(b, moof.off + moof.hdr, moof.end, 'mfhd')
		if (mfhd === undefined) throw new Error('a fragment without its mfhd')
		const f: Fragment = { mfhd: mfhd.off + mfhd.hdr + 4, trafs: [] }
		for (const traf of boxes(b, moof.off + moof.hdr, moof.end)) {
			if (traf.type !== 'traf') continue
			const tfhd = find(b, traf.off + traf.hdr, traf.end, 'tfhd')
			const tfdt = find(b, traf.off + traf.hdr, traf.end, 'tfdt')
			if (tfhd === undefined || tfdt === undefined) throw new Error('a fragment without its tfhd or tfdt')
			const p = tfhd.off + tfhd.hdr
			const flags = v.getUint32(p) & 0xffffff
			const track = v.getUint32(p + 4)
			if (!have.has(track)) throw new Error('a fragment of a track its init segment does not have')
			// Data offsets from anywhere but the moof would point elsewhere once moved.
			if (flags & 0x01) throw new Error('a fragment with an absolute data offset')
			let q = p + 8
			if (flags & 0x02) q += 4
			const fallback = flags & 0x08 ? v.getUint32(q) : (have.get(track)?.duration ?? 0)
			let duration = 0
			for (const trun of boxes(b, traf.off + traf.hdr, traf.end)) {
				if (trun.type !== 'trun') continue
				const r = trun.off + trun.hdr
				const tf = v.getUint32(r) & 0xffffff
				const n = v.getUint32(r + 4)
				let s = r + 8 + (tf & 0x01 ? 4 : 0) + (tf & 0x04 ? 4 : 0)
				if (!(tf & 0x100)) {
					duration += n * fallback
					continue
				}
				const step = 4 * (1 + (tf & 0x200 ? 1 : 0) + (tf & 0x400 ? 1 : 0) + (tf & 0x800 ? 1 : 0))
				for (let k = 0; k < n; k++, s += step) duration += v.getUint32(s)
			}
			f.trafs.push({ track, tfdt, time: readTime(b, tfdt), duration })
		}
		out.push(f)
	}

	return out
}

function readTime(b: Uint8Array, tfdt: Box): number {
	const p = tfdt.off + tfdt.hdr
	const v = view(b)

	return b[p] === 1 ? Number(v.getBigUint64(p + 4)) : v.getUint32(p + 4)
}

function writeTime(b: Uint8Array, tfdt: Box, time: number): void {
	const p = tfdt.off + tfdt.hdr
	const v = view(b)
	if (b[p] === 1) {
		v.setBigUint64(p + 4, BigInt(time))
		return
	}
	// A 32-bit tfdt holds 13 hours at 90 kHz; growing the box would move the samples.
	if (time > 0xffffffff) throw new Error('the range is too long for this recording to export in one file')
	v.setUint32(p + 4, time)
}

/**
 * header is the init segment of the export. `mvhd`'s creation time is the
 * wall clock time of the first sample, in ms. An `empty_moov` says nothing
 * of how long the fragments are, and a player that guesses from the first
 * few guesses short, so the file says: `mehd`, and the durations of the
 * movie and of each track (`end`, in its ticks), which a browser reads.
 */
function header(src: Uint8Array, at: number, end: Map<number, number>): Uint8Array {
	const init = src.slice()
	const v = view(init)
	const moov = find(init, 0, init.length, 'moov')
	const mvhd = moov === undefined ? undefined : find(init, moov.off + moov.hdr, moov.end, 'mvhd')
	if (moov === undefined || mvhd === undefined) return init
	/** put writes a field that is 64 bits in a version 1 box and 32 in a version 0. */
	const put = (box: Box, v0: number, v1: number, n: number): void => {
		const q = box.off + box.hdr
		if (init[q] === 1) v.setBigUint64(q + v1, BigInt(n))
		else v.setUint32(q + v0, Math.min(n, 0xffffffff))
	}
	const p = mvhd.off + mvhd.hdr
	const timescale = v.getUint32(p + (init[p] === 1 ? 20 : 12))
	put(mvhd, 4, 4, Math.floor(at / 1000) + epoch1904)
	let duration = 0
	for (const trak of boxes(init, moov.off + moov.hdr, moov.end)) {
		if (trak.type !== 'trak') continue
		const tkhd = find(init, trak.off + trak.hdr, trak.end, 'tkhd')
		const mdia = find(init, trak.off + trak.hdr, trak.end, 'mdia')
		const mdhd = mdia === undefined ? undefined : find(init, mdia.off + mdia.hdr, mdia.end, 'mdhd')
		if (tkhd === undefined || mdhd === undefined) continue
		const id = v.getUint32(tkhd.off + tkhd.hdr + (init[tkhd.off + tkhd.hdr] === 1 ? 20 : 12))
		const q = mdhd.off + mdhd.hdr
		const scale = v.getUint32(q + (init[q] === 1 ? 20 : 12))
		const ticks = end.get(id) ?? 0
		const movie = Math.round((ticks / scale) * timescale)
		put(tkhd, 20, 28, movie)
		put(mdhd, 16, 24, ticks)
		duration = Math.max(duration, movie)
	}
	put(mvhd, 16, 24, duration)

	const mvex = find(init, moov.off + moov.hdr, moov.end, 'mvex')
	if (mvex === undefined) return init
	const mehd = find(init, mvex.off + mvex.hdr, mvex.end, 'mehd')
	if (mehd !== undefined) {
		const q = mehd.off + mehd.hdr
		if (init[q] === 1) v.setBigUint64(q + 4, BigInt(duration))
		else v.setUint32(q + 4, Math.min(duration, 0xffffffff))
		return init
	}
	// A version 1 `mehd` first in `mvex`; the boxes around it grow by its size.
	const box = new Uint8Array(20)
	const w = view(box)
	w.setUint32(0, 20)
	box.set([0x6d, 0x65, 0x68, 0x64, 1], 4)
	w.setBigUint64(12, BigInt(duration))
	for (const x of [moov, mvex]) {
		if (x.hdr === 16) v.setBigUint64(x.off + 8, BigInt(x.end - x.off + 20))
		else v.setUint32(x.off, x.end - x.off + 20)
	}
	const cut = mvex.off + mvex.hdr

	return concat([init.subarray(0, cut), box, init.subarray(cut)])
}

function setUint32(b: Uint8Array, at: number, n: number): void {
	view(b).setUint32(at, n)
}

function view(b: Uint8Array): DataView {
	return new DataView(b.buffer, b.byteOffset, b.byteLength)
}

function same(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false

	return true
}
