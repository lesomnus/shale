/**
 * Enough of fragmented MP4 to play a lamina from the middle (§23.1, §40):
 * the boxes of an init segment (the video track's timescale), the index a
 * lamina ends with (`mfro`, then `mfra`'s `tfra`: every keyframe's decode
 * time and byte offset), and a fragment's decode time (`tfdt`). Nothing is
 * decoded here; the browser's Media Source Extensions do that.
 *
 * @module
 */

/** Bytes is what the browser takes: a view on an ArrayBuffer of its own. */
export type Bytes = Uint8Array<ArrayBuffer>

export interface Box {
	type: string
	/** Where the box begins, where its payload begins, and where it ends. */
	off: number
	hdr: number
	end: number
}

/** boxes walks the boxes of a byte range. */
export function boxes(b: Uint8Array, off = 0, end = b.length): Box[] {
	const out: Box[] = []
	const v = new DataView(b.buffer, b.byteOffset, b.byteLength)
	while (off + 8 <= end) {
		let size = v.getUint32(off)
		const type = String.fromCharCode(b[off + 4]!, b[off + 5]!, b[off + 6]!, b[off + 7]!)
		let hdr = 8
		if (size === 1) {
			if (off + 16 > end) break
			size = Number(v.getBigUint64(off + 8))
			hdr = 16
		}
		if (size === 0) size = end - off
		if (size < hdr || off + size > end) break
		out.push({ type, off, hdr, end: off + size })
		off += size
	}

	return out
}

/** find is the first child of a type inside a range, or undefined. */
export function find(b: Uint8Array, off: number, end: number, type: string): Box | undefined {
	return boxes(b, off, end).find((x) => x.type === type)
}

/** Init is what an init segment says that a player needs. */
export interface Init {
	/** The init segment's bytes: `ftyp` and `moov`. */
	bytes: Bytes
	/** The video track's ID and timescale, ticks per second. */
	track: number
	timescale: number
	/**
	 * The codecs of every track, as a MIME `codecs` parameter: what a
	 * SourceBuffer is made for. A browser refuses an init segment that lacks
	 * a track its SourceBuffer was promised, so it is what the segment holds.
	 */
	codecs: string
}

/**
 * parseInit reads the init segment at the start of a lamina: its length is
 * where the first `moof` begins, the video track's timescale is in its
 * `mdhd`, and each track's codec in its `stsd`.
 */
export function parseInit(b: Bytes): Init | undefined {
	const v = new DataView(b.buffer, b.byteOffset, b.byteLength)
	let end = 0
	let moov: Box | undefined
	for (const x of boxes(b)) {
		if (x.type === 'moof') break
		if (x.type === 'moov') moov = x
		end = x.end
	}
	if (moov === undefined) return undefined
	const codecs: string[] = []
	for (const trak of boxes(b, moov.off + moov.hdr, moov.end)) {
		if (trak.type !== 'trak') continue
		const c = codecOf(b, trak)
		if (c !== undefined && !codecs.includes(c)) codecs.push(c)
	}
	for (const trak of boxes(b, moov.off + moov.hdr, moov.end)) {
		if (trak.type !== 'trak') continue
		const tkhd = find(b, trak.off + trak.hdr, trak.end, 'tkhd')
		const mdia = find(b, trak.off + trak.hdr, trak.end, 'mdia')
		if (tkhd === undefined || mdia === undefined) continue
		const hdlr = find(b, mdia.off + mdia.hdr, mdia.end, 'hdlr')
		const mdhd = find(b, mdia.off + mdia.hdr, mdia.end, 'mdhd')
		if (hdlr === undefined || mdhd === undefined) continue
		const handler = String.fromCharCode(...b.subarray(hdlr.off + hdlr.hdr + 8, hdlr.off + hdlr.hdr + 12))
		if (handler !== 'vide') continue
		const tv = b[tkhd.off + tkhd.hdr]
		const track = v.getUint32(tkhd.off + tkhd.hdr + (tv === 1 ? 20 : 12))
		const mv = b[mdhd.off + mdhd.hdr]
		const timescale = v.getUint32(mdhd.off + mdhd.hdr + (mv === 1 ? 20 : 12))

		return { bytes: b.subarray(0, end), track, timescale, codecs: codecs.join(',') }
	}

	return undefined
}

/** at walks down a path of boxes from a box, or undefined. */
function at(b: Uint8Array, from: Box, ...path: string[]): Box | undefined {
	let x: Box | undefined = from
	for (const type of path) {
		x = find(b, x.off + x.hdr, x.end, type)
		if (x === undefined) return undefined
	}

	return x
}

/**
 * codecOf is a track's codec as RFC 6381 names it: `avc1.PPCCLL` from the
 * `avcC`, `mp4a.40.N` from the `esds`, `opus`; the sample entry's type for
 * anything else.
 */
function codecOf(b: Uint8Array, trak: Box): string | undefined {
	const stsd = at(b, trak, 'mdia', 'minf', 'stbl', 'stsd')
	if (stsd === undefined) return undefined
	// Version and flags, then the entry count; the first entry is the one.
	const entry = boxes(b, stsd.off + stsd.hdr + 8, stsd.end)[0]
	if (entry === undefined) return undefined
	const hex = (n: number): string => n.toString(16).padStart(2, '0')
	switch (entry.type) {
		case 'avc1':
		case 'avc3': {
			// A visual sample entry: 8 bytes of reserved and the reference
			// index, 70 of picture fields, then its boxes.
			const avcC = find(b, entry.off + entry.hdr + 78, entry.end, 'avcC')
			if (avcC === undefined) return entry.type
			const p = avcC.off + avcC.hdr
			return `${entry.type}.${hex(b[p + 1]!)}${hex(b[p + 2]!)}${hex(b[p + 3]!)}`
		}
		case 'mp4a': {
			// An audio sample entry: 8 bytes of reserved and the reference
			// index, 20 of sound fields, then its boxes.
			const esds = find(b, entry.off + entry.hdr + 28, entry.end, 'esds')
			const object = esds === undefined ? undefined : audioObject(b, esds.off + esds.hdr + 4, esds.end)
			return object === undefined ? 'mp4a.40.2' : `mp4a.${hex(object.type)}${object.audio === undefined ? '' : `.${object.audio}`}`
		}
		case 'Opus':
			return 'opus'
		default:
			return entry.type
	}
}

/**
 * audioObject reads an `esds` descriptor: the object type of its decoder
 * config and, for MPEG-4 audio (0x40), the audio object type in the first
 * five bits of its specific info.
 */
function audioObject(b: Uint8Array, p: number, end: number): { type: number; audio: number | undefined } | undefined {
	const descriptor = (): { tag: number; body: number; end: number } | undefined => {
		if (p + 2 > end) return undefined
		const tag = b[p++]!
		let size = 0
		for (let i = 0; i < 4; i++) {
			const x = b[p++]!
			size = (size << 7) | (x & 0x7f)
			if ((x & 0x80) === 0) break
		}
		return { tag, body: p, end: Math.min(end, p + size) }
	}
	const es = descriptor()
	if (es === undefined || es.tag !== 0x03) return undefined
	const flags = b[p + 2]!
	p += 3
	if (flags & 0x80) p += 2
	if (flags & 0x40) p += 1 + b[p]!
	if (flags & 0x20) p += 2
	const config = descriptor()
	if (config === undefined || config.tag !== 0x04) return undefined
	const type = b[config.body]!
	if (type !== 0x40) return { type, audio: undefined }
	p = config.body + 13
	const specific = descriptor()
	if (specific === undefined || specific.tag !== 0x05) return { type, audio: undefined }

	return { type, audio: b[specific.body]! >> 3 }
}

/** Key is one entry of a lamina's index: a keyframe's decode time and offset. */
export interface Key {
	time: number
	offset: number
}

/** indexSize is what the last 16 bytes of a lamina say its index is, or 0. */
export function indexSize(tail: Uint8Array): number {
	if (tail.length < 16) return 0
	const t = tail.subarray(tail.length - 16)
	if (String.fromCharCode(t[4]!, t[5]!, t[6]!, t[7]!) !== 'mfro') return 0

	return new DataView(t.buffer, t.byteOffset, t.byteLength).getUint32(12)
}

/** parseIndex reads an `mfra`: the track it is for and its keys. */
export function parseIndex(b: Uint8Array): { track: number; keys: Key[] } | undefined {
	const mfra = find(b, 0, b.length, 'mfra')
	if (mfra === undefined) return undefined
	const tfra = find(b, mfra.off + mfra.hdr, mfra.end, 'tfra')
	if (tfra === undefined) return undefined
	const v = new DataView(b.buffer, b.byteOffset, b.byteLength)
	let p = tfra.off + tfra.hdr
	const version = b[p]!
	const track = v.getUint32(p + 4)
	const sizes = v.getUint32(p + 8)
	const n = v.getUint32(p + 12)
	const extra = ((sizes >> 4) & 3) + 1 + ((sizes >> 2) & 3) + 1 + (sizes & 3) + 1
	p += 16
	const keys: Key[] = []
	for (let i = 0; i < n; i++) {
		if (version === 1) {
			if (p + 16 + extra > tfra.end) break
			keys.push({ time: Number(v.getBigUint64(p)), offset: Number(v.getBigUint64(p + 8)) })
			p += 16 + extra
		} else {
			if (p + 8 + extra > tfra.end) break
			keys.push({ time: v.getUint32(p), offset: v.getUint32(p + 4) })
			p += 8 + extra
		}
	}

	return { track, keys }
}

/**
 * fragmentTime is the decode time of the first fragment in a range whose
 * `traf` is the track's, from its `tfdt`; undefined when there is none.
 */
export function fragmentTime(b: Uint8Array, track: number, off = 0): number | undefined {
	const v = new DataView(b.buffer, b.byteOffset, b.byteLength)
	for (const x of boxes(b, off)) {
		if (x.type !== 'moof') continue
		for (const traf of boxes(b, x.off + x.hdr, x.end)) {
			if (traf.type !== 'traf') continue
			const tfhd = find(b, traf.off + traf.hdr, traf.end, 'tfhd')
			const tfdt = find(b, traf.off + traf.hdr, traf.end, 'tfdt')
			if (tfhd === undefined || tfdt === undefined) continue
			if (v.getUint32(tfhd.off + tfhd.hdr + 4) !== track) continue
			const ver = b[tfdt.off + tfdt.hdr]
			return ver === 1 ? Number(v.getBigUint64(tfdt.off + tfdt.hdr + 4)) : v.getUint32(tfdt.off + tfdt.hdr + 4)
		}
	}

	return undefined
}

/** firstFragment is where the first `moof` of a range begins, or -1. */
export function firstFragment(b: Uint8Array): number {
	for (const x of boxes(b)) {
		if (x.type === 'moof') return x.off
	}

	return -1
}

/** concat joins byte arrays. */
export function concat(parts: Uint8Array[]): Bytes {
	let n = 0
	for (const p of parts) n += p.length
	const out = new Uint8Array(n)
	let off = 0
	for (const p of parts) {
		out.set(p, off)
		off += p.length
	}

	return out
}
