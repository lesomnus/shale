/**
 * Playback: one camera at any moment (§40). The laminae `Timeline` names and
 * the relay's recent window are fragmented MP4 that the browser's Media
 * Source Extensions play as they are (§23.1, §39.4): a moment picked on the
 * axis becomes a Range from the keyframe its lamina's index names, the
 * fragments stream in ahead of the playhead, the next lamina follows, and
 * the last minutes come from the relay until the playhead reaches now and
 * the player hands over to live. A range of the axis exports as one MP4:
 * the init segment and the fragments between two keyframes, concatenated.
 *
 * @module
 */

import { timestampFromMs } from '@bufbuild/protobuf/wkt'
import { useEffect, useRef, useState, type ReactNode } from 'react'

import { useQuery } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { LaminaService, ReadState, type TimelineLamina, type TimelineSource } from '../gen/shale/lamina_svc_pb.js'
import type { LiveSource } from '../gen/shale/set_svc_pb.js'
import type { Source } from '../gen/shale/set_pb.js'
import { SetService, SourceService } from '../gen/shale/set_svc_pb.js'

import { concat, firstFragment, fragmentTime, indexSize, parseIndex, parseInit, type Bytes, type Init, type Key } from './fmp4.js'
import { useLive, whep, type Status } from './live.js'
import { PickSet, gapClass, unhex } from './segments.js'
import { On, useSurfaces } from './surface.js'
import { Badge, Err, Loading, byId, date, hex, sameId, usePolled } from './ui.js'

export function Playback(props: { set: string | undefined; source: string | undefined }): ReactNode {
	return (
		<>
			<h1>Playback</h1>
			<p className="lede">A camera at any moment: what is stored, what the relay still holds of the last minutes, and live at the end of it. A range of the axis downloads as one MP4.</p>
			<On surface="tenant" note="Recordings are a tenant's: sign in to the tenant API.">
				{props.set === undefined ? <PickSet to="playback" /> : <OfSet id={unhex(props.set)} source={props.source === undefined ? undefined : unhex(props.source)} />}
			</On>
		</>
	)
}

function OfSet(props: { id: Uint8Array; source: Uint8Array | undefined }): ReactNode {
	const set = useQuery(SetService.method.get, { ref: byId(props.id) })
	const sources = useQuery(SourceService.method.list, { filters: [{ set: byId(props.id) }], size: 200 })
	const live = useLive(props.id)
	if (set.state === 'error') return <Err error={set.error} />
	if (sources.state === 'error') return <Err error={sources.error} />
	if (set.data === undefined || sources.data === undefined) return <Loading />
	const cameras = [...sources.data.items].sort((a, b) => a.ordinal - b.ordinal)
	const picked = cameras.find((v) => sameId(v.id, props.source)) ?? cameras[0]
	if (picked === undefined) return <p className="empty">This set has no camera yet.</p>
	const feed = live.sources?.find((v) => sameId(v.sourceId, picked.id))

	return (
		<>
			<h2>
				{set.data.alias}
				<span className="dim"> · </span>
				<select value={hex(picked.id)} onChange={(e) => (location.hash = `#/playback/${hex(props.id)}/${e.target.value}`)} aria-label="camera">
					{cameras.map((v) => (
						<option key={key(v.id)} value={hex(v.id)}>
							{v.alias} #{v.ordinal}
						</option>
					))}
				</select>
			</h2>
			<Deck key={hex(picked.id)} source={picked} live={feed} />
		</>
	)
}

const MINUTE = 60_000
const HOUR = 60 * MINUTE
const SPANS: ReadonlyArray<{ ms: number; label: string }> = [
	{ ms: 15 * MINUTE, label: '15 min' },
	{ ms: HOUR, label: '1 h' },
	{ ms: 6 * HOUR, label: '6 h' },
	{ ms: 24 * HOUR, label: '24 h' },
]

type Mode = 'idle' | 'playback' | 'live'

/** Deck is the player of one camera: the axis, the video, the controls. */
function Deck(props: { source: Source; live: LiveSource | undefined }): ReactNode {
	const sandbox = useSurfaces().mode.kind === 'sandbox'
	const [span, setSpan] = useState(HOUR)
	const [to, setTo] = useState(() => Date.now())
	useEffect(() => {
		const t = setInterval(() => setTo(Date.now()), 30_000)

		return () => clearInterval(t)
	}, [])
	const from = to - span
	const tl = usePolled(LaminaService.method.timeline, { source: byId(props.source.id), from: timestampFromMs(from), to: timestampFromMs(to), size: 1000 }, 30_000)
	const video = useRef<HTMLVideoElement>(null)
	const engine = useRef<Engine | null>(null)
	const [mode, setMode] = useState<Mode>('idle')
	const [status, setStatus] = useState<Status>({ tone: 'dim', text: 'pick a moment on the axis' })
	const [head, setHead] = useState<number | undefined>(undefined)
	const [paused, setPaused] = useState(true)
	const [mark, setMark] = useState<{ a: number | undefined; b: number | undefined }>({ a: undefined, b: undefined })
	const [exporting, setExporting] = useState<string | undefined>(undefined)
	const strip = tl.data?.sources[0]
	const laminae = strip?.laminae ?? []

	// The engine follows the timeline and the relay as they change.
	useEffect(() => {
		engine.current?.setTimeline(laminae)
	}, [tl.data])
	useEffect(() => {
		engine.current?.setRecent(props.live)
	}, [props.live?.recentUrl, props.live?.viewToken])
	useEffect(() => () => engine.current?.destroy(), [])

	// Live: WHEP on the same element, the engine gone; back to playback on
	// the next pick.
	useEffect(() => {
		const el = video.current
		if (mode !== 'live' || el === null || props.live === undefined) return
		engine.current?.destroy()
		engine.current = null
		setHead(undefined)

		return whep(el, props.live.whepUrl, props.live.viewToken, setStatus)
	}, [mode, props.live?.whepUrl])

	const seek = (wall: number): void => {
		const el = video.current
		if (el === null || sandbox) return
		if (mode === 'live') el.srcObject = null
		setMode('playback')
		if (engine.current?.stopped === true) {
			// The last one's source closed under it: a fresh one for this pick.
			engine.current.destroy()
			engine.current = null
		}
		if (engine.current === null) {
			engine.current = new Engine(el, {
				status: setStatus,
				head: setHead,
				paused: setPaused,
				live: () => setMode('live'),
			})
			engine.current.setTimeline(laminae)
			engine.current.setRecent(props.live)
		}
		void engine.current.seek(wall)
	}
	const toggle = (): void => {
		const el = video.current
		if (el === null) return
		if (el.paused) void el.play().catch(() => {})
		else el.pause()
	}
	const doExport = async (): Promise<void> => {
		if (mark.a === undefined || mark.b === undefined) return
		const [a, b] = mark.a < mark.b ? [mark.a, mark.b] : [mark.b, mark.a]
		setExporting('fetching')
		try {
			const bytes = await exportRange(laminae, a, b, (n) => setExporting(`fetching, ${Math.round(n / 1e6)} MB`))
			const name = `${props.source.alias}-${new Date(a).toISOString().replace(/[:.]/g, '-')}-${Math.round((b - a) / 1000)}s.mp4`
			const url = URL.createObjectURL(new Blob([bytes], { type: 'video/mp4' }))
			const link = document.createElement('a')
			link.href = url
			link.download = name
			link.click()
			setTimeout(() => URL.revokeObjectURL(url), 60_000)
			setExporting(undefined)
		} catch (err) {
			setExporting(`failed: ${String(err).slice(0, 80)}`)
		}
	}
	const now = Date.now()
	const at = head === undefined ? undefined : new Date(head)

	return (
		<div className="deck">
			<div className="player">
				<video ref={video} playsInline muted controls onTimeUpdate={() => engine.current?.tick()} onSeeking={() => engine.current?.seeking()} />
				<div className="under">
					<b>
						{props.source.alias}
						<span className="dim"> #{props.source.ordinal}</span>
					</b>
					<Badge tone={status.tone}>{status.text}</Badge>
					<span className="mono">{at === undefined ? (mode === 'live' ? 'live' : '--:--:--') : at.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</span>
				</div>
			</div>
			<div className="controls">
				<button onClick={() => seek((head ?? now) - 10_000)} disabled={sandbox} title="ten seconds back">
					⟲ 10 s
				</button>
				<button onClick={toggle} disabled={mode === 'idle'} title="play or pause">
					{paused ? '▶' : '❚❚'}
				</button>
				<button onClick={() => seek((head ?? now) + 10_000)} disabled={sandbox || head === undefined} title="ten seconds on">
					10 s ⟳
				</button>
				<button onClick={() => seek(now - 10_000)} disabled={sandbox} title="what the relay still holds: the last ten seconds">
					10 s ago
				</button>
				<button onClick={() => setMode('live')} disabled={props.live === undefined || mode === 'live'} className={mode === 'live' ? 'primary' : ''} title="live, through the relay">
					● Live
				</button>
				<span className="grow" />
				<select value={span} onChange={(e) => setSpan(Number(e.target.value))} aria-label="span">
					{SPANS.map((s) => (
						<option key={s.ms} value={s.ms}>
							{s.label}
						</option>
					))}
				</select>
			</div>
			<Axis strip={strip} from={from} to={to} head={head} mark={mark} onPick={seek} />
			<div className="controls">
				<button onClick={() => setMark({ ...mark, a: head })} disabled={head === undefined} title="the export starts at the playhead">
					mark start
				</button>
				<button onClick={() => setMark({ ...mark, b: head })} disabled={head === undefined} title="the export ends at the playhead">
					mark end
				</button>
				<span className="mono dim">
					{mark.a !== undefined ? new Date(mark.a).toLocaleTimeString() : '…'} – {mark.b !== undefined ? new Date(mark.b).toLocaleTimeString() : '…'}
				</span>
				<button className="primary" onClick={() => void doExport()} disabled={mark.a === undefined || mark.b === undefined || exporting !== undefined || sandbox}>
					{exporting ?? 'download MP4'}
				</button>
			</div>
			{tl.state === 'error' && <Err error={tl.error} />}
			{sandbox && <p className="dim">The sandbox has no bytes to play: the axis is real, the video is not.</p>}
		</div>
	)
}

/** Axis is the time axis: what is stored and what is missing, the playhead, the export marks. */
function Axis(props: { strip: TimelineSource | undefined; from: number; to: number; head: number | undefined; mark: { a: number | undefined; b: number | undefined }; onPick: (wall: number) => void }): ReactNode {
	const w = 1000
	const h = 36
	const span = props.to - props.from
	const x = (ms: number): number => Math.max(0, Math.min(w, ((ms - props.from) / span) * w))
	const rects: ReactNode[] = []
	for (const g of props.strip?.gaps ?? []) {
		const a = date(g.from)?.getTime()
		const b = date(g.to)?.getTime()
		if (a === undefined || b === undefined) continue
		rects.push(<rect key={`g${a}`} className={gapClass(g.reason)} x={x(a)} y={6} width={Math.max(1, x(b) - x(a))} height={h - 12} />)
	}
	for (const l of props.strip?.laminae ?? []) {
		const a = date(l.dateStarted)?.getTime()
		const b = date(l.dateEnded)?.getTime()
		if (a === undefined || b === undefined) continue
		rects.push(<rect key={`l${a}`} className={l.state === ReadState.AVAILABLE ? 'lamina' : 'unavailable'} x={x(a)} y={8} width={Math.max(1, x(b) - x(a) - 0.5)} height={h - 16} rx={1} />)
	}
	const pick = (e: React.MouseEvent<SVGSVGElement>): void => {
		const r = e.currentTarget.getBoundingClientRect()
		const wall = props.from + ((e.clientX - r.left) / r.width) * span
		props.onPick(Math.round(wall))
	}
	const fmt = (ms: number): string => new Date(ms).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })

	return (
		<div className="axis">
			<svg className="strip" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" onClick={pick} role="slider" aria-label="time">
				<rect className="axis-bg" x={0} y={6} width={w} height={h - 12} />
				{rects}
				{props.mark.a !== undefined && props.mark.b !== undefined && (
					<rect className="mark" x={x(Math.min(props.mark.a, props.mark.b))} y={2} width={Math.max(2, Math.abs(x(props.mark.a) - x(props.mark.b)))} height={h - 4} />
				)}
				{props.head !== undefined && <line className="head" x1={x(props.head)} x2={x(props.head)} y1={0} y2={h} />}
			</svg>
			<div className="ticks">
				<span>{fmt(props.from)}</span>
				<span>{fmt(props.from + span / 2)}</span>
				<span>{fmt(props.to)}</span>
			</div>
		</div>
	)
}

// ---- the engine -----------------------------------------------------------

/** Lamina is a stored one as the engine keeps it, with times in ms. */
interface Lamina {
	id: string
	start: number
	end: number
	size: number
	url: string
	available: boolean
}

/** Meta is what a lamina's bytes say about themselves, fetched once. */
interface Meta {
	init: Init
	keys: Key[]
	/** Where the fragments end: the index begins there. */
	dataEnd: number
}

/** A fetch of a byte range, by the token the URL carries. */
async function range(url: string, from: number, to: number): Promise<Bytes> {
	const r = await fetch(url, { headers: { Range: `bytes=${from}-${to}` } })
	if (r.status !== 206 && r.status !== 200) throw new Error(`${r.status} from the node`)
	const b = new Uint8Array(await r.arrayBuffer())

	return r.status === 200 ? b.slice(from, to + 1) : b
}

const metas = new Map<string, Promise<Meta>>()

/** metaOf reads a lamina's index and init segment, once per lamina. */
function metaOf(l: Lamina): Promise<Meta> {
	let p = metas.get(l.id)
	if (p === undefined) {
		p = (async () => {
			const tail = await range(l.url, Math.max(0, l.size - 16), l.size - 1)
			const n = indexSize(tail)
			if (n > 0) {
				const idx = parseIndex(await range(l.url, l.size - n, l.size - 1))
				if (idx !== undefined && idx.keys.length > 0) {
					const first = idx.keys[0]!
					const init = parseInit(await range(l.url, 0, first.offset - 1))
					if (init === undefined) throw new Error('no init segment')

					return { init, keys: idx.keys, dataEnd: l.size - n }
				}
			}
			// No index (a lamina cut short): the init segment and the first
			// fragment are what the head says, and playback starts there.
			const head = await range(l.url, 0, Math.min(l.size, 512 << 10) - 1)
			const init = parseInit(head)
			const off = firstFragment(head)
			if (init === undefined || off < 0) throw new Error('not a lamina this plays')
			const time = fragmentTime(head, init.track, off) ?? 0

			return { init, keys: [{ time, offset: off }], dataEnd: l.size }
		})()
		metas.set(l.id, p)
		p.catch(() => metas.delete(l.id))
	}

	return p
}

/** ahead is how far past the playhead the engine keeps bytes, in seconds. */
const ahead = 20
/** chunk is one Range fetch of a lamina. */
const chunk = 1 << 20

interface Hooks {
	status: (s: Status) => void
	head: (wall: number | undefined) => void
	paused: (v: boolean) => void
	live: () => void
}

/**
 * Engine drives one MediaSource: a moment becomes a lamina and a keyframe,
 * the bytes stream in ahead of the playhead, the next lamina follows, and
 * the relay's recent window takes over where the store ends.
 */
class Engine {
	private readonly ms = new MediaSource()
	private sb: SourceBuffer | undefined
	private readonly url: string
	/** base is the wall time of media time zero: a day before the engine was made. */
	private readonly base: number
	private laminae: Lamina[] = []
	private recent: { url: string; token: string } | undefined
	private gen = 0
	/** codecs is what the SourceBuffer was last made or changed for. */
	private codecs = ''
	private queue: Array<{ bytes: Bytes; place: { offset: number; codecs: string } | undefined; done: () => void; fail: (e: unknown) => void }> = []
	private stream: { lamina: Lamina; meta: Meta; next: number } | { recentEnd: number } | undefined
	private pumping = false
	private closed = false
	/** target is the media time the engine itself last set, so its own seek is not taken for the viewer's. */
	private target: number | undefined
	private loading = false

	constructor(private readonly video: HTMLVideoElement, private readonly hooks: Hooks) {
		this.base = Date.now() - 24 * HOUR
		this.url = URL.createObjectURL(this.ms)
		this.ms.addEventListener('sourceopen', () => {
			if (this.sb !== undefined) return
			this.ms.duration = (Date.now() + HOUR - this.base) / 1000
			// The SourceBuffer waits for the first init segment: the tracks a
			// capture writes differ (video alone, or AAC or Opus beside it),
			// and a browser refuses a segment that lacks a promised one.
			this.next()
		})
		this.ms.addEventListener('sourceclose', () => {
			// The element let go of the source, an error most likely: its
			// SourceBuffer is gone, and nothing more can be played through it.
			if (this.closed) return
			this.closed = true
			this.gen++
			this.queue = []
			this.hooks.status({ tone: 'bad', text: `the player stopped${video.error === null ? '' : `: ${video.error.message}`}`.slice(0, 120) })
		})
		video.srcObject = null
		video.src = this.url
		video.addEventListener('play', () => this.hooks.paused(false))
		video.addEventListener('pause', () => this.hooks.paused(true))
	}

	setTimeline(laminae: TimelineLamina[]): void {
		this.laminae = laminae
			.map((l) => ({
				id: hex(l.laminaId),
				start: date(l.dateStarted)?.getTime() ?? 0,
				end: date(l.dateEnded)?.getTime() ?? 0,
				size: Number(l.size),
				url: l.url,
				available: l.state === ReadState.AVAILABLE && !l.incomplete,
			}))
			.filter((l) => l.start > 0 && l.end > l.start && l.url !== '')
			.sort((a, b) => a.start - b.start)
	}

	setRecent(live: LiveSource | undefined): void {
		this.recent = live === undefined || live.recentUrl === '' ? undefined : { url: live.recentUrl, token: live.viewToken }
	}

	/** stopped is when the engine plays nothing more: destroyed, or its source closed. */
	get stopped(): boolean {
		return this.closed
	}

	destroy(): void {
		this.closed = true
		this.gen++
		this.queue = []
		try {
			if (this.ms.readyState === 'open') this.ms.endOfStream()
		} catch {
			// Closed already.
		}
		URL.revokeObjectURL(this.url)
		if (this.video.src === this.url) this.video.removeAttribute('src')
	}

	/** wall is a media time as wall-clock ms, and media the other way. */
	private wall(t: number): number {
		return this.base + t * 1000
	}
	private media(wall: number): number {
		return (wall - this.base) / 1000
	}

	/** seek plays from a moment: what is buffered, a lamina, or the recent window. */
	async seek(wall: number): Promise<void> {
		if (this.closed) return
		const gen = ++this.gen
		const t = this.media(wall)
		this.hooks.head(wall)
		if (this.buffered(t)) {
			this.setTime(t)
			void this.video.play().catch(() => {})
			return
		}
		this.stream = undefined
		this.queue = []
		if (this.sb !== undefined && this.sb.updating) this.sb.abort()
		this.loading = true
		try {
			const lamina = this.laminae.find((l) => l.available && l.start <= wall && wall < l.end)
			if (lamina !== undefined) {
				await this.playLamina(lamina, wall, gen)
				return
			}
			if (this.recent !== undefined && wall > Date.now() - 20 * MINUTE && (await this.playRecent(wall, gen))) return
			if (gen !== this.gen) return
			const next = this.laminae.find((l) => l.available && l.start > wall)
			if (next !== undefined) {
				this.hooks.status({ tone: 'warn', text: `nothing at ${new Date(wall).toLocaleTimeString()}; from the next recording` })
				await this.playLamina(next, next.start, gen)
				return
			}
			this.hooks.status({ tone: 'warn', text: 'nothing recorded at that moment' })
		} finally {
			if (gen === this.gen) this.loading = false
		}
	}

	/** setTime moves the playhead, and remembers it was the engine that did. */
	private setTime(t: number): void {
		this.target = t
		this.video.currentTime = t
	}

	/** seeking is the element's own seek bar: a moment picked there, not one the engine set. */
	seeking(): void {
		if (this.closed || !this.video.seeking || this.loading) return
		const t = this.video.currentTime
		if (this.target !== undefined && Math.abs(t - this.target) < 0.1) return
		if (this.buffered(t)) return
		void this.seek(this.wall(t))
	}

	/** tick keeps the bytes ahead of the playhead and the playhead reported. */
	tick(): void {
		if (this.closed) return
		const t = this.video.currentTime
		this.hooks.head(this.wall(t))
		if (this.stream !== undefined && 'recentEnd' in this.stream && this.wall(t) >= this.stream.recentEnd - 1500) {
			// The relay's window is played out: live from here.
			this.hooks.status({ tone: 'ok', text: 'caught up: live' })
			this.hooks.live()
			return
		}
		void this.pump(this.gen)
		this.evict(t)
	}

	/** live is the SourceBuffer while it is still the source's: none once that closed. */
	private live(): SourceBuffer | undefined {
		return this.closed ? undefined : this.sb
	}

	private buffered(t: number): boolean {
		const sb = this.live()
		if (sb === undefined) return false
		for (let i = 0; i < sb.buffered.length; i++) {
			if (sb.buffered.start(i) - 0.3 <= t && t < sb.buffered.end(i) - 0.3) return true
		}

		return false
	}

	/** bufferedAhead is how many seconds are buffered past the playhead. */
	private bufferedAhead(): number {
		const sb = this.live()
		if (sb === undefined) return 0
		const t = this.video.currentTime
		for (let i = 0; i < sb.buffered.length; i++) {
			if (sb.buffered.start(i) - 0.3 <= t && t <= sb.buffered.end(i)) return sb.buffered.end(i) - t
		}

		return 0
	}

	private evict(t: number): void {
		const sb = this.live()
		if (sb === undefined || sb.updating || sb.buffered.length === 0) return
		const start = sb.buffered.start(0)
		if (t - start > 180) sb.remove(start, t - 90)
	}

	private async playLamina(l: Lamina, wall: number, gen: number): Promise<void> {
		this.hooks.status({ tone: 'dim', text: 'reading the index' })
		let meta: Meta
		try {
			meta = await metaOf(l)
		} catch (err) {
			if (gen === this.gen) this.hooks.status({ tone: 'bad', text: String(err).slice(0, 80) })
			return
		}
		if (gen !== this.gen) return
		const first = meta.keys[0]!
		const rel = (wall - l.start) / 1000
		let k = first
		for (const x of meta.keys) {
			if ((x.time - first.time) / meta.init.timescale <= rel) k = x
		}
		// The lamina's first key fragment came at date_started: that is where
		// its decode time sits on the wall clock.
		const offset = this.media(l.start) - first.time / meta.init.timescale
		await this.append(meta.init.bytes, { offset, codecs: meta.init.codecs })
		if (gen !== this.gen) return
		this.stream = { lamina: l, meta, next: k.offset }
		this.setTime(Math.max(this.media(wall), this.media(l.start) + (k.time - first.time) / meta.init.timescale))
		void this.video.play().catch(() => {})
		this.hooks.status({ tone: 'ok', text: 'playing the recording' })
		await this.pump(gen)
	}

	private async playRecent(wall: number, gen: number): Promise<boolean> {
		if (this.recent === undefined) return false
		this.hooks.status({ tone: 'dim', text: 'asking the relay' })
		const since = Math.max(1, Math.ceil((Date.now() - wall) / 1000) + 2)
		const r = await fetch(`${this.recent.url}?token=${encodeURIComponent(this.recent.token)}&since=${since}`)
		if (gen !== this.gen) return true
		if (r.status !== 200) return false
		const start = Date.parse(r.headers.get('Shale-Recent-Start') ?? '')
		const seconds = Number(r.headers.get('Shale-Recent-Seconds') ?? '0')
		const b = new Uint8Array(await r.arrayBuffer())
		if (gen !== this.gen) return true
		const init = parseInit(b)
		const time = init === undefined ? undefined : fragmentTime(b, init.track, init.bytes.length)
		if (init === undefined || time === undefined || !Number.isFinite(start)) {
			this.hooks.status({ tone: 'bad', text: 'the relay answered something this cannot play' })
			return true
		}
		const offset = this.media(start) - time / init.timescale
		await this.append(b, { offset, codecs: init.codecs })
		if (gen !== this.gen) return true
		this.stream = { recentEnd: start + seconds * 1000 }
		this.setTime(Math.max(this.media(wall), this.media(start)))
		void this.video.play().catch(() => {})
		this.hooks.status({ tone: 'ok', text: `the relay's last ${Math.round(seconds)} s` })

		return true
	}

	/** pump fetches the next chunk of the lamina while less than `ahead` is buffered. */
	private async pump(gen: number): Promise<void> {
		if (this.pumping) return
		this.pumping = true
		try {
			while (gen === this.gen && this.stream !== undefined && 'lamina' in this.stream && this.bufferedAhead() < ahead) {
				const s = this.stream
				if (s.next >= s.meta.dataEnd) {
					// The lamina is done: the one that follows it, or the relay.
					const after = this.laminae.find((l) => l.available && l.start >= s.lamina.end - 1000 && l.id !== s.lamina.id)
					if (after !== undefined && after.start - s.lamina.end < 1000) {
						const meta = await metaOf(after)
						if (gen !== this.gen) return
						const first = meta.keys[0]!
						await this.append(meta.init.bytes, { offset: this.media(after.start) - first.time / meta.init.timescale, codecs: meta.init.codecs })
						if (gen !== this.gen) return
						this.stream = { lamina: after, meta, next: first.offset }
						continue
					}
					this.stream = undefined
					if (this.recent !== undefined && Date.now() - s.lamina.end < 20 * MINUTE) {
						// The rest is the relay's: the open lamina.
						const g = ++this.gen
						this.hooks.status({ tone: 'dim', text: 'the store ends here; asking the relay for the rest' })
						if (!(await this.playRecent(s.lamina.end, g)) && g === this.gen) this.hooks.status({ tone: 'warn', text: 'the recording ends here' })
					} else if (after !== undefined) {
						this.hooks.status({ tone: 'warn', text: `a gap until ${new Date(after.start).toLocaleTimeString()}` })
						void this.seek(after.start)
					} else {
						this.hooks.status({ tone: 'warn', text: 'the recording ends here' })
					}
					return
				}
				const end = Math.min(s.meta.dataEnd, s.next + chunk)
				const bytes = await range(s.lamina.url, s.next, end - 1)
				if (gen !== this.gen) return
				s.next = end
				await this.append(bytes)
			}
		} catch (err) {
			if (gen === this.gen) this.hooks.status({ tone: 'bad', text: String(err).slice(0, 80) })
		} finally {
			this.pumping = false
		}
	}

	/**
	 * append queues bytes for the SourceBuffer; an init segment comes placed:
	 * the timestamp offset to set first, and the codecs it holds.
	 */
	private append(bytes: Bytes, place?: { offset: number; codecs: string }): Promise<void> {
		return new Promise((done, fail) => {
			this.queue.push({ bytes, place, done, fail })
			this.next()
		})
	}

	private next(): void {
		if (this.closed) return
		let sb = this.sb
		// Before the first init segment the source must be open to take a SourceBuffer.
		if (sb === undefined ? this.ms.readyState !== 'open' : sb.updating) return
		const job = this.queue.shift()
		if (job === undefined) return
		try {
			if (job.place !== undefined) {
				const type = `video/mp4; codecs="${job.place.codecs}"`
				if (sb === undefined) {
					sb = this.ms.addSourceBuffer(type)
					sb.mode = 'segments'
					sb.addEventListener('updateend', () => this.next())
					sb.addEventListener('error', () => this.hooks.status({ tone: 'bad', text: 'the browser refused the bytes' }))
					this.sb = sb
				} else {
					// A new placement begins a new segment: whatever the parser
					// held of the last one goes, so the offset may be set.
					if (this.ms.readyState === 'open') sb.abort()
					// A lamina of another encoder: High before, Baseline now.
					if (job.place.codecs !== this.codecs) sb.changeType(type)
				}
				this.codecs = job.place.codecs
				sb.timestampOffset = job.place.offset
			}
			if (sb === undefined) throw new Error('media before an init segment')
			sb.appendBuffer(job.bytes)
			job.done()
		} catch (err) {
			job.fail(err)
		}
	}
}

/** exportRange is the init segment and the fragments of a range, as one MP4. */
async function exportRange(laminae: TimelineLamina[], a: number, b: number, progress: (bytes: number) => void): Promise<Bytes> {
	const parts: Uint8Array[] = []
	let init: Bytes | undefined
	let n = 0
	const rows: Lamina[] = laminae
		.map((l) => ({
			id: hex(l.laminaId),
			start: date(l.dateStarted)?.getTime() ?? 0,
			end: date(l.dateEnded)?.getTime() ?? 0,
			size: Number(l.size),
			url: l.url,
			available: l.state === ReadState.AVAILABLE && !l.incomplete,
		}))
		.filter((l) => l.available && l.end > a && l.start < b)
		.sort((x, y) => x.start - y.start)
	if (rows.length === 0) throw new Error('nothing stored in that range')
	for (const l of rows) {
		const meta = await metaOf(l)
		const first = meta.keys[0]!
		if (init === undefined || !same(init, meta.init.bytes)) {
			init = meta.init.bytes
			parts.push(init)
			n += init.length
		}
		let from = first.offset
		let to = meta.dataEnd
		for (const k of meta.keys) {
			const at = l.start + ((k.time - first.time) / meta.init.timescale) * 1000
			if (at <= a) from = k.offset
			if (at > b) {
				to = k.offset
				break
			}
		}
		for (let off = from; off < to; off += chunk) {
			const bytes = await range(l.url, off, Math.min(to, off + chunk) - 1)
			parts.push(bytes)
			n += bytes.length
			progress(n)
		}
	}

	return concat(parts)
}

function same(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) return false
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false

	return true
}
