/**
 * Timed frames (§39.4): how long live takes to reach this page, frame by
 * frame. The relay puts an SEI in each video access unit naming the frame
 * and saying when it passed the producer (t1, t2) and the relay (t3, t4);
 * `timing-worker.ts` reads it from the encoded frame, and this side notes
 * when the frame arrived (t5) and was shown (t6), measures this browser's
 * clock against the relay's, and reports the spans only a viewer can see
 * back to the session, which the relay keeps as metrics.
 *
 * @module
 */

import type { Timed } from './sei.js'

/** supported says the browser can read timed frames and see when it shows them. */
export function supported(): boolean {
	return typeof RTCRtpScriptTransform !== 'undefined' && 'requestVideoFrameCallback' in HTMLVideoElement.prototype
}

let worker: Worker | undefined
let sessions = 0
const listeners = new Map<number, (t: Timed) => void>()

/**
 * transform reads the timed frames a receiver gets, in the page's one
 * worker, and hands each to `on`; the returned function stops listening.
 */
export function transform(receiver: RTCRtpReceiver, on: (t: Timed) => void): () => void {
	if (worker === undefined) {
		worker = new Worker(new URL('./timing-worker.ts', import.meta.url), { type: 'module' })
		worker.onmessage = (ev: MessageEvent<Timed>) => listeners.get(ev.data.id)?.(ev.data)
	}
	const id = ++sessions
	listeners.set(id, on)
	receiver.transform = new RTCRtpScriptTransform(worker, { id })

	return () => listeners.delete(id)
}

/** Now on this page's clock, in Unix milliseconds, as video frame callbacks read. */
const now = (): number => performance.timeOrigin + performance.now()

/** One frame's times on this side, in Unix milliseconds of this browser's clock. */
interface Seen {
	t1: number
	t4: number
	/** t5, when the browser had the whole frame, if it says. */
	t5: number | undefined
	/** t6, when it shows it. */
	t6: number
}

/** How often a viewer reports, and how many reports' clock readings it keeps. */
const every = 10_000
const readings = 8

/**
 * Report is a session's measuring: it is told each timed frame, watches the
 * video element for when each is shown, and reports every ten seconds.
 */
export interface Report {
	/** frame takes what the worker read from one frame. */
	frame: (t: Timed) => void
	stop: () => void
}

/**
 * report measures a timed session (§39.4): `post` sends one report and
 * answers the relay's clock; `onDelay` is told the median of t6 - t1 over
 * each report, in milliseconds.
 */
export function report(el: HTMLVideoElement, post: (body: string) => Promise<{ received: number; replied: number } | undefined>, onDelay: (ms: number) => void): Report {
	const timings = new Map<number, Timed>()
	let seen: Seen[] = []
	let stopped = false
	// The relay's clock minus this one's, from the readings with the
	// shortest round trips; undefined until one came.
	const clock: { offset: number; rtt: number }[] = []
	const offset = (): number | undefined => {
		let best: { offset: number; rtt: number } | undefined
		for (const c of clock) if (best === undefined || c.rtt < best.rtt) best = c

		return best?.offset
	}

	const shown = (_: number, md: VideoFrameCallbackMetadata): void => {
		if (stopped) return
		el.requestVideoFrameCallback(shown)
		if (md.rtpTimestamp === undefined) return
		const t = timings.get(md.rtpTimestamp)
		if (t === undefined) return
		timings.delete(md.rtpTimestamp)
		seen.push({ t1: t.t1, t4: t.t4, t5: md.receiveTime === undefined ? undefined : performance.timeOrigin + md.receiveTime, t6: performance.timeOrigin + md.expectedDisplayTime })
	}
	el.requestVideoFrameCallback(shown)

	const send = async (): Promise<void> => {
		const at = offset()
		const frames = at === undefined ? [] : seen.map((s) => span(s, at))
		seen = []
		const c0 = now()
		const answer = await post(JSON.stringify({ frames })).catch(() => undefined)
		const c1 = now()
		if (answer === undefined) return
		// As NTP does: the relay's clock against this one, from when each
		// side sent and received, less the time the relay spent.
		clock.push({ offset: (answer.received - c0 + (answer.replied - c1)) / 2, rtt: c1 - c0 - (answer.replied - answer.received) })
		if (clock.length > readings) clock.shift()
		const totals = frames.map((f) => f.total).filter((v): v is number => v !== undefined).sort((a, b) => a - b)
		const mid = totals[Math.floor(totals.length / 2)]
		if (mid !== undefined) onDelay(mid)
	}
	// The first report is empty: it only reads the clock.
	void send()
	const timer = setInterval(() => void send(), every)

	return {
		frame: (t) => {
			timings.set(t.rtp, t)
			// Frames never shown, dropped or behind a hidden tab, are not
			// kept past a few seconds of them.
			if (timings.size > 300) timings.delete(timings.keys().next().value as number)
		},
		stop: () => {
			stopped = true
			clearInterval(timer)
		},
	}
}

/** span is one frame's report, with the relay's clock `offset` ahead of this one's. */
function span(s: Seen, offset: number): { network?: number; playout?: number; total?: number } {
	const out: { network?: number; playout?: number; total?: number } = {}
	if (s.t5 !== undefined && s.t4 !== 0) out.network = s.t5 + offset - s.t4
	if (s.t5 !== undefined) out.playout = s.t6 - s.t5
	if (s.t1 !== 0) out.total = s.t6 + offset - s.t1

	return out
}
