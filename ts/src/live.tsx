/**
 * Live: every camera of a set, played through the relay the CP assigned its
 * producer (§39.4). `Live` answers a WHEP URL and a view token per source; the
 * page posts an SDP offer to the relay and plays the answer, and asks `Live`
 * again before the token lapses.
 *
 * In the sandbox there is no camera behind the relay it plays: each source
 * is drawn by this page itself, a moving scene on a canvas, so the wall can
 * be looked at and tested without a byte of real video.
 *
 * @module
 */

import { useEffect, useRef, useState, type ReactNode, type RefObject } from 'react'

import { useQuery, useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { Source as SourceEntity } from '../gen/entities.js'
import type { LiveSource } from '../gen/shale/set_svc_pb.js'
import type { Source } from '../gen/shale/set_pb.js'
import { SetService } from '../gen/shale/set_svc_pb.js'

import { app } from './client.js'
import { PickSet, unhex } from './segments.js'
import { On, useSurfaces } from './surface.js'
import { Badge, Err, Loading, byId, date, hex, type Tone } from './ui.js'

export function Live(props: { set: string | undefined }): ReactNode {
	return (
		<>
			<h1>Live</h1>
			<p className="lede">The cameras of a set as they are now, through the relay. Nothing is recorded here, and nothing here is missed by the recording.</p>
			<On surface="tenant" note="Watching is a tenant's: sign in to the tenant API.">
				{props.set === undefined ? <PickSet to="live" /> : <Wall id={unhex(props.set)} />}
			</On>
		</>
	)
}

/** useLive asks `Live` for a set, and again before its tokens lapse. */
export function useLive(id: Uint8Array): { sources: LiveSource[] | undefined; error: unknown } {
	const s = useSurfaces()
	const transport = s.sessions.tenant?.session.transport
	const [state, setState] = useState<{ sources: LiveSource[] | undefined; error: unknown }>({ sources: undefined, error: null })
	useEffect(() => {
		if (transport === undefined) return
		let stop = false
		let timer: ReturnType<typeof setTimeout> | undefined
		const ask = async (): Promise<void> => {
			try {
				const r = await app(transport).set.live({ ref: byId(id) })
				if (stop) return
				setState({ sources: r.sources, error: null })
				// Again a minute before the earliest token lapses, and not
				// sooner than every ten seconds.
				const exp = Math.min(...r.sources.map((v) => date(v.dateExpires)?.getTime() ?? Infinity))
				const wait = Number.isFinite(exp) ? Math.max(10_000, exp - Date.now() - 60_000) : 5 * 60_000
				timer = setTimeout(() => void ask(), wait)
			} catch (err) {
				if (stop) return
				setState({ sources: undefined, error: err })
				timer = setTimeout(() => void ask(), 10_000)
			}
		}
		void ask()

		return () => {
			stop = true
			if (timer !== undefined) clearTimeout(timer)
		}
	}, [transport, hex(id)])

	return state
}

function Wall(props: { id: Uint8Array }): ReactNode {
	const set = useQuery(SetService.method.get, { ref: byId(props.id) })
	const live = useLive(props.id)
	if (set.state === 'error') return <Err error={set.error} />
	if (live.error !== null) return <Err error={live.error} />
	if (set.data === undefined || live.sources === undefined) return <Loading />

	return (
		<>
			<h2>{set.data.alias}</h2>
			<div className="wall">
				{[...live.sources]
					.sort((a, b) => a.ordinal - b.ordinal)
					.map((v) => (
						<Player key={key(v.sourceId)} set={props.id} source={v} />
					))}
			</div>
		</>
	)
}

export type Status = { tone: Tone; text: string }

/** Player is one camera: WHEP against the relay, or the sandbox's own scene. */
function Player(props: { set: Uint8Array; source: LiveSource }): ReactNode {
	const v = props.source
	const src = useRow<Source>(SourceEntity.typeName, v.sourceId)
	const video = useRef<HTMLVideoElement>(null)
	const [status, setStatus] = useState<Status>({ tone: 'dim', text: 'connecting' })
	// The sandbox's relay is played by the page: there is no byte of video
	// behind its address, and the wall is what is being looked at.
	const fake = useSurfaces().mode.kind === 'sandbox'

	useEffect(() => {
		const el = video.current
		if (el === null || !fake) return

		return fakeScene(el, src?.alias ?? hex(v.sourceId).slice(0, 8), setStatus)
	}, [fake])
	useWhep(video, v.whepUrl, v.viewToken, setStatus, !fake)

	return (
		<div className="player">
			<video ref={video} autoPlay playsInline muted controls />
			<div className="under">
				<b>
					{src?.alias ?? hex(v.sourceId).slice(0, 8)}
					<span className="dim"> #{v.ordinal}</span>
				</b>
				<Badge tone={status.tone}>{status.text}</Badge>
				<a className="dim" href={`#/playback/${hex(props.set)}/${hex(v.sourceId)}`} title="the last minutes, and the recordings">
					rewind
				</a>
			</div>
		</div>
	)
}

/**
 * useWhep keeps one WHEP session on a video element while `on`: opened for
 * the URL, renewed with each fresh token the page is handed (§39.4), and
 * opened again only when the relay will not renew it -- the session ended,
 * or a relay that does not take renewals -- since the relay ends a session
 * with its token.
 */
export function useWhep(video: RefObject<HTMLVideoElement | null>, url: string | undefined, token: string | undefined, setStatus: (s: Status) => void, on: boolean): void {
	const session = useRef<Whep | null>(null)
	const latest = useRef(token)
	latest.current = token
	const [again, setAgain] = useState(0)
	useEffect(() => {
		const el = video.current
		if (!on || el === null || url === undefined || latest.current === undefined) return
		const s = whep(el, url, latest.current, setStatus)
		session.current = s

		return () => {
			s.close()
			if (session.current === s) session.current = null
		}
	}, [on, url, again])
	useEffect(() => {
		const s = session.current
		if (s === null || token === undefined) return
		void s.renew(token).then((ok) => {
			if (!ok && session.current === s) setAgain((n) => n + 1)
		})
	}, [token])
}

/** Whep is one WHEP session. */
export interface Whep {
	close: () => void
	/** renew hands the session a fresh token; false when the relay would not take it. */
	renew: (token: string) => Promise<boolean>
}

/** whep plays one source through the relay, as web/live.html does. */
export function whep(el: HTMLVideoElement, url: string, token: string, setStatus: (s: Status) => void): Whep {
	const pc = new RTCPeerConnection()
	let session: string | null = null
	let closed = false
	let current = token
	const base = url.slice(0, url.indexOf('/whep/'))
	pc.addTransceiver('video', { direction: 'recvonly' })
	pc.addTransceiver('audio', { direction: 'recvonly' })
	pc.ontrack = (ev) => {
		const stream = ev.streams[0]
		if (stream !== undefined) el.srcObject = stream
	}
	pc.onconnectionstatechange = () => {
		switch (pc.connectionState) {
			case 'connected':
				setStatus({ tone: 'ok', text: 'live' })
				break
			case 'failed':
			case 'disconnected':
				setStatus({ tone: 'bad', text: pc.connectionState })
				break
			default:
				setStatus({ tone: 'dim', text: pc.connectionState })
		}
	}
	const opened = (async (): Promise<void> => {
		try {
			const offer = await pc.createOffer()
			await pc.setLocalDescription(offer)
			await new Promise<void>((r) => {
				if (pc.iceGatheringState === 'complete') r()
				else pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && r()
			})
			if (closed) return
			setStatus({ tone: 'dim', text: 'asking the relay' })
			const resp = await fetch(url, {
				method: 'POST',
				headers: { 'Content-Type': 'application/sdp', Authorization: 'Shale ' + token },
				body: pc.localDescription?.sdp ?? '',
			})
			if (resp.status !== 201) {
				setStatus({ tone: 'bad', text: `refused: ${resp.status} ${(await resp.text()).trim()}`.slice(0, 80) })

				return
			}
			session = resp.headers.get('Location')
			await pc.setRemoteDescription({ type: 'answer', sdp: await resp.text() })
		} catch (err) {
			if (!closed) setStatus({ tone: 'bad', text: String(err).slice(0, 80) })
		}
	})()

	return {
		close: () => {
			closed = true
			if (session !== null) {
				void fetch(base + session, { method: 'DELETE' }).catch(() => {})
			}
			pc.close()
			el.srcObject = null
		},
		renew: async (token) => {
			if (token === current) return true
			await opened
			if (closed || session === null) return false
			try {
				const resp = await fetch(base + session, { method: 'PATCH', headers: { Authorization: 'Shale ' + token } })
				if (resp.status !== 204) return false
			} catch {
				return false
			}
			current = token

			return true
		},
	}
}

/**
 * fakeScene is the sandbox's camera: a canvas with the time, the camera's
 * name and something moving, captured as a stream the video element plays
 * like any other.
 */
function fakeScene(el: HTMLVideoElement, name: string, setStatus: (s: Status) => void): () => void {
	const canvas = document.createElement('canvas')
	canvas.width = 640
	canvas.height = 360
	const ctx = canvas.getContext('2d')
	if (ctx === null) {
		setStatus({ tone: 'bad', text: 'no canvas' })

		return () => {}
	}
	const started = performance.now()
	let raf = 0
	let last = 0
	const draw = (t: number): void => {
		raf = requestAnimationFrame(draw)
		if (t - last < 1000 / 15) return
		last = t
		const s = (t - started) / 1000
		ctx.fillStyle = '#1b2230'
		ctx.fillRect(0, 0, 640, 360)
		// A floor and a wall, so it reads as a room.
		ctx.fillStyle = '#2a3446'
		ctx.fillRect(0, 240, 640, 120)
		// Something that moves: a box crossing the room.
		const xPos = ((s * 60) % 760) - 60
		ctx.fillStyle = '#c9a15a'
		ctx.fillRect(xPos, 200, 60, 50)
		ctx.fillStyle = '#7a5f2f'
		ctx.fillRect(xPos + 10, 210, 40, 30)
		// A little noise, as a camera has.
		for (let i = 0; i < 80; i++) {
			ctx.fillStyle = `rgba(255,255,255,${(Math.random() * 0.08).toFixed(3)})`
			ctx.fillRect(Math.random() * 640, Math.random() * 360, 2, 2)
		}
		ctx.fillStyle = '#e8e6e1'
		ctx.font = '16px system-ui, sans-serif'
		ctx.fillText(name, 12, 26)
		ctx.font = '14px ui-monospace, monospace'
		ctx.fillText(new Date().toISOString().replace('T', ' ').slice(0, 19), 12, 348)
		ctx.fillStyle = '#e25d4a'
		ctx.beginPath()
		ctx.arc(624, 20, 5, 0, Math.PI * 2)
		ctx.fill()
	}
	raf = requestAnimationFrame(draw)
	const stream = canvas.captureStream(15)
	el.srcObject = stream
	setStatus({ tone: 'ok', text: 'live (sandbox)' })

	return () => {
		cancelAnimationFrame(raf)
		for (const t of stream.getTracks()) t.stop()
		el.srcObject = null
	}
}
