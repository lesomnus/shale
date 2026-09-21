/**
 * The small parts every page is made of: times as "how long ago", bytes and
 * bit rates in the units an operator reads, identifiers short enough to
 * recognize, and a badge for a state.
 *
 * @module
 */

import type { DescMessage, DescMethodUnary, MessageInitShape, MessageShape } from '@bufbuild/protobuf'
import { timestampDate, type Timestamp } from '@bufbuild/protobuf/wkt'
import { useEffect, useState } from 'react'

import type { Entry } from '@lesomnus/payday/query'
import { useApp, useQuery } from '@lesomnus/payday/react'

/**
 * usePolled is a list read again every `every` milliseconds. A row the list
 * answered with is watched and stays current on its own; what a watch
 * cannot carry is a row that was not there when the stream opened -- a host
 * that joined, a segment that was cut -- because a watch names rows rather
 * than a predicate (payday's `watch:`). Arrivals come from reading the list
 * again, which for a page of a few hundred rows is cheap.
 */
export function usePolled<I extends DescMessage, O extends DescMessage>(method: DescMethodUnary<I, O>, input: MessageInitShape<I>, every = 5000): Entry<MessageShape<O>> {
	const app = useApp()
	const entry = useQuery(method, input)
	const key = JSON.stringify(input, (_, v: unknown) => (v instanceof Uint8Array ? Array.from(v) : typeof v === 'bigint' ? String(v) : v))
	useEffect(() => {
		const t = setInterval(() => {
			app.queries.call(method, input, { revalidate: true }).catch(() => {})
		}, every)

		return () => clearInterval(t)
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [app, method, key, every])

	return entry
}

/** hex is an identifier as the CLI prints it: a UUID. */
export function hex(id: Uint8Array | undefined): string {
	if (id === undefined || id.length === 0) return ''
	const h = Array.from(id, (b) => b.toString(16).padStart(2, '0')).join('')
	if (h.length !== 32) return h

	return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`
}

/** short is the tail of an identifier, enough to tell rows apart. */
export function short(id: Uint8Array | undefined): string {
	const h = hex(id)

	return h.length > 12 ? h.slice(-12) : h
}

/** sameId compares two identifiers byte for byte. */
export function sameId(a: Uint8Array | undefined, b: Uint8Array | undefined): boolean {
	if (a === undefined || b === undefined || a.length !== b.length) return false
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false
	}

	return true
}

/** byId is a reference by identifier, the way every service takes one. */
export function byId(id: Uint8Array): { key: { case: 'id'; value: Uint8Array } } {
	return { key: { case: 'id', value: id } }
}

/** date is a protobuf timestamp as a Date, or undefined. */
export function date(ts: Timestamp | undefined): Date | undefined {
	return ts === undefined ? undefined : timestampDate(ts)
}

/** useNow is the clock, ticking every `every` milliseconds for what shows an age. */
export function useNow(every = 5000): number {
	const [now, setNow] = useState(Date.now())
	useEffect(() => {
		const t = setInterval(() => setNow(Date.now()), every)

		return () => clearInterval(t)
	}, [every])

	return now
}

/** ago is a duration in words: "12 s", "3 min", "2 h", "5 d". */
export function ago(from: Date | undefined, now: number): string {
	if (from === undefined) return 'never'
	const s = Math.max(0, Math.round((now - from.getTime()) / 1000))
	if (s < 60) return `${s} s`
	if (s < 3600) return `${Math.floor(s / 60)} min`
	if (s < 86400) return `${(s / 3600).toFixed(s < 36000 ? 1 : 0)} h`

	return `${Math.floor(s / 86400)} d`
}

/** Ago draws how long ago a time was, and keeps it current. */
export function Ago(props: { at: Timestamp | undefined; className?: string }): React.ReactNode {
	const now = useNow()
	const d = date(props.at)

	return (
		<span className={props.className} title={d?.toISOString()}>
			{d === undefined ? 'never' : `${ago(d, now)} ago`}
		</span>
	)
}

/** When draws a time as a clock reading, with the date when it is not today. */
export function When(props: { at: Timestamp | undefined }): React.ReactNode {
	const d = date(props.at)
	if (d === undefined) return <span className="dim">-</span>
	const today = new Date().toDateString() === d.toDateString()
	const t = d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })

	return <span title={d.toISOString()}>{today ? t : `${d.toLocaleDateString()} ${t}`}</span>
}

/** bytes is a size in the units a disk is sold in. */
export function bytes(n: bigint | number): string {
	let v = Number(n)
	const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
	let i = 0
	while (v >= 1000 && i < units.length - 1) {
		v /= 1000
		i++
	}

	return `${v.toFixed(v >= 100 || i === 0 ? 0 : 1)} ${units[i]}`
}

/** bps is a bit rate as the configuration writes it. */
export function bps(n: bigint | number): string {
	const v = Number(n)
	if (v >= 1e6) return `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1)} Mbps`
	if (v >= 1e3) return `${(v / 1e3).toFixed(0)} kbps`

	return `${v} bps`
}

/** seconds is a duration in words. */
export function seconds(s: number): string {
	if (s < 90) return `${Math.round(s)} s`
	if (s < 5400) return `${(s / 60).toFixed(1)} min`

	return `${(s / 3600).toFixed(1)} h`
}

export type Tone = 'ok' | 'warn' | 'bad' | 'dim' | 'info'

/** Badge is a state, in a color that says how much to worry. */
export function Badge(props: { tone: Tone; children: React.ReactNode; title?: string }): React.ReactNode {
	return (
		<span className={`badge badge-${props.tone}`} title={props.title}>
			{props.children}
		</span>
	)
}

/** Bar is a share of a whole, for capacity and rate against a ceiling. */
export function Bar(props: { value: number; of: number; tone?: Tone; title?: string }): React.ReactNode {
	const share = props.of > 0 ? Math.max(0, Math.min(1, props.value / props.of)) : 0

	return (
		<span className={`bar bar-${props.tone ?? 'info'}`} title={props.title}>
			<span className="bar-fill" style={{ width: `${(share * 100).toFixed(1)}%` }} />
		</span>
	)
}

/** Empty is a list with nothing in it, said plainly. */
export function Empty(props: { children: React.ReactNode }): React.ReactNode {
	return <p className="empty">{props.children}</p>
}

/** Err is an error the page could not do anything about. */
export function Err(props: { error: unknown }): React.ReactNode {
	return <p className="bad">{String(props.error)}</p>
}

/** Loading is a query not yet answered. */
export function Loading(): React.ReactNode {
	return <p className="dim">loading…</p>
}

/** confirm asks before something that cannot be undone. */
export function confirmThen(question: string, then: () => void): void {
	if (window.confirm(question)) then()
}
