/**
 * Segments arriving: the laminae of one set as they are allocated, uploaded
 * and committed (§12), watched so a row that changes state changes here, and
 * the last hour of each source as a strip, laminae and gaps with their
 * reasons (§19).
 *
 * @module
 */

import { timestampFromMs } from '@bufbuild/protobuf/wkt'
import { useEffect, useState, type ReactNode } from 'react'

import { useQuery, useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { Source as SourceEntity } from '../gen/entities.js'
import { LaminaSkipReason, LaminaState, type Lamina } from '../gen/shale/lamina_pb.js'
import { GapReason, LaminaService, ReadState, type TimelineSource } from '../gen/shale/lamina_svc_pb.js'
import type { Source } from '../gen/shale/set_pb.js'
import { SetService } from '../gen/shale/set_svc_pb.js'

import { tenantOf } from './session.js'
import { On, useSurfaces } from './surface.js'
import { Badge, Empty, Err, Loading, When, byId, bytes, date, hex, seconds, usePolled, type Tone } from './ui.js'

export function Segments(props: { set: string | undefined }): ReactNode {
	return (
		<>
			<h1>Segments</h1>
			<p className="lede">Every segment a producer cut, as its lamina: allocated, then committed by a node, or skipped, or lost.</p>
			<On surface="tenant" note="Laminae belong to a tenant: sign in to the tenant API.">
				{props.set === undefined ? <PickSet to="segments" /> : <OfSet id={unhex(props.set)} />}
			</On>
		</>
	)
}

/** unhex is the inverse of `hex`: a UUID back to its bytes. */
export function unhex(s: string): Uint8Array {
	const h = s.replace(/-/g, '')
	const out = new Uint8Array(h.length / 2)
	for (let i = 0; i < out.length; i++) out[i] = parseInt(h.slice(2 * i, 2 * i + 2), 16)

	return out
}

/** PickSet lists the sets, for a page reached without one. */
export function PickSet(props: { to: string }): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const sets = useQuery(SetService.method.list, { filters: [{ tenant: { key: { case: 'alias', value: tenant } } }], size: 200 })
	if (sets.state === 'error') return <Err error={sets.error} />
	if (sets.data === undefined) return <Loading />
	if (sets.data.items.length === 0) return <Empty>No set yet.</Empty>

	return (
		<ul>
			{sets.data.items.map((v) => (
				<li key={key(v.id)}>
					<a href={`#/${props.to}/${hex(v.id)}`}>{v.alias}</a>
					{v.name !== '' && <span className="dim"> · {v.name}</span>}
				</li>
			))}
		</ul>
	)
}

function state(v: Lamina): { tone: Tone; text: string } {
	switch (v.state) {
		case LaminaState.PENDING:
			return { tone: 'info', text: 'in progress' }
		case LaminaState.COMMITTED:
			return v.incomplete ? { tone: 'warn', text: 'committed, incomplete' } : { tone: 'ok', text: 'committed' }
		case LaminaState.SKIPPED:
			return { tone: 'dim', text: v.skipReason === LaminaSkipReason.DARK ? 'skipped: dark' : 'skipped' }
		case LaminaState.LOST:
			return { tone: 'bad', text: 'lost' }
		case LaminaState.DELETING:
		case LaminaState.DELETED:
			return { tone: 'dim', text: 'deleted' }
		default:
			return { tone: 'dim', text: '?' }
	}
}

function SourceName(props: { id: Uint8Array | undefined }): ReactNode {
	const s = useRow<Source>(SourceEntity.typeName, props.id)

	return <>{s?.alias ?? <span className="dim mono">{hex(props.id).slice(0, 8)}</span>}</>
}

function OfSet(props: { id: Uint8Array }): ReactNode {
	const set = useQuery(SetService.method.get, { ref: byId(props.id) })
	const laminae = usePolled(LaminaService.method.list, { filters: [{ set: byId(props.id) }], size: 80 })
	if (set.state === 'error') return <Err error={set.error} />
	if (laminae.state === 'error') return <Err error={laminae.error} />
	if (set.data === undefined || laminae.data === undefined) return <Loading />

	const rows = [...laminae.data.items].sort((a, b) => (date(b.dateStarted)?.getTime() ?? 0) - (date(a.dateStarted)?.getTime() ?? 0))

	return (
		<>
			<h2>
				{set.data.alias} · the last hour
			</h2>
			<Strips set={props.id} />
			<h2>{set.data.alias} · segments</h2>
			{rows.length === 0 ? (
				<Empty>Nothing recorded yet.</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>camera</th>
								<th>started</th>
								<th>length</th>
								<th>state</th>
								<th>size</th>
								<th>key</th>
							</tr>
						</thead>
						<tbody>
							{rows.map((v) => {
								const st = state(v)
								const a = date(v.dateStarted)
								const b = date(v.dateEnded)

								return (
									<tr key={key(v.id)}>
										<td>
											<SourceName id={v.source?.id} />
										</td>
										<td>
											<When at={v.dateStarted} />
										</td>
										<td className="num">
											{a !== undefined && b !== undefined ? seconds((b.getTime() - a.getTime()) / 1000) : <span className="dim">open</span>}
											{v.endedEstimated && <span className="dim"> ~</span>}
										</td>
										<td>
											<Badge tone={st.tone}>{st.text}</Badge>
										</td>
										<td className="num">{v.size > 0n ? bytes(v.size) : <span className="dim">-</span>}</td>
										<td className="dim mono">{v.laminaKey.split('/').pop()?.slice(0, 13)}</td>
									</tr>
								)
							})}
						</tbody>
					</table>
				</div>
			)}
		</>
	)
}

const HOUR = 3_600_000

/** Strips is the last hour of every source of a set, asked again each minute. */
function Strips(props: { set: Uint8Array }): ReactNode {
	const [to, setTo] = useState(() => Date.now())
	useEffect(() => {
		const t = setInterval(() => setTo(Date.now()), 60_000)

		return () => clearInterval(t)
	}, [])
	const from = to - HOUR
	const tl = useQuery(LaminaService.method.timeline, {
		set: byId(props.set),
		from: timestampFromMs(from),
		to: timestampFromMs(to),
		size: 1000,
	})
	if (tl.state === 'error') return <Err error={tl.error} />
	if (tl.data === undefined) return <Loading />

	return (
		<>
			<div className="legend">
				<span>
					<i style={{ background: 'var(--ok)' }} /> stored
				</span>
				<span>
					<i style={{ background: 'var(--warn)' }} /> unavailable
				</span>
				<span>
					<i style={{ background: 'var(--info)', opacity: 0.6 }} /> in progress
				</span>
				<span>
					<i style={{ background: '#2b2f52' }} /> dark
				</span>
				<span>
					<i style={{ background: 'var(--bad)' }} /> lost
				</span>
				<span>
					<i style={{ background: 'var(--dim-bg)', border: '1px solid var(--line)' }} /> not received
				</span>
			</div>
			<div className="wrap">
				<table>
					<tbody>
						{[...tl.data.sources]
							.sort((a, b) => a.ordinal - b.ordinal)
							.map((s) => (
								<tr key={key(s.sourceId)}>
									<td style={{ width: 140 }}>
										<SourceName id={s.sourceId} />
									</td>
									<td>
										<Strip source={s} from={from} to={to} />
									</td>
								</tr>
							))}
					</tbody>
				</table>
			</div>
		</>
	)
}

function gapClass(r: GapReason): string {
	switch (r) {
		case GapReason.IN_PROGRESS:
			return 'gap-in-progress'
		case GapReason.DARK:
			return 'gap-dark'
		case GapReason.LOST:
			return 'gap-lost'
		case GapReason.DELETED:
			return 'gap-deleted'
		case GapReason.UNAVAILABLE:
			return 'unavailable'
		default:
			return 'gap-not-received'
	}
}

function Strip(props: { source: TimelineSource; from: number; to: number }): ReactNode {
	const w = 1000
	const span = props.to - props.from
	const x = (ms: number): number => Math.max(0, Math.min(w, ((ms - props.from) / span) * w))
	const rects: ReactNode[] = []
	for (const g of props.source.gaps) {
		const a = date(g.from)?.getTime()
		const b = date(g.to)?.getTime()
		if (a === undefined || b === undefined) continue
		rects.push(<rect key={`g${a}`} className={gapClass(g.reason)} x={x(a)} y={0} width={Math.max(1, x(b) - x(a))} height={22} />)
	}
	for (const l of props.source.laminae) {
		const a = date(l.dateStarted)?.getTime()
		const b = date(l.dateEnded)?.getTime()
		if (a === undefined || b === undefined) continue
		rects.push(
			<rect
				key={`l${a}`}
				className={l.state === ReadState.AVAILABLE ? 'lamina' : 'unavailable'}
				x={x(a)}
				y={2}
				width={Math.max(1, x(b) - x(a) - 0.5)}
				height={18}
				rx={1}
			>
				<title>{`${new Date(a).toLocaleTimeString()} – ${new Date(b).toLocaleTimeString()} · ${bytes(l.size)}${l.incomplete ? ' · incomplete' : ''}`}</title>
			</rect>,
		)
	}

	return (
		<svg className="strip" viewBox={`0 0 ${w} 22`} preserveAspectRatio="none">
			{rects}
		</svg>
	)
}
