/**
 * Devices: the quarantine queue first, then every disk and the sinks on it
 * (§27). A device the node scored as failing is quarantined on its own; what
 * is an operator's decision is what happens next: back on probation after a
 * cable is reseated, retired to reads only, declared dead so its laminae are
 * LOST and the disk is forgotten.
 *
 * @module
 */

import { useState, type ReactNode } from 'react'

import { useCall, useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { Node as NodeEntity } from '../gen/entities.js'
import { Pressure } from '../gen/shale/common_pb.js'
import type { Node } from '../gen/shale/host_pb.js'
import { DeviceHealth, SinkAttachment, type Device, type Sink } from '../gen/shale/storage_pb.js'
import { DeviceService, SinkService } from '../gen/shale/storage_svc_pb.js'

import { On } from './surface.js'
import { Ago, Badge, Bar, Empty, Err, Loading, When, byId, bytes, confirmThen, sameId, usePolled, type Tone } from './ui.js'

export function Devices(): ReactNode {
	return (
		<>
			<h1>Devices</h1>
			<p className="lede">
				A node scores every disk from what it sees on it and quarantines one that fails; the queue here is what waits for a decision. A
				replaced disk joins as a new device with a new sink.
			</p>
			<On surface="cluster" note="Disks are the cluster's: sign in to the cluster API as an operator.">
				<Queue />
			</On>
		</>
	)
}

function health(h: DeviceHealth): { tone: Tone; text: string } {
	switch (h) {
		case DeviceHealth.HEALTHY:
			return { tone: 'ok', text: 'healthy' }
		case DeviceHealth.SUSPECT:
			return { tone: 'warn', text: 'suspect' }
		case DeviceHealth.QUARANTINED:
			return { tone: 'bad', text: 'quarantined' }
		case DeviceHealth.RETIRED:
			return { tone: 'dim', text: 'retired' }
		case DeviceHealth.DEAD:
			return { tone: 'dim', text: 'dead' }
		default:
			return { tone: 'dim', text: '?' }
	}
}

function NodeName(props: { id: Uint8Array | undefined }): ReactNode {
	const n = useRow<Node>(NodeEntity.typeName, props.id)

	return <>{n?.alias ?? n?.hostname ?? <span className="dim">?</span>}</>
}

function Queue(): ReactNode {
	const devices = usePolled(DeviceService.method.list, { filters: [], size: 500 })
	const sinks = usePolled(SinkService.method.list, { filters: [], size: 500 })
	if (devices.state === 'error') return <Err error={devices.error} />
	if (sinks.state === 'error') return <Err error={sinks.error} />
	if (devices.data === undefined || sinks.data === undefined) return <Loading />

	const queue = devices.data.items.filter((v) => v.health === DeviceHealth.QUARANTINED || v.health === DeviceHealth.SUSPECT)
	const rest = devices.data.items.filter((v) => v.health !== DeviceHealth.QUARANTINED && v.health !== DeviceHealth.SUSPECT)

	return (
		<>
			<h2>Quarantine queue</h2>
			{queue.length === 0 ? (
				<Empty>Nothing is quarantined.</Empty>
			) : (
				<div className="cards">
					{queue.map((v) => (
						<Quarantined key={key(v.id)} device={v} />
					))}
				</div>
			)}

			<h2>Disks</h2>
			{rest.length === 0 ? (
				<Empty>No disk yet: a node lists its sinks when it joins (§22.2).</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>node</th>
								<th>device</th>
								<th>health</th>
								<th>score</th>
								<th>errors</th>
								<th>latency</th>
								<th>smart</th>
								<th>sinks</th>
							</tr>
						</thead>
						<tbody>
							{rest.map((v) => (
								<DiskRow key={key(v.id)} device={v} sinks={sinks.data?.items.filter((s) => sameId(s.device?.id, v.id)) ?? []} />
							))}
						</tbody>
					</table>
				</div>
			)}
		</>
	)
}

function Quarantined(props: { device: Device }): ReactNode {
	const v = props.device
	const q = v.quarantine
	const h = health(v.health)
	const release = useCall(DeviceService.method.release)
	const retire = useCall(DeviceService.method.retire)
	const dead = useCall(DeviceService.method.declareDead)
	const locate = useCall(DeviceService.method.locate)
	const [reason, setReason] = useState('')
	const busy = release.state === 'pending' || retire.state === 'pending' || dead.state === 'pending'
	const err = [release, retire, dead, locate].find((c) => c.state === 'error')

	return (
		<div className="card">
			<div className="title">
				<b>
					<NodeName id={v.node?.id} /> · {v.slot || v.alias} <span className="dim mono">{v.hardwareId}</span>
				</b>
				<Badge tone={h.tone}>{h.text}</Badge>
			</div>
			<dl>
				<dt>why</dt>
				<dd>{q?.reason || '-'}</dd>
				<dt>since</dt>
				<dd>
					<Ago at={q?.date} />
					{q?.operator && <span className="dim"> by an operator</span>}
				</dd>
				<dt>score</dt>
				<dd>{(q?.score ?? v.failureScore).toFixed(2)}</dd>
				{q !== undefined && q.recentErrors.length > 0 && (
					<>
						<dt>recent</dt>
						<dd className="mono">{q.recentErrors.slice(-3).join('; ')}</dd>
					</>
				)}
				{q?.dateProbationEnds !== undefined && (
					<>
						<dt>probation ends</dt>
						<dd>
							<When at={q.dateProbationEnds} />
						</dd>
					</>
				)}
				<dt>disk</dt>
				<dd>
					{v.model || '-'} {v.capacity > 0n && <span className="dim">{bytes(v.capacity)}</span>}
				</dd>
			</dl>
			<input value={reason} placeholder="the reason, for the record" onChange={(e) => setReason(e.target.value)} />
			<div className="toolbar">
				<button type="button" disabled={busy || reason === ''} onClick={() => release.call({ ref: byId(v.id), reason }).catch(() => {})}>
					release
				</button>
				<button type="button" disabled={busy || reason === ''} onClick={() => retire.call({ ref: byId(v.id), reason }).catch(() => {})}>
					retire
				</button>
				<button
					type="button"
					className="danger"
					disabled={busy || reason === ''}
					onClick={() => confirmThen('Declare this disk dead? Its laminae become LOST and the device is forgotten.', () => dead.call({ ref: byId(v.id), reason }).catch(() => {}))}
				>
					declare dead
				</button>
				<button type="button" disabled={locate.state === 'pending'} onClick={() => locate.call({ ref: byId(v.id), off: false }).catch(() => {})}>
					locate
				</button>
			</div>
			{err !== undefined && <p className="bad">{String(err.error)}</p>}
		</div>
	)
}

function DiskRow(props: { device: Device; sinks: Sink[] }): ReactNode {
	const v = props.device
	const h = health(v.health)
	const r = v.report
	const locate = useCall(DeviceService.method.locate)

	return (
		<tr>
			<td>
				<NodeName id={v.node?.id} />
			</td>
			<td>
				{v.slot || v.alias} <span className="dim mono">{v.hardwareId}</span>
				<div className="dim">
					{v.model}
					{v.capacity > 0n && ` · ${bytes(v.capacity)}`}
				</div>
			</td>
			<td>
				<Badge tone={h.tone}>{h.text}</Badge>
			</td>
			<td className="num">{v.failureScore.toFixed(2)}</td>
			<td className="num">{r === undefined ? '-' : `${r.ioErrors} io · ${r.failedWrites} writes · ${r.timeouts} timeouts`}</td>
			<td className="num">{r === undefined ? '-' : `w ${r.writeLatencyMs.toFixed(0)} · r ${r.readLatencyMs.toFixed(0)} ms`}</td>
			<td>
				{v.smart === undefined ? (
					<span className="dim">-</span>
				) : (
					<span className={v.smart.passed ? 'ok' : 'bad'}>
						{v.smart.passed ? 'passed' : 'failed'}
						{v.smart.reallocated + v.smart.pending > 0n && <span className="dim"> · {String(v.smart.reallocated + v.smart.pending)} bad sectors</span>}
						{v.smart.temperature > 0 && <span className="dim"> · {v.smart.temperature.toFixed(0)} °C</span>}
					</span>
				)}
			</td>
			<td>
				{props.sinks.length === 0 ? (
					<span className="dim">none</span>
				) : (
					props.sinks.map((s) => <SinkLine key={key(s.id)} sink={s} />)
				)}
				<button type="button" disabled={locate.state === 'pending'} onClick={() => locate.call({ ref: byId(v.id), off: false }).catch(() => {})} title="light the bay LED">
					locate
				</button>
			</td>
		</tr>
	)
}

function SinkLine(props: { sink: Sink }): ReactNode {
	const s = props.sink
	const used = s.capacity - s.free
	const tone: Tone = s.pressure === Pressure.CRITICAL ? 'bad' : s.pressure === Pressure.RECLAIM ? 'warn' : 'ok'
	let att: ReactNode = null
	if (s.attachment === SinkAttachment.PENDING_ADOPTION) att = <Badge tone="warn">pending adoption</Badge>
	else if (s.attachment === SinkAttachment.RETIRED) att = <Badge tone="dim">retired</Badge>
	else if (!s.acceptWrites) att = <Badge tone="warn">reads only</Badge>

	return (
		<div>
			<span className="mono">{s.alias || s.path}</span>{' '}
			<Bar value={Number(used)} of={Number(s.capacity)} tone={tone} title={`${bytes(used)} of ${bytes(s.capacity)}`} />{' '}
			<span className="dim num">
				{bytes(used)} / {bytes(s.capacity)} · {Number(s.laminae).toLocaleString()} laminae
			</span>{' '}
			{att}
			{s.warnings.length > 0 && <div className="warn">{s.warnings.join('; ')}</div>}
		</div>
	)
}
