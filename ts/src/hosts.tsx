/**
 * Hosts: what is waiting to be adopted, and what has been (§33.4).
 *
 * Nodes and relays are the cluster's and wait for an operator; producers and
 * readers belong to a tenant and wait for its admin, a producer for a set to
 * record. What the page shows for a pending host is what the design says an
 * operator checks before adopting it: the hostname, the hardware identity,
 * the key fingerprint the host prints in its own log, and whether this is a
 * known host coming back with a new key.
 *
 * @module
 */

import { useState, type ReactNode } from 'react'

import { useCall, useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { Set as SetEntity } from '../gen/entities.js'
import { HardwareIdKind } from '../gen/shale/common_pb.js'
import { HostState, type Node, type Producer, type Reader, type Relay } from '../gen/shale/host_pb.js'
import type { HostJoin } from '../gen/shale/common_pb.js'
import { NodeService, ProducerService, ReaderService, RelayService } from '../gen/shale/host_svc_pb.js'
import type { Set } from '../gen/shale/set_pb.js'
import { SetService } from '../gen/shale/set_svc_pb.js'

import { tenantOf } from './session.js'
import { On, useSurfaces } from './surface.js'
import { Ago, Badge, Empty, Err, Loading, byId, bytes, date, hex, useNow, usePolled } from './ui.js'

const DOWN_AFTER_MS = 90_000

export function Hosts(): ReactNode {
	return (
		<>
			<h1>Hosts</h1>
			<p className="lede">
				A host joins on its own and waits to be adopted. Check the hostname, the hardware identity and the key fingerprint it printed in its
				log before you adopt it; a known host back with a new key continues its old row.
			</p>
			<On surface="cluster" note="Nodes and relays are the cluster's: sign in to the cluster API as an operator to see them.">
				<ClusterHosts />
			</On>
			<On surface="tenant" note="Producers and readers belong to a tenant: sign in to the tenant API to see them.">
				<TenantHosts />
			</On>
		</>
	)
}

function hardwareKind(k: HardwareIdKind): string {
	switch (k) {
		case HardwareIdKind.DMI:
			return 'DMI'
		case HardwareIdKind.DEVICE_TREE:
			return 'device tree'
		case HardwareIdKind.MACHINE_ID:
			return 'machine-id'
		default:
			return ''
	}
}

/** Identity is what an operator checks against the host's own log. */
function Identity(props: { join: HostJoin | undefined; hardwareId: string }): ReactNode {
	const j = props.join

	return (
		<div className="mono">
			<div>
				{props.hardwareId || j?.hardwareId || '-'} <span className="dim">{hardwareKind(j?.hardwareIdKind ?? HardwareIdKind.UNSPECIFIED)}</span>
			</div>
			{j !== undefined && (
				<div className="dim">
					key {j.keyFingerprint || '-'} · from {j.sourceAddress || '?'} · {j.version || ''}
				</div>
			)}
		</div>
	)
}

function Seen(props: { at: Node['dateSeen']; adopted: boolean }): ReactNode {
	const now = useNow()
	const d = date(props.at)
	if (!props.adopted) return <Badge tone="warn">pending</Badge>
	if (d === undefined) return <Badge tone="dim">never seen</Badge>
	const down = now - d.getTime() > DOWN_AFTER_MS

	return (
		<>
			<Badge tone={down ? 'bad' : 'ok'}>{down ? 'down' : 'up'}</Badge> <Ago at={props.at} className="dim" />
		</>
	)
}

function CertExpiry(props: { at: Node['dateCertExpires'] }): ReactNode {
	const now = useNow(60_000)
	const d = date(props.at)
	if (d === undefined) return <span className="dim">-</span>
	const days = (d.getTime() - now) / 86_400_000

	return <span className={days < 14 ? 'warn' : 'dim'}>{days < 0 ? 'expired' : `${Math.floor(days)} d`}</span>
}

// ---- the cluster's hosts -------------------------------------------------

function ClusterHosts(): ReactNode {
	const nodes = usePolled(NodeService.method.list, { filters: [], size: 200 })
	const relays = usePolled(RelayService.method.list, { filters: [], size: 200 })
	if (nodes.state === 'error') return <Err error={nodes.error} />
	if (relays.state === 'error') return <Err error={relays.error} />
	if (nodes.data === undefined || relays.data === undefined) return <Loading />

	const pendingNodes = nodes.data.items.filter((v) => v.state === HostState.PENDING)
	const pendingRelays = relays.data.items.filter((v) => v.state === HostState.PENDING)
	const adoptedNodes = nodes.data.items.filter((v) => v.state === HostState.ADOPTED)
	const adoptedRelays = relays.data.items.filter((v) => v.state === HostState.ADOPTED)

	return (
		<>
			<h2>Waiting for the operator</h2>
			{pendingNodes.length + pendingRelays.length === 0 ? (
				<Empty>No node or relay is waiting.</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>kind</th>
								<th>hostname</th>
								<th>identity</th>
								<th>joined</th>
								<th></th>
							</tr>
						</thead>
						<tbody>
							{pendingNodes.map((v) => (
								<PendingNode key={key(v.id)} node={v} />
							))}
							{pendingRelays.map((v) => (
								<PendingRelay key={key(v.id)} relay={v} />
							))}
						</tbody>
					</table>
				</div>
			)}

			<h2>Storage nodes</h2>
			{adoptedNodes.length === 0 ? (
				<Empty>No node yet.</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>alias</th>
								<th>hostname</th>
								<th>state</th>
								<th>sinks</th>
								<th>uploads</th>
								<th>laminae</th>
								<th>version</th>
								<th>certificate</th>
							</tr>
						</thead>
						<tbody>
							{adoptedNodes.map((v) => (
								<tr key={key(v.id)}>
									<td>{v.alias}</td>
									<td>
										{v.hostname} <span className="dim mono">{v.dataAddress}</span>
										{v.status !== undefined && v.status.warnings.length > 0 && (
											<div className="warn">{v.status.warnings.join('; ')}</div>
										)}
									</td>
									<td>
										<Seen at={v.dateSeen} adopted />
									</td>
									<td className="num">
										{v.status?.sinks ?? '-'} <span className="dim">/ {v.status?.devices ?? '-'} devices</span>
									</td>
									<td className="num">{v.status === undefined ? '-' : String(v.status.uploadsInFlight)}</td>
									<td className="num">{v.status === undefined ? '-' : Number(v.status.indexLaminae).toLocaleString()}</td>
									<td className="dim">{v.version}</td>
									<td>
										<CertExpiry at={v.dateCertExpires} />
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			)}

			<h2>Relays</h2>
			{adoptedRelays.length === 0 ? (
				<Empty>No relay yet: nothing plays live until one joins (§39).</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>alias</th>
								<th>hostname</th>
								<th>state</th>
								<th>producers</th>
								<th>viewers</th>
								<th>egress</th>
								<th>ingest / whep</th>
								<th>certificate</th>
							</tr>
						</thead>
						<tbody>
							{adoptedRelays.map((v) => (
								<tr key={key(v.id)}>
									<td>{v.alias}</td>
									<td>{v.hostname}</td>
									<td>
										<Seen at={v.dateSeen} adopted />
									</td>
									<td className="num">
										{v.status?.attachedProducers ?? '-'} <span className="dim">· {v.status?.activeSources ?? '-'} sources live</span>
									</td>
									<td className="num">{v.status?.viewers ?? '-'}</td>
									<td className="num">{v.status === undefined ? '-' : `${bytes(v.status.egressBps / 8n)}/s`}</td>
									<td className="dim mono">
										{v.ingestAddress} · {v.whepAddress}
									</td>
									<td>
										<CertExpiry at={v.dateCertExpires} />
									</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			)}
		</>
	)
}

function PendingNode(props: { node: Node }): ReactNode {
	const adopt = useCall(NodeService.method.adopt)
	const v = props.node

	return (
		<tr>
			<td>
				<Badge tone="info">node</Badge>
				{v.join?.rejoin && (
					<>
						{' '}
						<Badge tone="warn" title="a known host, back with a new key: adopting it continues its row">
							known host, new key
						</Badge>
					</>
				)}
			</td>
			<td>
				{v.hostname}
				{v.alias !== '' && <span className="dim"> as {v.alias}</span>}
			</td>
			<td>
				<Identity join={v.join} hardwareId={v.hardwareId} />
			</td>
			<td>
				<Ago at={v.join?.dateJoined ?? v.dateCreated} />
			</td>
			<td className="actions">
				<button
					type="button"
					className="primary"
					disabled={adopt.state === 'pending'}
					onClick={() => adopt.call({ ref: byId(v.id) }).catch(() => {})}
				>
					adopt
				</button>
				{adopt.state === 'error' && <div className="bad">{String(adopt.error)}</div>}
			</td>
		</tr>
	)
}

function PendingRelay(props: { relay: Relay }): ReactNode {
	const adopt = useCall(RelayService.method.adopt)
	const v = props.relay

	return (
		<tr>
			<td>
				<Badge tone="info">relay</Badge>
				{v.join?.rejoin && (
					<>
						{' '}
						<Badge tone="warn">known host, new key</Badge>
					</>
				)}
			</td>
			<td>{v.hostname}</td>
			<td>
				<Identity join={v.join} hardwareId={v.hardwareId} />
			</td>
			<td>
				<Ago at={v.join?.dateJoined ?? v.dateCreated} />
			</td>
			<td className="actions">
				<button
					type="button"
					className="primary"
					disabled={adopt.state === 'pending'}
					onClick={() => adopt.call({ ref: byId(v.id) }).catch(() => {})}
				>
					adopt
				</button>
				{adopt.state === 'error' && <div className="bad">{String(adopt.error)}</div>}
			</td>
		</tr>
	)
}

// ---- a tenant's hosts ------------------------------------------------------

function TenantHosts(): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const filter = { tenant: { key: { case: 'alias' as const, value: tenant } } }
	const producers = usePolled(ProducerService.method.list, { filters: [filter], size: 200 })
	const readers = usePolled(ReaderService.method.list, { filters: [filter], size: 200 })
	const sets = usePolled(SetService.method.list, { filters: [filter], size: 200 }, 15000)
	if (producers.state === 'error') return <Err error={producers.error} />
	if (readers.state === 'error') return <Err error={readers.error} />
	if (producers.data === undefined || readers.data === undefined) return <Loading />

	const pendingP = producers.data.items.filter((v) => v.state === HostState.PENDING)
	const pendingR = readers.data.items.filter((v) => v.state === HostState.PENDING)
	const adoptedP = producers.data.items.filter((v) => v.state === HostState.ADOPTED)
	const adoptedR = readers.data.items.filter((v) => v.state === HostState.ADOPTED)

	return (
		<>
			<h2>Waiting for the tenant</h2>
			{pendingP.length + pendingR.length === 0 ? (
				<Empty>No producer or reader is waiting.</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>kind</th>
								<th>hostname</th>
								<th>identity</th>
								<th>joined</th>
								<th></th>
							</tr>
						</thead>
						<tbody>
							{pendingP.map((v) => (
								<PendingProducer key={key(v.id)} producer={v} sets={sets.data?.items ?? []} />
							))}
							{pendingR.map((v) => (
								<PendingReader key={key(v.id)} reader={v} />
							))}
						</tbody>
					</table>
				</div>
			)}

			<h2>Producers</h2>
			{adoptedP.length === 0 ? (
				<Empty>No producer yet: a producer is the host beside the cameras (§38).</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>alias</th>
								<th>hostname</th>
								<th>state</th>
								<th>set</th>
								<th>cameras</th>
								<th>host</th>
								<th>version</th>
								<th>certificate</th>
							</tr>
						</thead>
						<tbody>
							{adoptedP.map((v) => (
								<ProducerRow key={key(v.id)} producer={v} />
							))}
						</tbody>
					</table>
				</div>
			)}

			<h2>Readers</h2>
			{adoptedR.length === 0 ? (
				<Empty>No reader: a reader is a system that plays recordings, such as a monitoring wall.</Empty>
			) : (
				<div className="wrap">
					<table>
						<thead>
							<tr>
								<th>alias</th>
								<th>hostname</th>
								<th>state</th>
								<th>sites</th>
								<th>version</th>
							</tr>
						</thead>
						<tbody>
							{adoptedR.map((v) => (
								<tr key={key(v.id)}>
									<td>{v.alias}</td>
									<td>{v.hostname}</td>
									<td>
										<Seen at={v.dateSeen} adopted />
									</td>
									<td>{v.allSites ? 'the whole tenant' : 'some'}</td>
									<td className="dim">{v.version}</td>
								</tr>
							))}
						</tbody>
					</table>
				</div>
			)}
		</>
	)
}

function ProducerRow(props: { producer: Producer }): ReactNode {
	const v = props.producer
	const set = useRow<Set>(SetEntity.typeName, v.set?.id)
	const st = v.status
	const up = st?.sources.filter((r) => r.inputUp).length ?? 0
	const dark = st?.sources.filter((r) => r.dark).length ?? 0

	return (
		<tr>
			<td>{v.alias}</td>
			<td>{v.hostname}</td>
			<td>
				<Seen at={v.dateSeen} adopted />
			</td>
			<td>{set === undefined ? <span className="dim mono">{hex(v.set?.id).slice(0, 8)}</span> : <a href={`#/cameras`}>{set.alias}</a>}</td>
			<td>
				{st === undefined ? (
					<span className="dim">-</span>
				) : (
					<>
						{up} of {st.sources.length} up{dark > 0 && <span className="dim">, {dark} dark</span>}
					</>
				)}
			</td>
			<td className="dim num">
				{st?.load === undefined ? '-' : `cpu ${st.load.cpu.toFixed(2)} · ${st.load.temperature > 0 ? `${st.load.temperature.toFixed(0)} °C · ` : ''}${bytes(st.load.uplinkBps / 8n)}/s up`}
			</td>
			<td className="dim">{v.version}</td>
			<td>
				<CertExpiry at={v.dateCertExpires} />
			</td>
		</tr>
	)
}

function PendingProducer(props: { producer: Producer; sets: Set[] }): ReactNode {
	const adopt = useCall(ProducerService.method.adopt)
	const [setId, setSetId] = useState<string>('')
	const v = props.producer
	const chosen = props.sets.find((s) => hex(s.id) === setId) ?? props.sets[0]

	return (
		<tr>
			<td>
				<Badge tone="info">producer</Badge>
				{v.join?.rejoin && (
					<>
						{' '}
						<Badge tone="warn">known host, new key</Badge>
					</>
				)}
			</td>
			<td>{v.hostname}</td>
			<td>
				<Identity join={v.join} hardwareId={v.hardwareId} />
			</td>
			<td>
				<Ago at={v.join?.dateJoined ?? v.dateCreated} />
			</td>
			<td className="actions">
				<select value={chosen === undefined ? '' : hex(chosen.id)} onChange={(e) => setSetId(e.target.value)} title="the set this producer records">
					{props.sets.length === 0 && <option value="">no set yet</option>}
					{props.sets.map((s) => (
						<option key={key(s.id)} value={hex(s.id)}>
							{s.alias}
						</option>
					))}
				</select>{' '}
				<button
					type="button"
					className="primary"
					disabled={adopt.state === 'pending' || chosen === undefined}
					onClick={() => {
						if (chosen === undefined) return
						adopt.call({ ref: byId(v.id), set: byId(chosen.id) }).catch(() => {})
					}}
				>
					adopt
				</button>
				{adopt.state === 'error' && <div className="bad">{String(adopt.error)}</div>}
			</td>
		</tr>
	)
}

function PendingReader(props: { reader: Reader }): ReactNode {
	const adopt = useCall(ReaderService.method.adopt)
	const v = props.reader

	return (
		<tr>
			<td>
				<Badge tone="info">reader</Badge>
			</td>
			<td>{v.hostname}</td>
			<td>
				<Identity join={v.join} hardwareId={v.hardwareId} />
			</td>
			<td>
				<Ago at={v.join?.dateJoined ?? v.dateCreated} />
			</td>
			<td className="actions">
				<button
					type="button"
					className="primary"
					disabled={adopt.state === 'pending'}
					onClick={() => adopt.call({ ref: byId(v.id), sites: [] }).catch(() => {})}
					title="the whole tenant; narrow it to sites with the CLI"
				>
					adopt
				</button>
				{adopt.state === 'error' && <div className="bad">{String(adopt.error)}</div>}
			</td>
		</tr>
	)
}
