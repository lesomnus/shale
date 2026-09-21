/**
 * Cameras: every set of the tenant, its sources, and what its producer's
 * heartbeat says about each (§38.6): up or down, the frame rate, the measured
 * rate against the ceiling, a scene gone dark (§38.10), and the suggestion a
 * starved source earns (§38.5).
 *
 * @module
 */

import type { ReactNode } from 'react'

import { key } from '@lesomnus/payday/store'

import { HostState, type Producer } from '../gen/shale/host_pb.js'
import { ProducerService } from '../gen/shale/host_svc_pb.js'
import type { Set, Source } from '../gen/shale/set_pb.js'
import { UploadMode } from '../gen/shale/common_pb.js'
import { SetService, SourceService } from '../gen/shale/set_svc_pb.js'
import type { SourceReport } from '../gen/shale/common_pb.js'

import { tenantOf } from './session.js'
import { On, useSurfaces } from './surface.js'
import { Ago, Badge, Bar, Empty, Err, Loading, bps, byId, bytes, date, hex, sameId, seconds, useNow, usePolled, type Tone } from './ui.js'

const DOWN_AFTER_MS = 90_000

export function Cameras(): ReactNode {
	return (
		<>
			<h1>Cameras</h1>
			<p className="lede">
				A set is the cameras one producer records; what each camera is doing is what that producer said in its last heartbeat.
			</p>
			<On surface="tenant" note="Sets and cameras belong to a tenant: sign in to the tenant API.">
				<Sets />
			</On>
		</>
	)
}

function Sets(): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const filter = { tenant: { key: { case: 'alias' as const, value: tenant } } }
	const sets = usePolled(SetService.method.list, { filters: [filter], size: 200 }, 15000)
	const producers = usePolled(ProducerService.method.list, { filters: [filter], size: 200 })
	if (sets.state === 'error') return <Err error={sets.error} />
	if (sets.data === undefined) return <Loading />
	if (sets.data.items.length === 0) return <Empty>No set yet. A set is made with the CLI: `shale set add`, then a producer is adopted for it.</Empty>

	return (
		<>
			{sets.data.items.map((v) => (
				<SetBlock key={key(v.id)} set={v} producers={producers.data?.items.filter((p) => sameId(p.set?.id, v.id)) ?? []} />
			))}
		</>
	)
}

function SetBlock(props: { set: Set; producers: Producer[] }): ReactNode {
	const v = props.set
	const sources = usePolled(SourceService.method.list, { filters: [{ set: byId(v.id) }], size: 200 }, 10000)
	const producer = props.producers.find((p) => p.state === HostState.ADOPTED)
	const now = useNow()
	const seen = date(producer?.dateSeen)
	const alive = producer !== undefined && seen !== undefined && now - seen.getTime() <= DOWN_AFTER_MS
	const reports = new Map<string, SourceReport>()
	for (const r of producer?.status?.sources ?? []) reports.set(hex(r.sourceId), r)

	return (
		<section>
			<h2>
				{v.alias}
				{v.name !== '' && <span className="dim"> · {v.name}</span>}
			</h2>
			<div className="toolbar">
				{producer === undefined ? (
					<Badge tone="warn">no producer adopted</Badge>
				) : (
					<>
						<Badge tone={alive ? 'ok' : 'bad'}>{alive ? 'producer up' : 'producer down'}</Badge>
						<span className="dim">
							{producer.hostname} · seen <Ago at={producer.dateSeen} />
							{producer.status?.load !== undefined && (
								<>
									{' '}
									· cpu {producer.status.load.cpu.toFixed(2)}
									{producer.status.load.temperature > 0 && ` · ${producer.status.load.temperature.toFixed(0)} °C`}
									{producer.status.load.uplinkBps > 0n && ` · ${bps(producer.status.load.uplinkBps)} up`}
								</>
							)}
						</span>
					</>
				)}
				<span className="dim">
					{v.link?.mode === UploadMode.BUFFERED ? 'buffered' : 'live'} upload
					{v.maxBitrateTotal > 0n && ` · ${bps(v.maxBitrateTotal)} in all`}
				</span>
				<a href={`#/live/${hex(v.id)}`}>watch live</a>
				<a href={`#/segments/${hex(v.id)}`}>segments</a>
			</div>
			{sources.state === 'error' ? (
				<Err error={sources.error} />
			) : sources.data === undefined ? (
				<Loading />
			) : sources.data.items.length === 0 ? (
				<Empty>No source: the producer registers its cameras when it is adopted (§38.4).</Empty>
			) : (
				<div className="cards">
					{[...sources.data.items]
						.sort((a, b) => a.ordinal - b.ordinal)
						.map((src) => (
							<Camera key={key(src.id)} source={src} report={reports.get(hex(src.id))} producerUp={alive} />
						))}
				</div>
			)}
		</section>
	)
}

function Camera(props: { source: Source; report: SourceReport | undefined; producerUp: boolean }): ReactNode {
	const s = props.source
	const r = props.report
	const ceiling = Number(r?.maxBitrate ?? s.profile?.maxBitrate ?? 0n)
	const rate = Number(r?.measuredBitrate ?? 0n)
	let state: { tone: Tone; text: string }
	if (!props.producerUp) state = { tone: 'bad', text: 'producer down' }
	else if (r === undefined) state = { tone: 'dim', text: 'not reported' }
	else if (!r.inputUp) state = { tone: 'bad', text: 'input down' }
	else if (r.dark) state = { tone: 'info', text: 'dark: not stored' }
	else state = { tone: 'ok', text: 'recording' }
	const video = s.contentType === '' || s.contentType.startsWith('video/')
	const share = ceiling > 0 ? rate / ceiling : 0

	return (
		<div className="card">
			<div className="title">
				<b>
					{s.alias}
					<span className="dim"> #{s.ordinal}</span>
				</b>
				<Badge tone={state.tone}>{state.text}</Badge>
			</div>
			<dl>
				{!video && (
					<>
						<dt>content</dt>
						<dd className="mono">{s.contentType}</dd>
					</>
				)}
				<dt>rate</dt>
				<dd>
					<Bar value={rate} of={ceiling} tone={share > 0.95 ? 'warn' : 'ok'} title={`${bps(rate)} of ${bps(ceiling)}`} /> {bps(rate)}
					<span className="dim"> of {bps(ceiling)}</span>
				</dd>
				{video && (
					<>
						<dt>frames</dt>
						<dd>
							{r === undefined ? '-' : `${r.frameRate.toFixed(1)} fps`}
							{r !== undefined && r.keyframeIntervalMs > 0n && <span className="dim"> · keyframe every {seconds(Number(r.keyframeIntervalMs) / 1000)}</span>}
						</dd>
					</>
				)}
				<dt>segment</dt>
				<dd>
					{s.profile === undefined ? '-' : `${seconds(Number(s.profile.durationSeconds))} · ${bytes((s.profile.maxBitrate * s.profile.durationSeconds) / 8n)} at most`}
				</dd>
				{r !== undefined && (r.captureRestarts > 0n || r.earlyCuts > 0n) && (
					<>
						<dt>trouble</dt>
						<dd className="warn">
							{r.captureRestarts > 0n && `${r.captureRestarts} restarts`}
							{r.captureRestarts > 0n && r.earlyCuts > 0n && ' · '}
							{r.earlyCuts > 0n && `${r.earlyCuts} early cuts`}
						</dd>
					</>
				)}
				{r !== undefined && r.error !== '' && (
					<>
						<dt>last error</dt>
						<dd className="bad mono">{r.error}</dd>
					</>
				)}
				{s.starvation?.starved && (
					<>
						<dt>suggestion</dt>
						<dd className="warn">
							starved at its ceiling: raise to {bps(s.starvation.suggestedMaxBitrate > 0n ? s.starvation.suggestedMaxBitrate : BigInt(Math.round(ceiling * 1.25)))}
						</dd>
					</>
				)}
			</dl>
		</div>
	)
}
