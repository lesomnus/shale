/**
 * Cameras: every set of the tenant, its sources, and what its producer's
 * heartbeat says about each (§38.6): up or down, the frame rate, the measured
 * rate against the ceiling, a scene gone dark (§38.10), the suggestion a
 * starved source earns with the raise auto_raise applied or the limit that
 * held it (§38.5), and why cameras the producer's catalog names are not
 * recording yet. Above them, the camera jobs that stopped and wait on an
 * operator, with why: a TLS key a job's pin does not name is shown beside
 * that pin (docs/camera-tls-identity.md). And the cameras seen at another
 * address, with what checking their key there came to
 * (docs/camera-worker.md, "Transport candidates"). Each camera with a camera
 * device shows its own clock as the worker last read it, against the
 * worker's (docs/camera-worker.md, "Camera clocks").
 *
 * @module
 */

import type { ReactNode } from 'react'

import { useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

import { CameraBinding as CameraBindingEntity } from '../gen/entities.js'
import {
	CameraJobStage,
	type CameraBinding,
	type CameraClockObservation,
	type CameraDevice,
	type CameraJob,
	type CameraObservedEndpoint,
	type CameraTransportCandidate,
} from '../gen/shale/camera_pb.js'
import { CameraBindingService, CameraDeviceService, CameraJobService } from '../gen/shale/camera_svc_pb.js'
import { HostState, type Producer } from '../gen/shale/host_pb.js'
import { ProducerService } from '../gen/shale/host_svc_pb.js'
import type { Set, Source } from '../gen/shale/set_pb.js'
import { AdmissionWaitReason, RaiseLimit, RateControl, UploadMode, type AdmissionWait, type RaiseBlocked } from '../gen/shale/common_pb.js'
import { SetService, SourceService } from '../gen/shale/set_svc_pb.js'
import type { SourceReport } from '../gen/shale/common_pb.js'

import { tenantOf } from './session.js'
import { On, useSurfaces } from './surface.js'
import { Ago, Badge, Bar, Empty, Err, HOSTS_DENIED, Loading, bps, byId, bytes, date, denied, hex, sameId, seconds, short, useNow, usePolled, type Tone } from './ui.js'

const DOWN_AFTER_MS = 90_000

/** A camera clock further off than this from the worker's is a warning. */
const CLOCK_WARN_MS = 2_000

export function Cameras(): ReactNode {
	return (
		<>
			<h1>Cameras</h1>
			<p className="lede">
				A set is the cameras one producer records; what each camera is doing is what that producer said in its last heartbeat.
			</p>
			<On surface="tenant" note="Sets and cameras belong to a tenant: sign in to the tenant API.">
				<StoppedJobs />
				<MovedCameras />
				<Sets />
			</On>
		</>
	)
}

/**
 * StoppedJobs are the tenant's camera jobs waiting on an operator. Reading
 * them is a camera grant at roster, so a person without one sees nothing
 * here rather than an error.
 */
function StoppedJobs(): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const jobs = usePolled(CameraJobService.method.list, { filters: [{ tenant: { key: { case: 'alias' as const, value: tenant } } }], size: 500, reverse: true }, 15000)
	if (jobs.state === 'error' || jobs.data === undefined) return null
	const stopped = jobs.data.items.filter((j) => j.stage === CameraJobStage.WAITING)
	if (stopped.length === 0) return null

	return (
		<section>
			<h2>Camera jobs that stopped</h2>
			<div className="cards">
				{stopped.map((j) => (
					<StoppedJob key={key(j.id)} job={j} />
				))}
			</div>
		</section>
	)
}

/**
 * StoppedJob is one job and why it stopped. One that met a changed TLS key
 * shows the key it pins and the key the camera presents instead; nothing was
 * sent to the camera over it. Only `camera-policy approve-tls` with exactly
 * that key lets the camera's next job pin it.
 */
function StoppedJob(props: { job: CameraJob }): ReactNode {
	const j = props.job
	const binding = useRow<CameraBinding>(CameraBindingEntity.typeName, j.binding?.id)
	const progress = j.observed?.progress
	const changed = progress?.changedTlsIdentity
	// After its own firmware upgrade a job pins the key that upgrade accepted.
	const pinned = progress?.firmware?.complete === true && progress.firmware.tlsIdentity !== undefined ? progress.firmware.tlsIdentity : j.inputs?.deviceTlsIdentity

	return (
		<div className="card">
			<div className="title">
				<b>{binding?.sourceAlias || short(j.id)}</b>
				<Badge tone={changed === undefined ? 'warn' : 'bad'}>{changed === undefined ? `stopped: ${j.observed?.code || 'waiting'}` : 'TLS key changed'}</Badge>
			</div>
			<dl>
				<dt>job</dt>
				<dd className="mono">{hex(j.id)}</dd>
				<dt>since</dt>
				<dd>
					<Ago at={j.dateUpdated} />
				</dd>
				{changed !== undefined && (
					<>
						<dt>pinned key</dt>
						<dd className="mono key">{pinned?.spkiSha256 ?? '-'}</dd>
						<dt>presented key</dt>
						<dd className="mono key bad">{changed.spkiSha256}</dd>
						<dt>to approve</dt>
						<dd className="mono key dim">
							shale camera-policy approve-tls {hex(j.id)} '{`{"spkiSha256":"${changed.spkiSha256}","expectedNetworkVersion":"<the network's dateUpdated>"}`}'
						</dd>
					</>
				)}
			</dl>
		</div>
	)
}

/**
 * MovedCameras are the tenant's cameras the worker saw somewhere other than
 * their authorized endpoints. Nothing moved them but a verified key or an
 * operator; each candidate shows what checking it came to. Reading them is a
 * camera grant at roster, so a person without one sees nothing here.
 */
function MovedCameras(): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const devices = usePolled(CameraDeviceService.method.list, { filters: [{ tenant: { key: { case: 'alias' as const, value: tenant } } }], size: 500 }, 15000)
	if (devices.state === 'error' || devices.data === undefined) return null
	const moved = devices.data.items.filter((d) => (d.transport?.candidates.length ?? 0) > 0)
	if (moved.length === 0) return null

	return (
		<section>
			<h2>Cameras seen at another address</h2>
			<div className="cards">
				{moved.map((d) => (
					<MovedCamera key={key(d.id)} device={d} />
				))}
			</div>
		</section>
	)
}

/** transportWhy says, for each fixed outcome code, what it means. */
const transportWhy: Record<string, string> = {
	verification_required: 'the worker is checking its key at the new address, without a credential',
	accepted: 'it presented its approved key there; a recovery job with the same pins moved it',
	insecure_http: 'its web transport is HTTP: there is no key to check',
	trust_missing: 'no key of this camera is approved: ordinary PKI does not tell one camera from another',
	tls_verification_skip: 'its policy skips TLS verification: there is no key to check',
	trust_mismatch: "the active policy approves another key than the camera's last job pinned",
	identity_missing: 'no authenticated serial and MAC to reinspect it by',
	recovery_unavailable: 'no published binding whose last job pinned a policy',
	policy_manual: "the network's active policy enrolls by hand",
	mutation_active: 'a job for this camera has not finished',
	evidence_conflict: 'its EPR or MAC contradicts what is known of this camera',
	ambiguous: 'it was seen at more than one new address',
	host_owned: 'another camera owns that address',
	key_mismatch: 'another key answered there: not this camera, and nothing but a TLS handshake reached it',
	expired: 'the observation expired before it was checked',
	assignment_changed: "the camera network's worker assignment changed while it was checked",
	trust_changed: "the network or the camera's approved key changed while it was checked",
	endpoints_changed: "the camera's endpoints changed while it was checked",
	job_changed: "the camera's last job changed while it was checked",
}

function hosts(eps: CameraObservedEndpoint[] | undefined): string {
	const out = new globalThis.Set<string>()
	for (const ep of eps ?? []) {
		try {
			out.add(new URL(ep.uri).hostname)
		} catch {
			out.add(ep.uri)
		}
	}
	return [...out].join(', ')
}

/**
 * MovedCamera is one camera and its candidates, newest first. One an operator
 * must decide shows the command that moves it: CameraDevice.Observe with the
 * device's current version and the candidate's endpoints.
 */
function MovedCamera(props: { device: CameraDevice }): ReactNode {
	const d = props.device
	const candidates = [...(d.transport?.candidates ?? [])].reverse()

	return (
		<div className="card">
			<div className="title">
				<b>{d.name || d.alias || d.deviceKey}</b>
				<span className="mono dim">{hosts(d.endpoints)}</span>
			</div>
			{candidates.map((c) => (
				<TransportCandidate key={c.id} device={d} candidate={c} />
			))}
		</div>
	)
}

function TransportCandidate(props: { device: CameraDevice; candidate: CameraTransportCandidate }): ReactNode {
	const c = props.candidate
	const now = useNow()
	const expires = date(c.dateExpires)
	const lapsed = c.state === 'pending' && expires !== undefined && expires.getTime() <= now
	const code = lapsed ? 'expired' : c.code
	const operator = c.state === 'operator_required' || lapsed
	const tone: Tone = c.state === 'accepted' ? 'ok' : operator ? 'bad' : 'warn'
	const label = c.state === 'accepted' ? 'moved' : operator ? 'operator action' : 'verifying'
	const approved = c.grant?.trust?.spkiSha256
	const endpoints = JSON.stringify(c.endpoints.map((ep) => ({ uri: ep.uri })))

	return (
		<dl>
			<dt>seen at</dt>
			<dd className="mono">
				{hosts(c.endpoints)} <Badge tone={tone}>{label}</Badge>
			</dd>
			<dt>why</dt>
			<dd>{transportWhy[code] ?? code}</dd>
			<dt>seen</dt>
			<dd>
				<Ago at={c.dateObserved} />
			</dd>
			{c.dateDecided !== undefined && (
				<>
					<dt>decided</dt>
					<dd>
						<Ago at={c.dateDecided} />
					</dd>
				</>
			)}
			{c.presentedSpkiSha256 !== '' && approved !== undefined && c.presentedSpkiSha256 !== approved && (
				<>
					<dt>approved key</dt>
					<dd className="mono key">{approved}</dd>
					<dt>presented key</dt>
					<dd className="mono key bad">{c.presentedSpkiSha256}</dd>
				</>
			)}
			{c.jobId.length > 0 && (
				<>
					<dt>recovery job</dt>
					<dd className="mono">{hex(c.jobId)}</dd>
				</>
			)}
			{operator && (
				<>
					<dt>to move it</dt>
					<dd className="mono key dim">
						shale camera-device observe {hex(props.device.id)} '{`{"expectedVersion":"<the device's dateUpdated>","endpoints":${endpoints}}`}'
					</dd>
				</>
			)}
		</dl>
	)
}

function Sets(): ReactNode {
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const filter = { tenant: { key: { case: 'alias' as const, value: tenant } } }
	const sets = usePolled(SetService.method.list, { filters: [filter], size: 200 }, 15000)
	const producers = usePolled(ProducerService.method.list, { filters: [filter], size: 200 })
	const devices = useSourceDevices(filter)
	if (sets.state === 'error') return <Err error={sets.error} />
	if (sets.data === undefined) return <Loading />
	if (sets.data.items.length === 0) return <Empty>No set yet. A set is made with the CLI: `shale set add`, then a producer is adopted for it.</Empty>

	// A person who holds only the basics reads the sets and their cameras
	// and not the producers (§33.1): the page says so once, and shows no
	// producer state rather than a producer that is not there.
	const hidden = denied(producers.error)

	return (
		<>
			{hidden && <p className="dim none-held">Whether each set's producer is up is not shown. {HOSTS_DENIED}</p>}
			{sets.data.items.map((v) => (
				<SetBlock
					key={key(v.id)}
					set={v}
					producers={hidden ? null : (producers.data?.items.filter((p) => sameId(p.set?.id, v.id)) ?? [])}
					devices={devices}
				/>
			))}
		</>
	)
}

/**
 * useSourceDevices maps each source a camera binding records to its camera
 * device, by the source's hex. Reading them is a camera grant at roster, so a
 * person without one gets an empty map and sees no clock rather than an error.
 */
function useSourceDevices(filter: { tenant: { key: { case: 'alias'; value: string } } }): Map<string, CameraDevice> {
	const devices = usePolled(CameraDeviceService.method.list, { filters: [filter], size: 500 }, 15000)
	const bindings = usePolled(CameraBindingService.method.list, { filters: [filter], size: 500 }, 15000)
	const out = new Map<string, CameraDevice>()
	if (devices.data === undefined || bindings.data === undefined) return out
	const byDevice = new Map(devices.data.items.map((d) => [hex(d.id), d]))
	for (const b of bindings.data.items) {
		const d = byDevice.get(hex(b.device?.id))
		if (d !== undefined && b.source !== undefined) out.set(hex(b.source.id), d)
	}

	return out
}

/** SetBlock is a set and its cameras; `producers` is null where the person may not read them. */
function SetBlock(props: { set: Set; producers: Producer[] | null; devices: Map<string, CameraDevice> }): ReactNode {
	const v = props.set
	const sources = usePolled(SourceService.method.list, { filters: [{ set: byId(v.id) }], size: 200 }, 10000)
	const hidden = props.producers === null
	const producer = props.producers?.find((p) => p.state === HostState.ADOPTED)
	const now = useNow()
	const seen = date(producer?.dateSeen)
	const alive = producer !== undefined && seen !== undefined && now - seen.getTime() <= DOWN_AFTER_MS
	const reports = new Map<string, SourceReport>()
	for (const r of producer?.status?.sources ?? []) reports.set(hex(r.sourceId), r)
	// Only what a producer still up says: a wait in an old heartbeat is not news.
	const wait = alive ? producer?.status?.admission : undefined
	const waiting = new Set((wait?.sourceIds ?? []).map((id) => hex(id)))

	return (
		<section>
			<h2>
				{v.alias}
				{v.name !== '' && <span className="dim"> · {v.name}</span>}
			</h2>
			<div className="toolbar">
				{hidden ? null : producer === undefined ? (
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
					{v.autoRaise && ' · starved ceilings raised by themselves'}
				</span>
				<a href={`#/live/${hex(v.id)}`}>watch live</a>
				<a href={`#/segments/${hex(v.id)}`}>segments</a>
			</div>
			{wait !== undefined && (
				<p className="warn">
					{waiting.size > 0 ? `${waiting.size} camera${waiting.size === 1 ? '' : 's'} waiting to start` : 'cameras waiting to start'}: {waitText(wait)}
					<span className="dim">
						{' '}
						· since <Ago at={wait.dateSince} />
					</span>
				</p>
			)}
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
							<Camera
								key={key(src.id)}
								source={src}
								report={reports.get(hex(src.id))}
								producerUp={hidden ? null : alive}
								wait={waiting.has(hex(src.id)) ? wait : undefined}
								autoRaise={v.autoRaise}
								device={props.devices.get(hex(src.id))}
							/>
						))}
				</div>
			)}
		</section>
	)
}

function Camera(props: {
	source: Source
	report: SourceReport | undefined
	/** Whether the producer is up; null where the person may not read it. */
	producerUp: boolean | null
	wait: AdmissionWait | undefined
	autoRaise: boolean
	device: CameraDevice | undefined
}): ReactNode {
	const s = props.source
	const r = props.report
	const starvation = s.starvation
	const ceiling = Number(r?.maxBitrate ?? s.profile?.maxBitrate ?? 0n)
	const rate = Number(r?.measuredBitrate ?? 0n)
	let state: { tone: Tone; text: string }
	if (props.producerUp === null) state = { tone: 'dim', text: 'state not shown' }
	else if (!props.producerUp) state = { tone: 'bad', text: 'producer down' }
	else if (r === undefined && props.wait !== undefined) state = { tone: 'warn', text: `waiting: ${waitShort(props.wait.reason)}` }
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
				{props.device !== undefined && (
					<>
						<dt>clock</dt>
						<CameraClock clock={props.device.clock} />
					</>
				)}
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
				{starvation?.starved && (
					<>
						<dt>suggestion</dt>
						<dd className="warn">
							starved at its ceiling: raise to {bps(starvation.suggestedMaxBitrate > 0n ? starvation.suggestedMaxBitrate : BigInt(Math.round(ceiling * 1.25)))}
							{starvation.blocked !== undefined ? (
								<>
									; held: {heldText(starvation.blocked)}
									<span className="dim">
										{' '}
										· since <Ago at={starvation.blocked.dateSince} />
									</span>
								</>
							) : (
								props.autoRaise && r !== undefined && <span className="dim">{notRaised(r)}</span>
							)}
						</dd>
					</>
				)}
				{starvation?.dateRaised !== undefined && (
					<>
						<dt>raised</dt>
						<dd>
							from {bps(starvation.raisedFrom)} to {bps(starvation.raisedMaxBitrate)} for starvation
							<span className="dim">
								{' '}
								· <Ago at={starvation.dateRaised} />
							</span>
						</dd>
					</>
				)}
			</dl>
		</div>
	)
}

/**
 * CameraClock is a camera's own clock as the worker last read it: how far it
 * is from the worker's, its mode and its time zone. Off by more than
 * CLOCK_WARN_MS, or a mode or zone it did not say, is a warning: a camera
 * with no time server to reach drifts, and Shale only reads its clock.
 */
function CameraClock(props: { clock: CameraClockObservation | undefined }): ReactNode {
	const c = props.clock
	if (c === undefined) return <dd className="dim">not read yet</dd>
	const off = c.offsetMs < 0n ? -c.offsetMs : c.offsetMs
	const warn = off > BigInt(CLOCK_WARN_MS) || c.mode === '' || c.tz === ''
	// The camera answers in whole seconds: the offset is right to within half
	// a second and half the round trip.
	const within = Number(c.rttMs) / 2 + 500

	return (
		<dd className={warn ? 'warn' : undefined} title={`${c.offsetMs} ms, ± ${within} ms`}>
			{clockOffset(c.offsetMs)} · {c.mode === '' ? 'mode unknown' : c.mode} · {c.tz === '' ? 'TZ unknown' : c.tz}
			<span className="dim">
				{' '}
				· read <Ago at={c.observedAt} />
			</span>
		</dd>
	)
}

/** clockOffset is an offset in milliseconds in the unit that reads best: "−147 d", "+13 s". */
function clockOffset(ms: bigint): string {
	const sign = ms < 0n ? '−' : '+'
	const abs = Number(ms < 0n ? -ms : ms)
	if (abs < 9_950) return `${sign}${(abs / 1000).toFixed(1)} s`
	if (abs < 59_500) return `${sign}${Math.round(abs / 1000)} s`
	if (abs < 3_570_000) return `${sign}${Math.round(abs / 60_000)} min`
	if (abs < 84_600_000) return `${sign}${Math.round(abs / 3_600_000)} h`

	return `${sign}${Math.round(abs / 86_400_000)} d`
}

/**
 * heldText says which limit keeps a starved camera's ceiling where it is
 * (§38.5), with what the raise needs against what the limit allows.
 */
function heldText(b: RaiseBlocked): string {
	switch (b.limit) {
		case RaiseLimit.UPLOAD_POLICY:
			return `the upload policy caps a ceiling at ${bps(b.available)}`
		case RaiseLimit.SET_BITRATE:
			return `the set's ceilings would sum to ${bps(b.required)}, and it allows ${bps(b.available)} in all`
		case RaiseLimit.UPLINK:
			return `recording and live would need ${bps(b.required)} (the ceilings × 1.2, × 2.2 when live runs always), and producer.uplink is ${bps(b.available)}`
		default:
			return 'a limit'
	}
}

/**
 * notRaised says why a set with auto_raise leaves a starved camera to a
 * person, when nothing holds it at a limit (§38.5); empty when it is only
 * waiting for its day.
 */
function notRaised(r: SourceReport): string {
	if (r.captureEpoch !== '') return ' · not raised by itself: its camera binding pins the ceiling'
	switch (r.rateControl) {
		case RateControl.VBR:
			return ' · raised at most once a day'
		case RateControl.CBR:
			return ' · not raised by itself: a CBR encoder spends its ceiling whatever the picture'
		case RateControl.COPY:
			return " · not raised by itself: the stream is the camera's own, so its rate is set on the camera"
		default:
			return ' · not raised by itself: the producer does not say how it encodes this camera'
	}
}

/** waitShort is a wait's reason in a word or two, for a camera's badge. */
function waitShort(reason: AdmissionWaitReason): string {
	switch (reason) {
		case AdmissionWaitReason.UPLINK:
			return 'uplink budget'
		case AdmissionWaitReason.BUFFER:
			return 'RAM budget'
		case AdmissionWaitReason.SET_BITRATE:
			return 'set budget'
		case AdmissionWaitReason.UPLOAD_POLICY:
			return 'upload policy'
		case AdmissionWaitReason.SOURCE_LIMIT:
			return 'too many cameras'
		case AdmissionWaitReason.STALE:
			return 'catalog changed'
		case AdmissionWaitReason.AUTHORIZATION:
			return 'refused'
		case AdmissionWaitReason.CEILING:
			return 'ceiling unknown'
		default:
			return 'see the producer log'
	}
}

/**
 * waitText says why cameras wait, with the amounts the producer reported
 * (§38.5): what admission needs against what it has, and which setting to
 * change. The producer keeps existing feeds and starts these when it fits.
 */
function waitText(w: AdmissionWait): string {
	switch (w.reason) {
		case AdmissionWaitReason.UPLINK:
			return `recording and live together need ${bps(w.required)} (the ceilings × 2.2), and producer.uplink is ${bps(w.available)}`
		case AdmissionWaitReason.BUFFER:
			return `their first segments need ${bytes(w.required)} of RAM, and ${bytes(w.available)} of producer.buffer is free`
		case AdmissionWaitReason.SET_BITRATE:
			return `the cameras need ${bps(w.required)}, and the set allows ${bps(w.available)} in all`
		case AdmissionWaitReason.UPLOAD_POLICY:
			return `a camera's ceiling is ${bps(w.required)}, and the upload policy allows ${bps(w.available)}`
		case AdmissionWaitReason.SOURCE_LIMIT:
			return `${w.required} cameras, and one assignment holds at most ${w.available}`
		case AdmissionWaitReason.STALE:
			return 'the catalog changed while it was admitted; the producer asks again'
		case AdmissionWaitReason.AUTHORIZATION:
			return "the control plane refused the producer's identity or scope"
		case AdmissionWaitReason.CEILING:
			return "a camera's ceiling could not be read from the camera"
		default:
			return "the producer's log says why"
	}
}
