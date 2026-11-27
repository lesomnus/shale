/**
 * Tokens: a person's API tokens for scripts -- keys roster mints them for
 * the app Shale (§33.1) -- made, seen and ended here by the person for
 * themselves. A token is shown once, when it is made, and is kept nowhere
 * here: not in the address, not in the browser's storage, not in a log;
 * only in this page's memory until *done* or the page goes.
 *
 * Where roster is elsewhere, making one is saying yes at the identity
 * service: the page opens its address in a new tab and asks every few
 * seconds whether the yes was said. Where roster runs inside Shale the
 * token is made at once.
 *
 * A token reaches every camera its person sees, or only those of one of
 * their lists of cameras (a Reach, §33.1): it carries the list by its
 * identifier, and may then do only what a list confines -- view and read
 * those cameras. The lists are shown and managed here too: what each names
 * and which tokens carry it; changing one changes what they reach, and
 * erasing one leaves them reaching nothing.
 *
 * @module
 */

import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'

import { ConnectError } from '@connectrpc/connect'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { useCall } from '@lesomnus/payday/react'

import { HolderKeyState, HolderService, type HolderKey, type HolderListKeysResponse } from '../gen/shale/payday/holder_svc_pb.js'
import type { Reach } from '../gen/shale/reach_pb.js'
import { ReachService } from '../gen/shale/reach_svc_pb.js'
import type { Source } from '../gen/shale/set_pb.js'
import { SourceService } from '../gen/shale/set_svc_pb.js'
import type { Site } from '../gen/shale/site_pb.js'
import { SiteService } from '../gen/shale/site_svc_pb.js'

import { On } from './surface.js'
import { Ago, Empty, Loading, When, byId, confirmThen, hex } from './ui.js'

export function Tokens(): ReactNode {
	return (
		<>
			<h1>Tokens</h1>
			<p className="lede">
				A token lets a script call Shale as you, with what you let it do here, until it ends or you end it. It is shown once, when it is
				made: keep it in the script's secret store.
			</p>
			<On surface="tenant" note="Tokens are your own: sign in to the tenant API.">
				<Page />
			</On>
		</>
	)
}

/** said is what the server said, in its words. */
function said(err: unknown): string {
	return err instanceof ConnectError ? err.rawMessage : String(err)
}

/** NAME is roster's rule for a key's name: an alias. */
const NAME = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/

/** serviceOf is a method's service, as people read it: `Set`, `Lamina`. */
function serviceOf(m: string): string {
	return /^\/shale\.([A-Za-z]+?)(?:Service)?\//.exec(m)?.[1] ?? m
}

/** verbOf is a method's own name. */
function verbOf(m: string): string {
	return m.slice(m.lastIndexOf('/') + 1)
}

function same(a: readonly string[], b: readonly string[]): boolean {
	if (a.length !== b.length) return false
	const s = new Set(a)

	return b.every((v) => s.has(v))
}

const DAY = 24 * 3600 * 1000

type Ends = '30' | '90' | '365' | 'date' | 'never'
type What = 'reads' | 'all' | 'choose'

function Page(): ReactNode {
	const list = useCall(HolderService.method.listKeys)
	const reaches = useCall(ReachService.method.list)
	const sites = useCall(SiteService.method.list)
	const sources = useCall(SourceService.method.list)
	const [made, setMade] = useState<string | null>(null)
	const reload = (): void => {
		list.call({}).catch(() => {})
		reaches.call({ size: 100 }).catch(() => {})
	}
	useEffect(() => {
		reload()
		sites.call({ size: 500 }).catch(() => {})
		sources.call({ size: 500 }).catch(() => {})
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [])
	const v = list.data
	const seen: Seen = { lists: reaches.data?.items ?? [], sites: sites.data?.items ?? [], sources: sources.data?.items ?? [] }

	return (
		<>
			{made !== null && <Made token={made} done={() => setMade(null)} />}
			<h2>Make a token</h2>
			{v === undefined ? (
				list.state === 'error' ? (
					<p className="bad">{said(list.error)}</p>
				) : (
					<Loading />
				)
			) : v.holds.length === 0 && v.noneHeld !== '' ? (
				<p className="warn none-held">You cannot make a token here. {v.noneHeld}</p>
			) : v.holds.length === 0 ? (
				<p className="warn none-held">
					You hold no role of Shale's, so you cannot make a token: a token can do only what you hold here. Ask an administrator for a role of
					shale's.
				</p>
			) : (
				<Make
					held={v}
					seen={seen}
					made={(token) => {
						setMade(token)
						reload()
					}}
					listed={reload}
				/>
			)}

			<h2>Your tokens</h2>
			{v === undefined ? null : v.keys.length === 0 ? <Empty>You have no tokens.</Empty> : <List v={v} seen={seen} ended={reload} />}

			<h2>Your lists of cameras</h2>
			<p className="dim">
				A token that carries a list reaches only the cameras it names, of those you see. Changing a list changes what every token that carries it
				reaches; erasing one leaves them reaching nothing.
			</p>
			{reaches.data === undefined ? (
				reaches.state === 'error' ? (
					<p className="bad">{said(reaches.error)}</p>
				) : (
					<Loading />
				)
			) : reaches.data.items.length === 0 ? (
				<Empty>You have no lists of cameras. Make one when you make a token.</Empty>
			) : (
				<Lists seen={seen} keys={v?.keys ?? []} remembered={Number(v?.remembered?.seconds ?? 30n)} changed={reload} />
			)}
		</>
	)
}

/** Seen is what the person sees, for choosing cameras: their lists, the sites and the cameras. */
type Seen = { lists: Reach[]; sites: Site[]; sources: Source[] }

/** Picked is what a list names, by identifier as the page writes one (hex). */
type Picked = { sites: ReadonlySet<string>; sources: ReadonlySet<string> }

const NOTHING: Picked = { sites: new Set(), sources: new Set() }

function pickedOf(r: Reach | undefined): Picked {
	return { sites: new Set((r?.cameras?.sites ?? []).map(hex)), sources: new Set((r?.cameras?.sources ?? []).map(hex)) }
}

/** camerasOf is a pick as the server takes it. */
function camerasOf(p: Picked, seen: Seen): { sites: Uint8Array[]; sources: Uint8Array[] } {
	return {
		sites: seen.sites.filter((v) => p.sites.has(hex(v.id))).map((v) => v.id),
		sources: seen.sources.filter((v) => p.sources.has(hex(v.id))).map((v) => v.id),
	}
}

function siteName(v: Site): string {
	return v.name !== '' ? v.name : v.alias
}

function cameraName(v: Source): string {
	return v.name !== '' ? v.name : v.alias
}

/** named is what a list names, in words: its sites and its cameras by name. */
function named(r: Reach, seen: Seen): string[] {
	const out: string[] = []
	for (const id of r.cameras?.sites ?? []) {
		const v = seen.sites.find((w) => hex(w.id) === hex(id))
		out.push(`every camera of ${v === undefined ? 'a site you no longer see' : siteName(v)}`)
	}
	for (const id of r.cameras?.sources ?? []) {
		const v = seen.sources.find((w) => hex(w.id) === hex(id))
		out.push(v === undefined ? 'a camera you no longer see' : cameraName(v))
	}

	return out
}

/** Cameras picks sites and cameras of what the person sees, shown by site with each camera's name. */
function Cameras(props: { seen: Seen; picked: Picked; pick: (p: Picked) => void }): ReactNode {
	const { seen, picked } = props
	const groups = useMemo(() => {
		const m = new Map<string, Source[]>()
		for (const v of seen.sources) {
			const k = hex(v.site?.id)
			m.set(k, [...(m.get(k) ?? []), v])
		}
		const out: { site: Site | undefined; sources: Source[] }[] = seen.sites.map((site) => ({ site, sources: m.get(hex(site.id)) ?? [] }))
		const loose = m.get('')
		if (loose !== undefined) out.push({ site: undefined, sources: loose })

		return out
	}, [seen.sites, seen.sources])
	const flip = (which: 'sites' | 'sources', id: string, on: boolean): void => {
		const n = new Set(picked[which])
		if (on) n.add(id)
		else n.delete(id)
		props.pick({ ...picked, [which]: n })
	}
	if (groups.length === 0) return <p className="dim">You see no cameras.</p>

	return (
		<div className="cameras">
			{groups.map((g) => {
				const site = g.site === undefined ? '' : hex(g.site.id)
				const whole = site !== '' && picked.sites.has(site)
				return (
					<div key={site} className="site">
						{g.site === undefined ? (
							<b>in no site</b>
						) : (
							<label className="whole">
								<input type="checkbox" checked={whole} onChange={(e) => flip('sites', site, e.target.checked)} /> <b>{siteName(g.site)}</b>{' '}
								<span className="dim">· every camera of it, and those added to it later</span>
							</label>
						)}
						{g.sources.map((v) => (
							<label key={hex(v.id)} className="camera">
								<input
									type="checkbox"
									checked={whole || picked.sources.has(hex(v.id))}
									disabled={whole}
									onChange={(e) => flip('sources', hex(v.id), e.target.checked)}
								/>{' '}
								{cameraName(v)}
							</label>
						))}
						{g.sources.length === 0 && <span className="dim"> no cameras yet</span>}
					</div>
				)
			})}
		</div>
	)
}

/** Made is a token, this once, with what a script writes. */
function Made(props: { token: string; done: () => void }): ReactNode {
	const [copied, setCopied] = useState<string | null>(null)
	const line = `export SHALE_CLIENT_TOKEN=${props.token}`
	const copy = (v: string, what: string): void => {
		navigator.clipboard.writeText(v).then(
			() => setCopied(what),
			() => setCopied('could not copy: select it and copy it by hand'),
		)
	}

	return (
		<div className="card token">
			<div className="title">
				<b>Your new token</b>
			</div>
			<p>This is the only time it is shown: copy it now. Shale does not keep it, and nobody can show it to you again.</p>
			<code className="key">{props.token}</code>
			<div className="toolbar">
				<button type="button" className="primary" onClick={() => copy(props.token, 'copied the token')}>
					copy
				</button>
				{copied !== null && <span className="dim">{copied}</span>}
			</div>
			<p className="dim">A script hands it to the command line as:</p>
			<code className="key">{line}</code>
			<div className="toolbar">
				<button type="button" onClick={() => copy(line, 'copied the line')}>
					copy the line
				</button>
				<button type="button" onClick={props.done}>
					done
				</button>
			</div>
		</div>
	)
}

type Which = 'all' | 'list'

/** NARROWED is why a token of a list is offered only what it is. */
const NARROWED =
	'A token of a list of cameras may only view and read those cameras: one that could change things, or read anything else, would not be narrowed to them, so Shale refuses it everything else and nothing else is offered.'

/** Make is the form, and what happens after *make*. */
function Make(props: { held: HolderListKeysResponse; seen: Seen; made: (token: string) => void; listed: () => void }): ReactNode {
	const begin = useCall(HolderService.method.beginKey)
	const add = useCall(ReachService.method.add)
	const seen = props.seen
	const [which, setWhich] = useState<Which>('all')
	const [reach, setReach] = useState<string>('new')
	const [listName, setListName] = useState('')
	const [picked, setPicked] = useState<Picked>(NOTHING)
	const narrowed = which === 'list'
	const holds = narrowed ? props.held.narrowed : props.held.holds
	const reads = narrowed ? props.held.reads.filter((m) => props.held.narrowed.includes(m)) : props.held.reads
	const [name, setName] = useState('')
	const [what, setWhat] = useState<What>(reads.length > 0 ? 'reads' : 'all')
	const [chosen, setChosen] = useState<ReadonlySet<string>>(new Set())
	const [ends, setEnds] = useState<Ends>('90')
	const [date, setDate] = useState('')
	const [waiting, setWaiting] = useState<{ at: string; code: string; handle: string; every: number } | null>(null)
	const [outcome, setOutcome] = useState<string | null>(null)
	const [error, setError] = useState<string | null>(null)

	// A list chosen once the lists arrive: the first, if there is one.
	useEffect(() => {
		const first = seen.lists[0]
		if (reach === 'new' && first !== undefined && which === 'all') setReach(hex(first.id))
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [seen.lists.length])

	const byService = useMemo(() => {
		const m = new Map<string, string[]>()
		for (const v of holds) {
			const s = serviceOf(v)
			m.set(s, [...(m.get(s) ?? []), v])
		}

		return [...m.entries()]
	}, [holds])

	// View and read where there is something to read; else everything it may.
	const how: What = what === 'reads' && reads.length === 0 ? 'all' : what
	const methods = how === 'reads' ? reads : how === 'all' ? holds : [...chosen].filter((m) => holds.includes(m))
	const nameOk = name !== '' && name.length <= 63 && NAME.test(name)
	const end = ((): Date | null | undefined => {
		switch (ends) {
			case '30':
			case '90':
			case '365':
				return new Date(Date.now() + Number(ends) * DAY)
			case 'never':
				return null
			case 'date': {
				if (date === '') return undefined
				// The end of the day chosen, here.
				const d = new Date(`${date}T23:59:59`)

				return Number.isNaN(d.getTime()) || d.getTime() <= Date.now() ? undefined : d
			}
		}
	})()
	const newList = narrowed && reach === 'new'
	const listOk = !narrowed || (newList ? listName.trim() !== '' && picked.sites.size + picked.sources.size > 0 : seen.lists.some((r) => hex(r.id) === reach))
	const ready = nameOk && methods.length > 0 && end !== undefined && listOk && waiting === null && begin.state !== 'pending' && add.state !== 'pending'

	const toggle = (m: string, on: boolean): void => {
		const n = new Set(chosen)
		if (on) {
			n.add(m)
			// A key that watches names Get beside Watch: each row a stream
			// sends is read as a Get (§33.1).
			if (verbOf(m) === 'Watch') {
				const get = m.replace(/\/Watch$/, '/Get')
				if (holds.includes(get)) n.add(get)
			}
		} else n.delete(m)
		setChosen(n)
	}

	const make = async (): Promise<void> => {
		setError(null)
		setOutcome(null)
		try {
			let scope = ''
			if (narrowed) {
				if (newList) {
					const r = await add.call({ name: listName.trim(), cameras: camerasOf(picked, seen) })
					scope = hex(r.id)
					setReach(scope)
					setListName('')
					setPicked(NOTHING)
					props.listed()
				} else scope = reach
			}
			const r = await begin.call({
				name,
				methods,
				scope,
				...(end === null || end === undefined ? {} : { dateExpired: timestampFromDate(end) }),
			})
			if (r.token !== '') {
				props.made(r.token)
				setName('')

				return
			}
			setWaiting({ at: r.approveAt, code: r.userCode, handle: r.handle, every: Number(r.interval?.seconds ?? 5n) * 1000 || 5000 })
			window.open(r.approveAt, '_blank', 'noopener,noreferrer')
		} catch (err: unknown) {
			setError(said(err))
		}
	}

	return (
		<div className="card make">
			<label>
				name
				<input value={name} onChange={(e) => setName(e.target.value)} placeholder="backup-script" maxLength={63} />
			</label>
			{name !== '' && !nameOk && <p className="warn">Small letters, digits and single hyphens, beginning with a letter, such as backup-01.</p>}

			<fieldset className="which">
				<legend>which cameras</legend>
				<label>
					<input type="radio" name="which" checked={which === 'all'} onChange={() => setWhich('all')} /> All I can see
				</label>
				<label>
					<input type="radio" name="which" checked={which === 'list'} onChange={() => setWhich('list')} /> A list of cameras
				</label>
				{narrowed && (
					<div className="choose">
						<select value={reach} onChange={(e) => setReach(e.target.value)}>
							{seen.lists.map((r) => (
								<option key={hex(r.id)} value={hex(r.id)}>
									{r.name}
								</option>
							))}
							<option value="new">a new list…</option>
						</select>
						{!newList &&
							((): ReactNode => {
								const r = seen.lists.find((w) => hex(w.id) === reach)
								return r === undefined ? null : <span className="dim">{named(r, seen).join(', ') || 'names nothing, so reaches nothing'}</span>
							})()}
						{newList && (
							<div className="new-list">
								<label>
									the list's name
									<input value={listName} onChange={(e) => setListName(e.target.value)} placeholder="gate cameras" maxLength={128} />
								</label>
								<Cameras seen={seen} picked={picked} pick={setPicked} />
							</div>
						)}
						<p className="dim why-narrowed">{NARROWED}</p>
					</div>
				)}
			</fieldset>

			<fieldset>
				<legend>what it may do</legend>
				{reads.length > 0 && (
					<label>
						<input type="radio" checked={how === 'reads'} onChange={() => setWhat('reads')} /> View and read{' '}
						<span className="dim">
							{narrowed
								? `· these cameras, live, their recordings and their export (${reads.length} methods)`
								: reads.some((m) => m.startsWith('/shale.ProducerService/') || m.startsWith('/shale.ReaderService/'))
									? `· cameras, live, recordings and their export, sites, producers and readers, as the console reads them (${reads.length} methods)`
									: `· cameras, live, recordings and their export, and sites, as everybody may (${reads.length} methods)`}
						</span>
					</label>
				)}
				<label>
					<input type="radio" checked={how === 'all'} onChange={() => setWhat('all')} />{' '}
					{narrowed ? 'Everything a token of cameras may do' : 'Everything I may do here'}{' '}
					<span className="dim">
						{narrowed ? `· of what you hold, all a list of cameras confines (${holds.length} methods)` : `· what your roles of Shale's give you (${holds.length} methods)`}
					</span>
				</label>
				<label>
					<input type="radio" checked={how === 'choose'} onChange={() => setWhat('choose')} /> Choose
				</label>
				{how === 'choose' && (
					<div className="choose">
						{byService.map(([service, ms]) => (
							<div key={service}>
								<b>{service}</b>{' '}
								{ms.map((m) => (
									<label key={m} className="method">
										<input type="checkbox" checked={chosen.has(m)} onChange={(e) => toggle(m, e.target.checked)} /> {verbOf(m)}
									</label>
								))}
							</div>
						))}
						<p className="dim">
							{narrowed ? 'Only what you hold of what a list of cameras confines is offered.' : 'Only what you hold is offered.'} A token that watches is
							given Get beside Watch, since each row a watch sends is read as a Get.
						</p>
					</div>
				)}
				{narrowed && holds.length === 0 && <p className="warn">You hold nothing a token of cameras may do: it views and reads cameras alone.</p>}
			</fieldset>

			<fieldset>
				<legend>when it ends</legend>
				<select value={ends} onChange={(e) => setEnds(e.target.value as Ends)}>
					<option value="30">in 30 days</option>
					<option value="90">in 90 days</option>
					<option value="365">in a year</option>
					<option value="date">on a date</option>
					<option value="never">never</option>
				</select>{' '}
				{ends === 'date' && <input type="date" value={date} onChange={(e) => setDate(e.target.value)} />}
				{ends === 'date' && date !== '' && end === undefined && <span className="warn"> a date after today</span>}
				{ends === 'never' && <span className="dim"> it works until you end it</span>}
			</fieldset>

			<div className="toolbar">
				<button type="button" className="primary" disabled={!ready} onClick={() => void make()}>
					make
				</button>
			</div>
			{error !== null && <p className="bad">{error}</p>}
			{outcome !== null && <p className="warn">{outcome}</p>}
			{waiting !== null && (
				<Waiting
					{...waiting}
					made={(token) => {
						setWaiting(null)
						setName('')
						props.made(token)
					}}
					over={(why) => {
						setWaiting(null)
						setOutcome(why)
					}}
				/>
			)}
		</div>
	)
}

/**
 * Waiting is the yes being asked for at the identity service: the address,
 * which was opened in a new tab, and the asking every few seconds.
 */
function Waiting(props: { at: string; code: string; handle: string; every: number; made: (token: string) => void; over: (why: string) => void }): ReactNode {
	const poll = useCall(HolderService.method.pollKey)
	const every = useRef(props.every)
	useEffect(() => {
		let live = true
		let t: ReturnType<typeof setTimeout> | undefined
		const ask = (): void => {
			poll.call({ handle: props.handle }).then(
				(r) => {
					if (!live) return
					switch (r.state) {
						case HolderKeyState.MADE:
							props.made(r.token)
							return
						case HolderKeyState.REFUSED:
							props.over(
								r.refusal !== ''
									? `The identity service would not make the token: ${r.refusal}. Nothing was made; make it again with what it would allow.`
									: 'You said no at the identity service, so no token was made. Make it again if you meant yes.',
							)
							return
						case HolderKeyState.LAPSED:
							props.over('Nobody said yes at the identity service in time, so no token was made. Make it again, and say yes within a quarter of an hour.')
							return
						default:
							if (r.interval !== undefined) every.current = Number(r.interval.seconds) * 1000 || every.current
							t = setTimeout(ask, every.current)
					}
				},
				(err: unknown) => {
					if (!live) return
					props.over(`${said(err)} -- make it again, or make one on the identity service's account page, for the app shale.`)
				},
			)
		}
		t = setTimeout(ask, every.current)

		return () => {
			live = false
			if (t !== undefined) clearTimeout(t)
		}
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [props.handle])

	return (
		<div className="waiting">
			<p>
				Say yes at the identity service:{' '}
				<a href={props.at} target="_blank" rel="noopener noreferrer">
					{props.at}
				</a>{' '}
				-- it opened in a new tab. It asks for the code <code>{props.code}</code>, and shows what the token may do and when it ends.
			</p>
			<p className="dim">Waiting for your yes…</p>
		</div>
	)
}

/** List is the person's tokens, with *end* on each. */
function List(props: { v: HolderListKeysResponse; seen: Seen; ended: () => void }): ReactNode {
	const v = props.v

	return (
		<div className="wrap">
			<table className="tokens">
				<thead>
					<tr>
						<th>name</th>
						<th>cameras</th>
						<th>may</th>
						<th>ends</th>
						<th>last used</th>
						<th>made</th>
						<th />
					</tr>
				</thead>
				<tbody>
					{v.keys.map((k) => (
						<Row key={hex(k.id)} k={k} v={v} seen={props.seen} ended={props.ended} />
					))}
				</tbody>
			</table>
		</div>
	)
}

/** Reaches is a token's cameras: all, or its list's name with what it names on demand, or that its list is gone. */
function Reaches(props: { k: HolderKey; seen: Seen }): ReactNode {
	const { k, seen } = props
	if (k.scope === '') return <>all</>
	const r = seen.lists.find((w) => hex(w.id) === k.scope)
	if (r === undefined) return <span className="warn gone">its list is gone, so it reaches nothing</span>
	const what = named(r, seen)

	return (
		<details>
			<summary>{r.name}</summary>
			{what.length === 0 ? <p className="dim">names nothing, so it reaches nothing</p> : (
				<ul className="methods">
					{what.map((w, i) => (
						<li key={i}>{w}</li>
					))}
				</ul>
			)}
		</details>
	)
}

function Row(props: { k: HolderKey; v: HolderListKeysResponse; seen: Seen; ended: () => void }): ReactNode {
	const { k, v } = props
	const end = useCall(HolderService.method.endKey)
	const remembered = Number(v.remembered?.seconds ?? 30n)
	const preset =
		v.reads.length > 0 && same(k.methods, v.reads)
			? 'View and read'
			: same(k.methods, v.holds)
				? 'Everything I may do here'
				: k.scope !== '' && v.narrowed.length > 0 && same(k.methods, v.narrowed)
					? 'Everything a token of cameras may do'
					: k.scope !== '' && same(k.methods, v.reads.filter((m) => v.narrowed.includes(m)))
						? 'View and read'
						: null

	return (
		<tr>
			<td className="mono">{k.name}</td>
			<td>
				<Reaches k={k} seen={props.seen} />
			</td>
			<td>
				{preset ?? (
					<details>
						<summary>
							{k.methods.length} {k.methods.length === 1 ? 'method' : 'methods'}
						</summary>
						<ul className="methods">
							{k.methods.map((m) => (
								<li key={m} className="mono">
									{m}
								</li>
							))}
						</ul>
					</details>
				)}
			</td>
			<td>{k.dateExpired === undefined ? 'never' : <When at={k.dateExpired} />}</td>
			<td>
				<Ago at={k.dateUsed} />
			</td>
			<td>
				<When at={k.dateCreated} />
			</td>
			<td className="actions">
				<button
					type="button"
					className="danger"
					disabled={end.state === 'pending'}
					onClick={() =>
						confirmThen(`End the token ${k.name}? Scripts using it stop within ${remembered} seconds, and it cannot be used again.`, () =>
							end.call({ id: k.id }).then(props.ended, () => {}),
						)
					}
				>
					end
				</button>
				{end.state === 'error' && <p className="bad">{said(end.error)}</p>}
			</td>
		</tr>
	)
}

/** carriers are the names of the tokens that carry a list. */
function carriers(r: Reach, keys: readonly HolderKey[]): string[] {
	return keys.filter((k) => k.scope === hex(r.id)).map((k) => k.name)
}

/** Lists is the person's lists of cameras: what each names, which tokens carry it, *change* and *erase*. */
function Lists(props: { seen: Seen; keys: readonly HolderKey[]; remembered: number; changed: () => void }): ReactNode {
	return (
		<div className="wrap">
			<table className="lists">
				<thead>
					<tr>
						<th>name</th>
						<th>names</th>
						<th>carried by</th>
						<th />
					</tr>
				</thead>
				<tbody>
					{props.seen.lists.map((r) => (
						<ListRow key={hex(r.id)} r={r} {...props} />
					))}
				</tbody>
			</table>
		</div>
	)
}

function ListRow(props: { r: Reach; seen: Seen; keys: readonly HolderKey[]; remembered: number; changed: () => void }): ReactNode {
	const { r, seen } = props
	const patch = useCall(ReachService.method.patch)
	const erase = useCall(ReachService.method.erase)
	const [editing, setEditing] = useState(false)
	const [name, setName] = useState(r.name)
	const [picked, setPicked] = useState<Picked>(pickedOf(r))
	const what = named(r, seen)
	const by = carriers(r, props.keys)
	const tokens = by.length === 0 ? null : by.length === 1 ? `the token ${by[0]}` : `the tokens ${by.join(', ')}`

	const save = (): void => {
		const q =
			tokens === null
				? `Change the list ${r.name}? No token carries it.`
				: `Change the list ${r.name}? It changes what ${tokens} ${by.length === 1 ? 'reaches' : 'reach'}: ${by.length === 1 ? 'it follows' : 'they follow'} within ${props.remembered} seconds.`
		confirmThen(q, () =>
			patch
				.call({ ref: byId(r.id), name: name.trim(), cameras: camerasOf(picked, seen), dateUpdatedForce: true })
				.then(() => {
					setEditing(false)
					props.changed()
				})
				.catch(() => {}),
		)
	}

	return (
		<>
			<tr>
				<td>{r.name}</td>
				<td>
					{what.length === 0 ? (
						<span className="dim">nothing, so it reaches nothing</span>
					) : (
						<details>
							<summary>
								{what.length} {what.length === 1 ? 'entry' : 'entries'}
							</summary>
							<ul className="methods">
								{what.map((w, i) => (
									<li key={i}>{w}</li>
								))}
							</ul>
						</details>
					)}
				</td>
				<td>{by.length === 0 ? <span className="dim">no token</span> : by.join(', ')}</td>
				<td className="actions">
					<button
						type="button"
						onClick={() => {
							setName(r.name)
							setPicked(pickedOf(r))
							setEditing(!editing)
						}}
					>
						change
					</button>
					<button
						type="button"
						className="danger"
						disabled={erase.state === 'pending'}
						onClick={() =>
							confirmThen(
								tokens === null ? `Erase the list ${r.name}? No token carries it.` : `Erase the list ${r.name}? T${tokens.slice(1)} will then reach nothing.`,
								() => erase.call(byId(r.id)).then(props.changed, () => {}),
							)
						}
					>
						erase
					</button>
					{erase.state === 'error' && <p className="bad">{said(erase.error)}</p>}
				</td>
			</tr>
			{editing && (
				<tr className="editing">
					<td colSpan={4}>
						<div className="card make">
							<label>
								the list's name
								<input value={name} onChange={(e) => setName(e.target.value)} maxLength={128} />
							</label>
							<Cameras seen={seen} picked={picked} pick={setPicked} />
							<div className="toolbar">
								<button type="button" className="primary" disabled={name.trim() === '' || patch.state === 'pending'} onClick={save}>
									save
								</button>
								<button type="button" onClick={() => setEditing(false)}>
									cancel
								</button>
							</div>
							{patch.state === 'error' && <p className="bad">{said(patch.error)}</p>}
						</div>
					</td>
				</tr>
			)}
		</>
	)
}
