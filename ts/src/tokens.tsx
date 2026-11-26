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
 * @module
 */

import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'

import { ConnectError } from '@connectrpc/connect'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { useCall } from '@lesomnus/payday/react'

import { HolderKeyState, HolderService, type HolderKey, type HolderListKeysResponse } from '../gen/shale/payday/holder_svc_pb.js'

import { On } from './surface.js'
import { Ago, Empty, Loading, When, confirmThen, hex } from './ui.js'

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
	const [made, setMade] = useState<string | null>(null)
	const reload = (): void => {
		list.call({}).catch(() => {})
	}
	useEffect(reload, [])
	const v = list.data

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
					made={(token) => {
						setMade(token)
						reload()
					}}
				/>
			)}

			<h2>Your tokens</h2>
			{v === undefined ? null : v.keys.length === 0 ? <Empty>You have no tokens.</Empty> : <List v={v} ended={reload} />}
		</>
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

/** Make is the form, and what happens after *make*. */
function Make(props: { held: HolderListKeysResponse; made: (token: string) => void }): ReactNode {
	const begin = useCall(HolderService.method.beginKey)
	const holds = props.held.holds
	const reads = props.held.reads
	const [name, setName] = useState('')
	const [what, setWhat] = useState<What>(reads.length > 0 ? 'reads' : 'all')
	const [chosen, setChosen] = useState<ReadonlySet<string>>(new Set())
	const [ends, setEnds] = useState<Ends>('90')
	const [date, setDate] = useState('')
	const [waiting, setWaiting] = useState<{ at: string; code: string; handle: string; every: number } | null>(null)
	const [outcome, setOutcome] = useState<string | null>(null)
	const [error, setError] = useState<string | null>(null)

	const byService = useMemo(() => {
		const m = new Map<string, string[]>()
		for (const v of holds) {
			const s = serviceOf(v)
			m.set(s, [...(m.get(s) ?? []), v])
		}

		return [...m.entries()]
	}, [holds])

	const methods = what === 'reads' ? reads : what === 'all' ? holds : [...chosen]
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
	const ready = nameOk && methods.length > 0 && end !== undefined && waiting === null && begin.state !== 'pending'

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

	const make = (): void => {
		setError(null)
		setOutcome(null)
		begin
			.call({
				name,
				methods,
				...(end === null || end === undefined ? {} : { dateExpired: timestampFromDate(end) }),
			})
			.then((r) => {
				if (r.token !== '') {
					props.made(r.token)
					setName('')

					return
				}
				setWaiting({ at: r.approveAt, code: r.userCode, handle: r.handle, every: Number(r.interval?.seconds ?? 5n) * 1000 || 5000 })
				window.open(r.approveAt, '_blank', 'noopener,noreferrer')
			})
			.catch((err: unknown) => setError(said(err)))
	}

	return (
		<div className="card make">
			<label>
				name
				<input value={name} onChange={(e) => setName(e.target.value)} placeholder="backup-script" maxLength={63} />
			</label>
			{name !== '' && !nameOk && <p className="warn">Small letters, digits and single hyphens, beginning with a letter, such as backup-01.</p>}

			<fieldset>
				<legend>what it may do</legend>
				{reads.length > 0 && (
					<label>
						<input type="radio" checked={what === 'reads'} onChange={() => setWhat('reads')} /> View and read{' '}
						<span className="dim">
							· cameras, live, recordings and their export, sites, producers and readers, as the console reads them ({reads.length} methods)
						</span>
					</label>
				)}
				<label>
					<input type="radio" checked={what === 'all'} onChange={() => setWhat('all')} /> Everything I may do here{' '}
					<span className="dim">· what your roles of Shale's give you ({holds.length} methods)</span>
				</label>
				<label>
					<input type="radio" checked={what === 'choose'} onChange={() => setWhat('choose')} /> Choose
				</label>
				{what === 'choose' && (
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
						<p className="dim">Only what you hold is offered. A token that watches is given Get beside Watch, since each row a watch sends is read as a Get.</p>
					</div>
				)}
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
				<button type="button" className="primary" disabled={!ready} onClick={make}>
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
function List(props: { v: HolderListKeysResponse; ended: () => void }): ReactNode {
	const v = props.v

	return (
		<div className="wrap">
			<table>
				<thead>
					<tr>
						<th>name</th>
						<th>may</th>
						<th>ends</th>
						<th>last used</th>
						<th>made</th>
						<th />
					</tr>
				</thead>
				<tbody>
					{v.keys.map((k) => (
						<Row key={hex(k.id)} k={k} v={v} ended={props.ended} />
					))}
				</tbody>
			</table>
		</div>
	)
}

function Row(props: { k: HolderKey; v: HolderListKeysResponse; ended: () => void }): ReactNode {
	const { k, v } = props
	const end = useCall(HolderService.method.endKey)
	const remembered = Number(v.remembered?.seconds ?? 30n)
	const preset = v.reads.length > 0 && same(k.methods, v.reads) ? 'View and read' : same(k.methods, v.holds) ? 'Everything I may do here' : null

	return (
		<tr>
			<td className="mono">{k.name}</td>
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
