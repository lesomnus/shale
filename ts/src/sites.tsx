/**
 * Sites: the sites the signed-in person sees, each with who sees it (its
 * kind, §33.1), and for somebody who may write a site, changing the kind
 * and the people it names.
 *
 * A site is seen by its members, by the company's people and its members,
 * or by everybody signed in; whoever sees every site sees each of them. A
 * change is seen within the ten seconds Shale keeps a person's sites.
 *
 * Whether the person may write a site is what they hold at Shale, as the
 * Tokens page reads it (`HolderService.ListKeys`): where roster runs inside
 * Shale and names no operators everybody may. A person is added to a site
 * by the alias they are known by, since a person cannot list people
 * without a grant to; somebody Shale has never seen sign in is not found.
 *
 * @module
 */

import { useEffect, useState, type ReactNode } from 'react'

import { Code, ConnectError } from '@connectrpc/connect'
import { useCall } from '@lesomnus/payday/react'

import { ReaderService } from '../gen/shale/host_svc_pb.js'
import type { SiteMember } from '../gen/shale/member_pb.js'
import { SiteMemberService } from '../gen/shale/member_svc_pb.js'
import { HolderService, type HolderListKeysResponse } from '../gen/shale/payday/holder_svc_pb.js'
import { SetService } from '../gen/shale/set_svc_pb.js'
import { SiteVisibility, type Site } from '../gen/shale/site_pb.js'
import { SiteService } from '../gen/shale/site_svc_pb.js'

import { tenantOf } from './session.js'
import { On, useSurfaces } from './surface.js'
import { Empty, Loading, byId, confirmThen, hex, short } from './ui.js'

export function Sites(): ReactNode {
	return (
		<>
			<h1>Sites</h1>
			<p className="lede">
				A site groups cameras, and says who sees them: the people it names, the company's people, or everybody signed in. Whoever sees every site
				sees each of them. A change is seen within ten seconds.
			</p>
			<On surface="tenant" note="Sites are a tenant's: sign in to the tenant API.">
				<Page />
			</On>
		</>
	)
}

/** said is what the server said, in its words. */
function said(err: unknown): string {
	return err instanceof ConnectError ? err.rawMessage : String(err)
}

/** KINDS are the three a site may be, each with who then sees it. */
const KINDS: ReadonlyArray<{ v: SiteVisibility; name: string; line: string }> = [
	{
		v: SiteVisibility.MEMBERS,
		name: 'Its members',
		line: 'The people and readers it names see its cameras, and nobody else but whoever sees every site.',
	},
	{
		v: SiteVisibility.COMPANY,
		name: "The company's people",
		line: "The company's people see its cameras, and the people and readers it names. Until Shale tells the company's people apart, nobody is one, and only its members see it.",
	},
	{
		v: SiteVisibility.EVERYBODY,
		name: 'Everybody',
		line: 'Everybody signed in to the tenant sees its cameras; a reader, only if it names it.',
	},
]

/** kindOf is a site's kind: one that says nothing is its members'. */
function kindOf(v: SiteVisibility): (typeof KINDS)[number] {
	return KINDS.find((k) => k.v === v) ?? KINDS[0]!
}

function siteName(v: Site): string {
	return v.name !== '' ? v.name : v.alias
}

const PATCH = '/shale.SiteService/Patch'
const MEMBER_ADD = '/shale.SiteMemberService/Add'
const MEMBER_ERASE = '/shale.SiteMemberService/Erase'

/** May is what the person may change here, and why not where they may not. */
type May = { kind: boolean; members: boolean; why: string | null }

/**
 * mayOf reads what the person holds at Shale: a method held, or nobody
 * holding any role of Shale's because nobody is named an operator, where
 * every person of the tenant may write a site.
 */
function mayOf(v: HolderListKeysResponse | undefined, err: unknown): May {
	if (v === undefined) {
		return { kind: false, members: false, why: err === undefined ? null : `Shale could not say what you hold, so nothing here can be changed: ${said(err)}` }
	}
	const everybody = v.holds.length === 0 && v.noneHeld !== ''
	const kind = everybody || v.holds.includes(PATCH)
	const members = everybody || (v.holds.includes(MEMBER_ADD) && v.holds.includes(MEMBER_ERASE))
	const why =
		kind && members
			? null
			: !kind && !members
				? "You see these sites, and cannot change who sees them: you hold no role of Shale's that writes a site or its members. Ask an administrator for one."
				: !kind
					? "You cannot change a site's kind: you hold no role of Shale's that writes a site. Ask an administrator for one."
					: "You cannot change a site's members: you hold no role of Shale's that adds and removes them. Ask an administrator for one."

	return { kind, members, why }
}

function Page(): ReactNode {
	const sites = useCall(SiteService.method.list)
	const sets = useCall(SetService.method.list)
	const keys = useCall(HolderService.method.listKeys)
	const reload = (): void => {
		sites.call({ size: 100 }).catch(() => {})
	}
	useEffect(() => {
		reload()
		sets.call({ size: 500 }).catch(() => {})
		keys.call({}).catch(() => {})
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [])
	const may = keys.state === 'pending' && keys.data === undefined ? { kind: false, members: false, why: null } : mayOf(keys.data, keys.error)
	const count = (site: Site): number => (sets.data?.items ?? []).filter((v) => hex(v.site?.id) === hex(site.id)).length

	if (sites.data === undefined) {
		return sites.state === 'error' ? <p className="bad">{said(sites.error)}</p> : <Loading />
	}

	return (
		<>
			{may.why !== null && <p className="warn none-held">{may.why}</p>}
			{sites.data.items.length === 0 ? (
				<Empty>You see no site. A camera in no site is seen by whoever sees every site alone.</Empty>
			) : (
				<div className="wrap">
					<table className="sites">
						<thead>
							<tr>
								<th>site</th>
								<th>cameras' sets</th>
								<th>who sees it</th>
								<th>members</th>
							</tr>
						</thead>
						<tbody>
							{sites.data.items.map((v) => (
								<Row key={hex(v.id)} site={v} sets={count(v)} may={may} changed={reload} />
							))}
						</tbody>
					</table>
				</div>
			)}
		</>
	)
}

function Row(props: { site: Site; sets: number; may: May; changed: () => void }): ReactNode {
	const { site, may } = props
	const patch = useCall(SiteService.method.patch)
	const kind = kindOf(site.visibility)
	const [chosen, setChosen] = useState<SiteVisibility>(kind.v)
	useEffect(() => setChosen(kind.v), [kind.v])
	const save = (): void => {
		const to = kindOf(chosen)
		confirmThen(`Make ${siteName(site)} seen by ${to.name.toLowerCase()}? ${to.line} It is so within ten seconds.`, () =>
			patch
				.call({ ref: byId(site.id), visibility: chosen, dateUpdatedForce: true })
				.then(props.changed)
				.catch(() => {}),
		)
	}

	return (
		<tr>
			<td>
				<b>{siteName(site)}</b> <span className="dim mono">{site.alias}</span>
			</td>
			<td className="num">{props.sets}</td>
			<td>
				{may.kind ? (
					<fieldset className="kinds">
						{KINDS.map((k) => (
							<label key={k.v}>
								<input type="radio" name={`kind-${hex(site.id)}`} checked={chosen === k.v} onChange={() => setChosen(k.v)} /> {k.name}{' '}
								<span className="dim">· {k.line}</span>
							</label>
						))}
						<div className="toolbar">
							<button type="button" className="primary" disabled={chosen === kind.v || patch.state === 'pending'} onClick={save}>
								save
							</button>
						</div>
						{patch.state === 'error' && <p className="bad">{said(patch.error)}</p>}
					</fieldset>
				) : (
					<>
						{kind.name} <span className="dim">· {kind.line}</span>
					</>
				)}
			</td>
			<td>
				<Members site={site} may={may} />
			</td>
		</tr>
	)
}

/** Members is who a site names, read on demand, with *remove* and *add* for who may. */
function Members(props: { site: Site; may: May }): ReactNode {
	const { site, may } = props
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	const list = useCall(SiteMemberService.method.list)
	const add = useCall(SiteMemberService.method.add)
	const [open, setOpen] = useState(false)
	const [alias, setAlias] = useState('')
	const [note, setNote] = useState<string | null>(null)
	const reload = (): void => {
		list.call({ filters: [{ site: byId(site.id) }], size: 500 }).catch(() => {})
	}
	useEffect(() => {
		if (open) reload()
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [open])

	// `@acme/bob` or `bob`, in the signed-in person's tenant.
	const typed = alias.trim().replace(/^@[^/]*\//, '').replace(/^@/, '')
	const join = (): void => {
		setNote(null)
		add
			.call({
				tenant: { key: { case: 'alias', value: tenant } },
				site: byId(site.id),
				holder: { key: { case: 'slug', value: { alias: typed, tenant: { key: { case: 'alias', value: tenant } } } } },
			})
			.then(() => {
				setAlias('')
				setNote(`@${tenant}/${typed} now sees ${siteName(site)}, within ten seconds.`)
				reload()
			})
			.catch((err: unknown) => {
				setNote(
					err instanceof ConnectError && err.code === Code.NotFound
						? `Nobody called @${tenant}/${typed} was found: Shale knows a person once they have signed in here, by the alias they sign in with.`
						: said(err),
				)
			})
	}

	return (
		<details onToggle={(e) => setOpen((e.target as HTMLDetailsElement).open)}>
			<summary>{list.data === undefined ? 'members' : `${list.data.items.length} ${list.data.items.length === 1 ? 'member' : 'members'}`}</summary>
			{list.state === 'error' ? (
				<p className="bad">
					{list.error instanceof ConnectError && list.error.code === Code.PermissionDenied
						? "You may not read who a site names: you hold no role of Shale's that reads its members."
						: said(list.error)}
				</p>
			) : list.data === undefined ? (
				open && <Loading />
			) : list.data.items.length === 0 ? (
				<p className="dim">It names nobody.</p>
			) : (
				<ul className="members">
					{list.data.items.map((m) => (
						<Member key={hex(m.id)} m={m} site={site} may={may} removed={reload} />
					))}
				</ul>
			)}
			{may.members && (
				<div className="toolbar">
					<input value={alias} onChange={(e) => setAlias(e.target.value)} placeholder={`@${tenant}/alias`} />
					<button type="button" disabled={typed === '' || add.state === 'pending'} onClick={join}>
						add
					</button>
				</div>
			)}
			{note !== null && <p className="dim">{note}</p>}
		</details>
	)
}

/** Member is one membership: the person by their alias, or the reader by its alias, where the person may read them. */
function Member(props: { m: SiteMember; site: Site; may: May; removed: () => void }): ReactNode {
	const { m, site, may } = props
	const holder = useCall(HolderService.method.get)
	const reader = useCall(ReaderService.method.get)
	const erase = useCall(SiteMemberService.method.erase)
	const s = useSurfaces()
	const tenant = tenantOf(s.sessions.tenant?.session.who ?? '')
	useEffect(() => {
		if (m.holder !== undefined) holder.call({ ref: byId(m.holder.id) }).catch(() => {})
		else if (m.reader !== undefined) reader.call({ ref: byId(m.reader.id) }).catch(() => {})
		// eslint-disable-next-line react-hooks/exhaustive-deps
	}, [hex(m.id)])
	const who =
		m.holder !== undefined
			? holder.data !== undefined
				? `@${tenant}/${holder.data.alias}`
				: `a person, ${short(m.holder.id)}`
			: reader.data !== undefined
				? `the reader ${reader.data.alias}`
				: `a reader, ${short(m.reader?.id)}`

	return (
		<li>
			{who}{' '}
			{may.members && (
				<button
					type="button"
					className="danger"
					disabled={erase.state === 'pending'}
					onClick={() =>
						confirmThen(`Remove ${who} from ${siteName(site)}? They stop seeing it within ten seconds, unless its kind shows it to them.`, () =>
							erase.call(byId(m.id)).then(props.removed, () => {}),
						)
					}
				>
					remove
				</button>
			)}
			{erase.state === 'error' && <span className="bad"> {said(erase.error)}</span>}
		</li>
	)
}
