/**
 * The two APIs as the pages see them: a session per surface, opened on
 * demand, and a sign-in form where one is missing.
 *
 * @module
 */

import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'

import { Provider, type App } from '@lesomnus/payday/react'
import { identityOf } from '@lesomnus/payday/store'

import { app } from './client.js'
import type { Sandbox } from './sandbox.js'
import {
	addrs,
	current,
	plain,
	signIn as signInReal,
	signOut as signOutReal,
	ssoSignIn,
	transportFor,
	ways as waysOf,
	type Addrs,
	type Session,
	type Surface,
	type Ways,
} from './session.js'
import { open } from './store.js'

/** Mode is where the server is: somewhere on the network, or in this page. */
export type Mode = { readonly kind: 'real'; readonly addrs: Addrs } | { readonly kind: 'sandbox'; readonly box: Sandbox }

/** Opened is a session with its store and queries. */
export interface Opened {
	readonly session: Session
	readonly app: App
}

export interface Surfaces {
	readonly mode: Mode
	readonly sessions: Partial<Record<Surface, Opened>>
	/**
	 * What a sign-out could not clear off this browser, by surface: the
	 * person is told on the sign-in that replaces the page, rather than left
	 * believing it went.
	 */
	readonly kept: Partial<Record<Surface, string>>
	signIn(surface: Surface, tenant: string, alias: string, password: string): Promise<void>
	signOut(surface: Surface): Promise<void>
}

const Context = createContext<Surfaces | null>(null)

export function useSurfaces(): Surfaces {
	const v = useContext(Context)
	if (v === null) throw new Error('outside <SurfacesProvider>')

	return v
}

const KEY = (surface: Surface): string => `shale.session.${surface}`
const IDENTITY = (surface: Surface): string => `shale.session.${surface}.identity`

/**
 * remembered is who was signed in on a surface the last time, for a reload,
 * and the name of their store when it was kept.
 */
export function remembered(surface: Surface): { tenant: string; alias: string; identity?: string } | null {
	try {
		const v = localStorage.getItem(KEY(surface))
		if (v === null) return null
		const m = /^@([^/]+)\/(.+)$/.exec(v)
		if (m === null) return null
		const identity = localStorage.getItem(IDENTITY(surface))

		return { tenant: m[1] ?? '', alias: m[2] ?? '', ...(identity === null ? {} : { identity }) }
	} catch {
		return null
	}
}

function remember(surface: Surface, v: Session | null): void {
	try {
		if (v === null) {
			localStorage.removeItem(KEY(surface))
			localStorage.removeItem(IDENTITY(surface))
		} else {
			localStorage.setItem(KEY(surface), v.who)
			localStorage.setItem(IDENTITY(surface), v.identity)
		}
	} catch {
		// A browser that keeps nothing still signs in; it forgets on reload.
	}
}

/**
 * letGo forgets a store and closes its mirror once the forget has reached
 * it, and says whether it did: a close refused is a forget that did not
 * clear the mirror, and what the person saw is still on this browser.
 */
async function letGo(app: App): Promise<boolean> {
	// close() answers for the forget as well, so its own refusal is that one.
	void app.store.forget().catch(() => {})
	try {
		await app.store.close()

		return true
	} catch {
		return false
	}
}

const KEPT = 'signed out, but this browser still holds what you saw: clear its data for this site to remove it'

/**
 * openSession makes a session on one surface, real or sandbox. The sandbox
 * believes whoever the page says, so its store is named by that.
 */
export async function openSession(mode: Mode, surface: Surface, tenant: string, alias: string, password: string): Promise<Opened> {
	const who = `@${tenant}/${alias}`
	let session: Session
	if (mode.kind === 'sandbox') {
		const identity = await identityOf(`${surface}:${who}`)
		session = { surface, who, identity, transport: plain(mode.box.transport, who), base: '', sandbox: true }
	} else {
		session = await signInReal(surface === 'tenant' ? mode.addrs.tenant : mode.addrs.cluster, tenant, alias, password)
	}
	const app = await open(session.transport, surface, session.identity)

	return { session, app }
}

/**
 * restore reopens a real session the browser still has the cookie for. The
 * listener is asked who its cookie names and the name of their store
 * (`GET /session`), which is also how a page that comes back from the issuer
 * learns it is signed in. A listener that cannot say just now leaves who was
 * signed in here last, and their store by the name it had, so a reload
 * draws the page it had; a cookie that lapsed then shows as the first call
 * refused, and a write made meanwhile waits for them to sign in again. The
 * sandbox has nothing to restore: it is new each load.
 */
export async function restore(mode: Mode, surface: Surface): Promise<Opened | null> {
	if (mode.kind === 'sandbox') return null
	const base = surface === 'tenant' ? mode.addrs.tenant : mode.addrs.cluster
	const w = await current(base)
	let who: string
	let identity: string
	let sso = false
	if (w === null) {
		remember(surface, null)

		return null
	} else if (w !== undefined) {
		who = w.who
		identity = w.identity ?? (await identityOf(`${surface}:${who}`))
		sso = w.sso
	} else {
		const r = remembered(surface)
		if (r === null) return null
		who = `@${r.tenant}/${r.alias}`
		identity = r.identity ?? (await identityOf(`${surface}:${who}`))
	}
	const session: Session = { surface, who, identity, transport: transportFor(base), base, sandbox: false, sso }
	remember(surface, session)
	const app = await open(session.transport, surface, identity)

	return { session, app }
}

export function SurfacesProvider(props: { mode: Mode; initial: Partial<Record<Surface, Opened>>; children: ReactNode }): ReactNode {
	const [sessions, setSessions] = useState(props.initial)
	const [kept, setKept] = useState<Partial<Record<Surface, string>>>({})
	// For the browser's console and for tests: what is signed in, and a
	// client over it (`shale.app(shale.sessions.tenant.session.transport)`).
	;(window as unknown as { shale: unknown }).shale = { mode: props.mode.kind, sessions, app }

	const value: Surfaces = {
		mode: props.mode,
		sessions,
		kept,
		async signIn(surface, tenant, alias, password) {
			const v = await openSession(props.mode, surface, tenant, alias, password)
			remember(surface, v.session)
			setKept((k) => {
				const n = { ...k }
				delete n[surface]

				return n
			})
			setSessions((s) => ({ ...s, [surface]: v }))
		},
		async signOut(surface) {
			const v = sessions[surface]
			if (v === undefined) return
			if (v.session.sso === true) {
				// Signing out of the issuer is signing out of everything it
				// signed in: the other surface's session ends here first,
				// then this one's two hops take the page away (§33.1) --
				// once what each store kept is off this browser, since the
				// page does not wait for it.
				const other: Surface = surface === 'tenant' ? 'cluster' : 'tenant'
				const o = sessions[other]
				if (o !== undefined) {
					await signOutReal({ ...o.session, sso: false })
					await letGo(o.app)
					remember(other, null)
				}
				await letGo(v.app)
				remember(surface, null)
				await signOutReal(v.session)

				return
			}
			await signOutReal(v.session)
			const cleared = await letGo(v.app)
			remember(surface, null)
			if (!cleared) setKept((k) => ({ ...k, [surface]: KEPT }))
			setSessions((s) => {
				const n = { ...s }
				delete n[surface]

				return n
			})
		},
	}

	return <Context.Provider value={value}>{props.children}</Context.Provider>
}

/**
 * On renders its children inside the store of one surface, or the sign-in
 * for it when there is none yet.
 */
export function On(props: { surface: Surface; children: ReactNode; note?: string }): ReactNode {
	const s = useSurfaces()
	const v = s.sessions[props.surface]
	if (v === undefined) return <SignIn surface={props.surface} {...(props.note === undefined ? {} : { note: props.note })} />

	return <Provider app={v.app}>{props.children}</Provider>
}

/**
 * SignIn is the way in for one surface: the issuer's button where the
 * listener signs in through one (§40.1), the form where it takes a password,
 * both where it takes both.
 */
export function SignIn(props: { surface: Surface; note?: string }): ReactNode {
	const s = useSurfaces()
	const sandbox = s.mode.kind === 'sandbox'
	const base = s.mode.kind === 'real' ? (props.surface === 'tenant' ? s.mode.addrs.tenant : s.mode.addrs.cluster) : ''
	const [ways, setWays] = useState<Ways | undefined>(sandbox ? { password: true, sso: false } : undefined)
	useEffect(() => {
		if (sandbox) return
		let live = true
		void waysOf(base).then((v) => {
			if (live) setWays(v)
		})

		return () => {
			live = false
		}
	}, [sandbox, base])
	const defaults = props.surface === 'cluster' ? { tenant: 'cluster', alias: 'ops' } : { tenant: 'acme', alias: 'admin' }
	const [tenant, setTenant] = useState(remembered(props.surface)?.tenant ?? defaults.tenant)
	const [alias, setAlias] = useState(remembered(props.surface)?.alias ?? defaults.alias)
	const [password, setPassword] = useState('')
	const [busy, setBusy] = useState(false)
	const [error, setError] = useState<string | null>(null)
	const kept = s.kept[props.surface]
	const where = props.surface === 'cluster' ? 'the cluster API, as an operator' : 'the tenant API, as somebody in a tenant'

	if (ways === undefined) {
		return (
			<div className="sign-in">
				<h1>sign in</h1>
				<p className="hint">…</p>
			</div>
		)
	}
	if (!ways.password) {
		return (
			<div className="sign-in">
				<h1>sign in</h1>
				<p className="hint">{props.note ?? `This page needs ${where}.`}</p>
				{kept !== undefined && <p className="bad">{kept}</p>}
				{ways.sso ? (
					<button type="button" className="primary" onClick={() => ssoSignIn(base, ways.login)}>
						Sign in with SSO
					</button>
				) : (
					<p className="bad">This server offers no way to sign in here.</p>
				)}
			</div>
		)
	}

	return (
		<form
			className="sign-in"
			onSubmit={(e) => {
				e.preventDefault()
				setBusy(true)
				setError(null)
				s.signIn(props.surface, tenant.trim(), alias.trim(), password)
					.catch((err: unknown) => setError(String(err)))
					.finally(() => setBusy(false))
			}}
		>
			<h1>sign in</h1>
			<p className="hint">
				{props.note ?? `This page needs ${where}.`}
				{sandbox ? ' The sandbox believes whoever you say you are.' : ''}
			</p>
			{kept !== undefined && <p className="bad">{kept}</p>}
			{ways.sso && (
				<>
					<button type="button" className="primary" onClick={() => ssoSignIn(base, ways.login)}>
						Sign in with SSO
					</button>
					<p className="hint">or with a password:</p>
				</>
			)}
			<label>
				tenant
				<input value={tenant} onChange={(e) => setTenant(e.target.value)} autoComplete="organization" />
			</label>
			<label>
				alias
				<input value={alias} onChange={(e) => setAlias(e.target.value)} autoComplete="username" autoFocus />
			</label>
			{!sandbox && (
				<label>
					password
					<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} autoComplete="current-password" />
				</label>
			)}
			<button type="submit" className="primary" disabled={busy}>
				{busy ? '…' : 'sign in'}
			</button>
			{error !== null && <p className="bad">{error}</p>}
			{!sandbox && (
				<p className="hint">
					<code>shale init</code> printed the first passwords; <code>shale holder set-password</code> changes them.
				</p>
			)}
		</form>
	)
}
