/**
 * The two APIs as the pages see them: a session per surface, opened on
 * demand, and a sign-in form where one is missing.
 *
 * @module
 */

import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'

import { Provider, type App } from '@lesomnus/payday/react'

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

/** remembered is who was signed in on a surface the last time, for a reload. */
export function remembered(surface: Surface): { tenant: string; alias: string } | null {
	try {
		const v = localStorage.getItem(KEY(surface))
		if (v === null) return null
		const m = /^@([^/]+)\/(.+)$/.exec(v)

		return m === null ? null : { tenant: m[1] ?? '', alias: m[2] ?? '' }
	} catch {
		return null
	}
}

function remember(surface: Surface, who: string | null): void {
	try {
		if (who === null) localStorage.removeItem(KEY(surface))
		else localStorage.setItem(KEY(surface), who)
	} catch {
		// A browser that keeps nothing still signs in; it forgets on reload.
	}
}

/** openSession makes a session on one surface, real or sandbox. */
export async function openSession(mode: Mode, surface: Surface, tenant: string, alias: string, password: string): Promise<Opened> {
	const who = `@${tenant}/${alias}`
	let session: Session
	if (mode.kind === 'sandbox') {
		session = { surface, who, transport: plain(mode.box.transport, who), base: '', sandbox: true }
	} else {
		session = await signInReal(surface === 'tenant' ? mode.addrs.tenant : mode.addrs.cluster, tenant, alias, password)
	}
	const app = await open(session.transport, `${surface}:${who}`)

	return { session, app }
}

/**
 * restore reopens a real session the browser still has the cookie for. The
 * listener is asked who its cookie names (`GET /session`), which is also how
 * a page that comes back from the issuer learns it is signed in; a server
 * too old to say falls back to who was signed in here last, and a cookie
 * that lapsed then shows as the first call refused. The sandbox has nothing
 * to restore: it is new each load.
 */
export async function restore(mode: Mode, surface: Surface): Promise<Opened | null> {
	if (mode.kind === 'sandbox') return null
	const base = surface === 'tenant' ? mode.addrs.tenant : mode.addrs.cluster
	const w = await current(base)
	let who: string
	let sso = false
	if (w === null) {
		remember(surface, null)

		return null
	} else if (w !== undefined) {
		who = w.who
		sso = w.sso
		remember(surface, who)
	} else {
		const r = remembered(surface)
		if (r === null) return null
		who = `@${r.tenant}/${r.alias}`
	}
	const session: Session = { surface, who, transport: transportFor(base), base, sandbox: false, sso }
	const app = await open(session.transport, `${surface}:${who}`)

	return { session, app }
}

export function SurfacesProvider(props: { mode: Mode; initial: Partial<Record<Surface, Opened>>; children: ReactNode }): ReactNode {
	const [sessions, setSessions] = useState(props.initial)
	// For the browser's console and for tests: what is signed in, and a
	// client over it (`shale.app(shale.sessions.tenant.session.transport)`).
	;(window as unknown as { shale: unknown }).shale = { mode: props.mode.kind, sessions, app }

	const value: Surfaces = {
		mode: props.mode,
		sessions,
		async signIn(surface, tenant, alias, password) {
			const v = await openSession(props.mode, surface, tenant, alias, password)
			remember(surface, v.session.who)
			setSessions((s) => ({ ...s, [surface]: v }))
		},
		async signOut(surface) {
			const v = sessions[surface]
			if (v === undefined) return
			if (v.session.sso === true) {
				// Signing out of the issuer is signing out of everything it
				// signed in: the other surface's session ends here first,
				// then this one's two hops take the page away (§33.1).
				const other: Surface = surface === 'tenant' ? 'cluster' : 'tenant'
				const o = sessions[other]
				if (o !== undefined) {
					await signOutReal({ ...o.session, sso: false })
					o.app.store.forget()
					remember(other, null)
				}
				v.app.store.forget()
				remember(surface, null)
				await signOutReal(v.session)

				return
			}
			await signOutReal(v.session)
			v.app.store.forget()
			v.app.store.close()
			remember(surface, null)
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
