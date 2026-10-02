/**
 * Who the console is, on which API.
 *
 * Shale has two API surfaces (§35): the **tenant** API, where a tenant's admin
 * sees its sets, cameras, producers and laminae, and the **cluster** API, where
 * an operator sees nodes, devices, sinks and relays. The console signs in to
 * each on its own, and a page that needs one it does not have asks for it.
 *
 * Against a real server a sign-in is one of two things (§40.1). With a
 * password, `POST /session` with the person's tenant, alias and password; with
 * the deployment's issuer (single sign-on, §33.1), the browser itself goes to
 * `/sso/login` on the surface's listener and comes back signed in. Either way
 * the answer is a cookie this script cannot read (`auth/authsession`), every
 * call after that carries it, and `GET /session` says who it names.
 * `GET /session/ways` says which of the two a listener offers. In the sandbox
 * there is nobody to lie to, so the credential is the plain header of
 * development mode on the one transport the page has.
 *
 * @module
 */

import { Code, ConnectError, type Transport } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'

export type Surface = 'tenant' | 'cluster'

/** Who is signed in on one surface, and how to reach it. */
export interface Session {
	readonly surface: Surface
	/** `@tenant/alias`, which is also what the store is keyed on. */
	readonly who: string
	readonly transport: Transport
	/** The HTTP origin of this surface; empty in the sandbox. */
	readonly base: string
	readonly sandbox: boolean
	/** Signed in through the issuer, so signing out is two hops. */
	readonly sso?: boolean
}

/** How a listener signs people in (`GET /session/ways`). */
export interface Ways {
	readonly password: boolean
	readonly sso: boolean
	readonly login?: string
	/**
	 * Where the cluster API's HTTP listener answers browsers, when the server
	 * names it -- behind an Ingress, where "the port beside" is nothing.
	 */
	readonly cluster?: string
}

/**
 * ways asks a listener how it signs people in. A server from before single
 * sign-on has no such route and takes a password.
 */
export async function ways(base: string): Promise<Ways> {
	try {
		const r = await withCredentials(base + '/session/ways', { cache: 'no-store' })
		if (!r.ok) return { password: true, sso: false }

		return (await r.json()) as Ways
	} catch {
		return { password: true, sso: false }
	}
}

/** Who a listener's cookie names (`GET /session`). */
export interface Who {
	readonly who: string
	readonly sso: boolean
	readonly operator: boolean
}

/**
 * current is who this browser is signed in as on a listener: null for
 * nobody, undefined when the listener cannot say (a server from before
 * `GET /session`, or one that is not answering).
 */
export async function current(base: string): Promise<Who | null | undefined> {
	try {
		const r = await withCredentials(base + '/session', { cache: 'no-store' })
		if (r.status === 401) return null
		if (!r.ok) return undefined

		return (await r.json()) as Who
	} catch {
		return undefined
	}
}

/**
 * ssoSignIn sends the browser to a listener's sign-in with the issuer, to
 * come back to this page. It does not return: the page goes.
 */
export function ssoSignIn(base: string, path = '/sso/login'): void {
	location.assign(`${base}${path}?next=${encodeURIComponent(location.href)}`)
}

/** Where each surface answers, for a page served away from the server. */
export interface Addrs {
	readonly tenant: string
	readonly cluster: string
}

/**
 * addrs is where the two surfaces are: `VITE_TENANT_ADDR` and
 * `VITE_CLUSTER_ADDR` when the page is `npm run dev`, else the origin the page
 * came from for the tenant API and, for the cluster API, the origin the server
 * named ({@link resolveAddrs}) or else the port beside it (7402 and 7403 by
 * default, §34.1).
 */
export function addrs(): Addrs {
	return resolved ?? guessed()
}

let resolved: Addrs | undefined

function guessed(): Addrs {
	const env = import.meta.env as Record<string, string | undefined>
	const tenant = env['VITE_TENANT_ADDR'] ?? location.origin
	let cluster = env['VITE_CLUSTER_ADDR']
	if (cluster === undefined) {
		try {
			const u = new URL(tenant)
			const port = u.port === '' ? (u.protocol === 'https:' ? 443 : 80) : Number(u.port)
			u.port = String(port + 1)
			cluster = u.origin
		} catch {
			cluster = tenant
		}
	}

	return { tenant, cluster }
}

/**
 * resolveAddrs asks the tenant listener where the cluster listener is, once,
 * and keeps the answer for {@link addrs}. Behind an Ingress the two are
 * different names on 443 (§40.4), and guessing the port beside -- `:444` --
 * would be a connection that hangs until it times out. A server that names
 * nothing, or a page from `npm run dev`, keeps the guess.
 */
export async function resolveAddrs(): Promise<Addrs> {
	if (resolved !== undefined) return resolved
	const g = guessed()
	const env = import.meta.env as Record<string, string | undefined>
	let cluster = g.cluster
	if (env['VITE_CLUSTER_ADDR'] === undefined) {
		const w = await ways(g.tenant)
		if (w.cluster !== undefined && w.cluster !== '') cluster = new URL(w.cluster).origin
	}
	resolved = { tenant: g.tenant, cluster }

	return resolved
}

/** The cookie goes with every call, which `fetch` does not do on its own. */
function withCredentials(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
	return fetch(input, { ...init, credentials: 'include' })
}

/** transportFor is the Connect transport of one surface of a real server. */
export function transportFor(base: string): Transport {
	return createConnectTransport({ baseUrl: base, fetch: withCredentials })
}

/**
 * signIn asks the server for a session cookie. What checking a secret means
 * is the server's (§33.1); a refusal is answered as one sentence.
 */
export async function signIn(base: string, tenant: string, alias: string, password: string): Promise<Session> {
	const r = await withCredentials(base + '/session', {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify({ tenant, alias, password }),
	})
	if (r.status === 401 || r.status === 403) {
		throw new Error('no such person, or wrong password')
	}
	if (!r.ok) {
		throw new Error(`sign-in: ${r.status} ${(await r.text()).trim()}`)
	}

	return {
		surface: base === addrs().cluster ? 'cluster' : 'tenant',
		who: `@${tenant}/${alias}`,
		transport: transportFor(base),
		base,
		sandbox: false,
	}
}

/**
 * signOut ends the session on the server and forgets it here. A session
 * from the issuer is ended in two hops (§33.1): `/sso/logout` ends this
 * listener's session and sends the browser to the issuer, which sends it
 * back here -- so for one of those this navigates away and does not return.
 */
export async function signOut(s: Session): Promise<void> {
	if (s.sandbox) return
	if (s.sso === true) {
		location.assign(`${s.base}/sso/logout?next=${encodeURIComponent(location.href)}`)

		return
	}
	try {
		await withCredentials(s.base + '/session', { method: 'DELETE' })
	} catch {
		// The cookie is gone from here either way.
	}
}

/**
 * plain wraps a transport so every call carries the plain header of
 * development mode, for the sandbox. One transport underneath, several
 * callers on top: a second transport would be a second connection.
 */
export function plain(t: Transport, who: string): Transport {
	const cred = `Plain ${who}`

	return {
		unary: (method, signal, timeoutMs, header, input, contextValues) => {
			const h = new Headers(header)
			h.set('authorization', cred)

			return t.unary(method, signal, timeoutMs, h, input, contextValues)
		},
		stream: (method, signal, timeoutMs, header, input, contextValues) => {
			const h = new Headers(header)
			h.set('authorization', cred)

			return t.stream(method, signal, timeoutMs, h, input, contextValues)
		},
	}
}

/** unauthenticated says whether an error is the server not knowing who called. */
export function unauthenticated(err: unknown): boolean {
	return err instanceof ConnectError && (err.code === Code.Unauthenticated || err.code === Code.PermissionDenied)
}

/** The tenant alias of a `@tenant/alias` credential. */
export function tenantOf(who: string): string {
	return who.replace(/^@/, '').split('/')[0] ?? ''
}
