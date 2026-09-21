/**
 * Who the console is, on which API.
 *
 * Shale has two API surfaces (§35): the **tenant** API, where a tenant's admin
 * sees its sets, cameras, producers and laminae, and the **cluster** API, where
 * an operator sees nodes, devices, sinks and relays. The console signs in to
 * each on its own, and a page that needs one it does not have asks for it.
 *
 * Against a real server a sign-in is `POST /session` with the person's tenant,
 * alias and password, answered with a cookie this script cannot read
 * (`auth/authsession`); every call after that carries the cookie. In the
 * sandbox there is nobody to lie to, so the credential is the plain header of
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
}

/** Where each surface answers, for a page served away from the server. */
export interface Addrs {
	readonly tenant: string
	readonly cluster: string
}

/**
 * addrs is where the two surfaces are: `VITE_TENANT_ADDR` and
 * `VITE_CLUSTER_ADDR` when the page is `npm run dev`, else the origin the page
 * came from for the tenant API and the port beside it for the cluster API
 * (7402 and 7403 by default, §34.1).
 */
export function addrs(): Addrs {
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

/** signOut ends the session on the server and forgets it here. */
export async function signOut(s: Session): Promise<void> {
	if (s.sandbox) return
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
