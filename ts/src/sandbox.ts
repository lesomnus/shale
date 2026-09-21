/**
 * The whole app, in the page.
 *
 * Almost all of this is `@lesomnus/payday/sandbox`. What is left is the two
 * things payday cannot know: which services this app has, and where this app's
 * worker file is.
 *
 * A reload is a fresh server: new instance, new database, nothing left over.
 * Somebody working on the front end starts no backend, migrates nothing, and
 * does not have to remember what state they left it in.
 *
 *     const sb = await start()
 *     const t = await sb.app.tenant.get({ ref: { key: { case: 'alias', value: 'acme' } } })
 *
 * @module
 */

import { start as open, type Load, type Sandbox as Instance } from '@lesomnus/payday/sandbox'

import { app, type App } from './client.js'

/** Sandbox is this app, and the instance it is running inside. */
export interface Sandbox {
	readonly app: App

	/**
	 * The transport underneath, for a page that wants to wrap it -- to say who
	 * is calling, say.
	 *
	 * Wrap this rather than calling `createDrpcTransport` on [Sandbox.sock]
	 * again. A second transport is a second connection nobody asked for, and if
	 * `@lesomnus/grpc-dgram` ever resolves to two copies the socket and the
	 * transport come from different ones -- which is not a type error, it is a
	 * refusal arriving with no status.
	 */
	readonly transport: Instance['transport']

	/** The wasm instance, for a page that wants to take it down. */
	readonly sock: Instance['sock']

	/** close stops the server. A reload does the same thing more thoroughly. */
	close(): void
}

/**
 * start compiles the app into the page and answers with a client for it.
 *
 * `onProgress` is how far the module has got. A payday app compiled to wasm is
 * tens of megabytes, and that is long enough that a page saying only "starting"
 * is indistinguishable from a page that has hung -- so a page with somewhere to
 * draw should pass this. `Load.from` says whether the bytes are the server's or
 * the last visit's, which is worth showing: reading a module off a disk fills a
 * bar exactly like downloading it does.
 *
 * See `Opts` in `@lesomnus/payday/sandbox` for `Load.total` being 0, and for
 * why the module is kept in the Cache API rather than the browser's own.
 *
 * The worker URL is resolved against **this** module rather than passed in,
 * because `sandbox-worker.ts` sits beside this file and a bundler rewrites
 * where it lands. It is this file's to know and not the package's: a default
 * inside `@lesomnus/payday` would resolve against `node_modules`.
 */
export async function start(url = '/app.wasm', onProgress?: (v: Load) => void): Promise<Sandbox> {
	const box = await open({
		url,
		worker: new URL('./sandbox-worker.ts', import.meta.url),

		// Passed on rather than defaulted, because a sandbox with nowhere to
		// draw has no use for the count.
		...(onProgress === undefined ? {} : { onProgress }),
	})

	// `box.transport` is a Connect `Transport` like any other, which is the
	// whole reason a sandbox is worth having: `app()` is the same call the real
	// server gets, and nothing above it knows which one it got. Code that only
	// ever ran against a fake is code that has never run.
	return {
		app: app(box.transport),
		transport: box.transport,
		sock: box.sock,
		close: box.close,
	}
}
