/**
 * Where the page starts.
 *
 * Two things happen before React does, and both have to: the transport is
 * decided -- a real server here, and a Go server compiled to wasm in a sandbox
 * -- and the store is opened for **this** credential and filled from its
 * mirror. Rendering over a store that has not been hydrated is rendering a
 * spinner for something the tab already had.
 *
 * @module
 */

import { createConnectTransport } from '@connectrpc/connect-web'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { Provider } from '@lesomnus/payday/react'

import { open } from './store.js'
import { Page } from './page.js'
import './style.css'

/**
 * Where the app answers.
 *
 * `npm run dev` is a different origin from the server, so the server has to say
 * this page may call it -- `origins:` under `server.http`. A build served by
 * the app itself is same-origin and needs none of that.
 */
const ADDR = import.meta.env['VITE_ADDR'] ?? 'http://localhost:8080'

const root = createRoot(document.getElementById('root') as HTMLElement)

/**
 * The credential, which this app believes because the server does.
 *
 * `auth.Plain` reads `@tenant/holder` and takes the caller's word for it, which
 * is right for a sandbox and for tests and is **not** something to serve where
 * anyone can reach it. A real one is a cookie this script cannot read rather
 * than a string it keeps: `auth/authsession` is the endpoint that mints it and
 * the handler that reads it back, and what this app supplies is what checking
 * a secret means. See `serve.go`.
 */
function credential(): string | null {
	return localStorage.getItem('credential')
}

function SignIn(): React.ReactNode {
	return (
		<form
			className="sign-in"
			onSubmit={(e) => {
				e.preventDefault()
				const v = new FormData(e.currentTarget).get('credential')
				if (typeof v !== 'string' || v === '') return

				localStorage.setItem('credential', v)
				void boot()
			}}
		>
			<h1>shale</h1>
			<label>
				sign in as
				<input name="credential" defaultValue="@acme/admin" autoFocus />
			</label>
			<button type="submit">go</button>
			<p>
				Run <code>go run ./cmd/shale init</code> first; it prints who to sign in as.
			</p>
		</form>
	)
}

async function boot(): Promise<void> {
	const who = credential()
	if (who === null) {
		root.render(
			<StrictMode>
				<SignIn />
			</StrictMode>,
		)

		return
	}

	const transport = createConnectTransport({
		baseUrl: ADDR,
		interceptors: [
			(next) => (req) => {
				req.header.set('authorization', `Plain ${who}`)

				return next(req)
			},
		],
	})

	const app = await open(transport, who)

	/**
	 * Signing out drops this caller's copy -- the rows, the answers and the
	 * mirror. Nothing there is a secret, since the server only ever sent what
	 * that caller could see, but it is *that caller's*, and leaving it where
	 * the next one opens the same page is the kind of thing that looks like a
	 * leak whether or not it is one.
	 */
	const out = (): void => {
		app.store.forget()
		app.store.close()
		localStorage.removeItem('credential')
		void boot()
	}

	root.render(
		<StrictMode>
			<Provider app={app}>
				<Page who={who} onSignOut={out} />
			</Provider>
		</StrictMode>,
	)
}

void boot()
