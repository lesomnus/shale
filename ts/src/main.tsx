/**
 * Where the page starts.
 *
 * Two things happen before React does: the server is found -- a real one on
 * the network, or the app compiled to wasm and started inside this page when
 * the address says `?sandbox` -- and the sessions a reload can restore are
 * restored, so a page that had a store draws it rather than a spinner.
 *
 * @module
 */

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { Console } from './app.js'
import type { Sandbox } from './sandbox.js'
import { addrs } from './session.js'
import { restore, type Mode, type Opened } from './surface.js'
import './style.css'

const root = createRoot(document.getElementById('root') as HTMLElement)

function wantsSandbox(): boolean {
	const env = import.meta.env as Record<string, string | undefined>

	return new URLSearchParams(location.search).has('sandbox') || env['VITE_SANDBOX'] === '1'
}

function Progress(props: { text: string }): React.ReactNode {
	return (
		<div className="sign-in">
			<h1>shale</h1>
			<p className="hint">{props.text}</p>
		</div>
	)
}

async function boot(): Promise<void> {
	let mode: Mode
	if (wantsSandbox()) {
		root.render(<Progress text="starting the sandbox: the whole server, compiled into this page…" />)
		let box: Sandbox
		try {
			// Loaded here rather than above: the console a server serves
			// (§40.4) never starts a sandbox, and this keeps the SQLite and
			// message-port machinery out of what it downloads.
			const { start } = await import('./sandbox.js')
			box = await start('/app.wasm', (v) => {
				const mb = (n: number) => (n / 1048576).toFixed(0)
				const text =
					v.total > 0 && v.loaded >= v.total
						? 'compiling the server…'
						: `fetching the server: ${mb(v.loaded)} MB${v.total > 0 ? ` of ${mb(v.total)}` : ''}${v.from === 'cache' ? ' (from the last visit)' : ''}${v.keeping ? '' : ' — not kept: this origin cannot cache it'}`
				root.render(<Progress text={text} />)
			})
		} catch (err) {
			root.render(<Progress text={`the sandbox did not start: ${String(err)}`} />)

			return
		}
		mode = { kind: 'sandbox', box }
	} else {
		mode = { kind: 'real', addrs: addrs() }
	}

	const initial: Partial<Record<'tenant' | 'cluster', Opened>> = {}
	for (const surface of ['tenant', 'cluster'] as const) {
		try {
			const v = await restore(mode, surface)
			if (v !== null) initial[surface] = v
		} catch {
			// Restored later, by signing in again.
		}
	}

	root.render(
		<StrictMode>
			<Console mode={mode} initial={initial} />
		</StrictMode>,
	)
}

void boot()
