/**
 * The shell: the pages down the side, the page in the middle, and who is
 * signed in where. Routing is the hash, so a build served from the app itself
 * works from any path.
 *
 * @module
 */

import { useEffect, useState, type ReactNode } from 'react'

import { Cameras } from './cameras.js'
import { Devices } from './devices.js'
import { Hosts } from './hosts.js'
import { Live } from './live.js'
import { Segments } from './segments.js'
import { SurfacesProvider, useSurfaces, type Mode, type Opened } from './surface.js'

/** useHash is the route: the hash, split on slashes. */
function useHash(): string[] {
	const read = (): string[] => location.hash.replace(/^#\/?/, '').split('/').filter((v) => v !== '')
	const [parts, setParts] = useState(read)
	useEffect(() => {
		const on = (): void => setParts(read())
		window.addEventListener('hashchange', on)

		return () => window.removeEventListener('hashchange', on)
	}, [])

	return parts
}

const PAGES: ReadonlyArray<{ path: string; title: string }> = [
	{ path: 'cameras', title: 'Cameras' },
	{ path: 'segments', title: 'Segments' },
	{ path: 'live', title: 'Live' },
	{ path: 'hosts', title: 'Hosts' },
	{ path: 'devices', title: 'Devices' },
]

export function Console(props: { mode: Mode; initial: Partial<Record<'tenant' | 'cluster', Opened>> }): ReactNode {
	return (
		<SurfacesProvider mode={props.mode} initial={props.initial}>
			<Shell />
		</SurfacesProvider>
	)
}

function Shell(): ReactNode {
	const parts = useHash()
	const page = parts[0] ?? 'cameras'
	const s = useSurfaces()

	let body: ReactNode
	switch (page) {
		case 'hosts':
			body = <Hosts />
			break
		case 'devices':
			body = <Devices />
			break
		case 'segments':
			body = <Segments set={parts[1]} />
			break
		case 'live':
			body = <Live set={parts[1]} />
			break
		default:
			body = <Cameras />
	}

	return (
		<div className="shell">
			<nav className="side">
				<div className="brand">
					shale
					<small>{s.mode.kind === 'sandbox' ? 'sandbox' : 'console'}</small>
				</div>
				{PAGES.map((p) => (
					<a key={p.path} href={`#/${p.path}`} className={page === p.path ? 'current' : ''}>
						{p.title}
					</a>
				))}
				<div className="who">
					{(['tenant', 'cluster'] as const).map((surface) => {
						const v = s.sessions[surface]

						return (
							<span key={surface}>
								{surface}: {v === undefined ? <span className="dim">not signed in</span> : v.session.who}{' '}
								{v !== undefined && (
									<button type="button" onClick={() => void s.signOut(surface)}>
										out
									</button>
								)}
							</span>
						)
					})}
				</div>
			</nav>
			<main>{body}</main>
		</div>
	)
}
