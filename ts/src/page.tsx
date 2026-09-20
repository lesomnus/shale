/**
 * One page, and the whole of what payday's client layer is for.
 *
 * Read it for what is **not** here. Nothing declares which query a write
 * invalidates, nothing pushes a new row into a list, and nothing tells the row
 * at the top of the page that the row in the table below it is the same row.
 * Every one of those falls out of the calls going through `useQuery` and
 * `useCall`: the framework knows what was drawn because it drew it.
 *
 * @module
 */

import { useState } from 'react'

import { useCall, useQuery, useRow } from '@lesomnus/payday/react'
import { key } from '@lesomnus/payday/store'

// `gen/` mirrors `proto/`, so these begin with the proto package -- and
// payday's own entities are inside it, at `gen/app/payday/`. See `client.ts`.
// `entities.js` is payday's own and is at the root of `gen/`.
import { Tenant } from '../gen/entities.js'
import type { Thing } from '../gen/app/thing_pb.js'
import type { Tenant as TenantMsg } from '../gen/app/payday/tenant_pb.js'
import { ThingService } from '../gen/app/thing_svc_pb.js'
import { TenantService } from '../gen/app/payday/tenant_svc_pb.js'

export function Page(props: { who: string; onSignOut: () => void }): React.ReactNode {
	// `@acme/admin` -- the tenant this credential is inside.
	const alias = props.who.replace(/^@/, '').split('/')[0] ?? ''

	// A `Get`, which is a query like any other: its answer is a row, so the
	// store holds it and anything else that names that row draws the same copy.
	// `Things` below never asks for a tenant and shows one anyway.
	const tenant = useQuery(TenantService.method.get, { ref: { key: { case: 'alias', value: alias } } })

	return (
		<main>
			<header>
				<h1>shale</h1>
				<span>{props.who}</span>
				<button onClick={props.onSignOut}>sign out</button>
			</header>

			{tenant.state === 'error' ? (
				<p className="bad">{String(tenant.error)}</p>
			) : tenant.data === undefined ? (
				<p>...</p>
			) : (
				<>
					<Add tenant={tenant.data.id} />
					<Things />
				</>
			)}
		</main>
	)
}

/** Add is a write, and the whole of what a write has to say. */
function Add(props: { tenant: Uint8Array }): React.ReactNode {
	const [alias, setAlias] = useState('')
	const add = useCall(ThingService.method.add)

	return (
		<form
			onSubmit={(e) => {
				e.preventDefault()
				if (alias === '') return

				// What happens next is not written down anywhere. The answer is
				// a row, so it goes into the store and every place showing that
				// row is right at once; and the lists over Things are read
				// again, because a create can change what belongs in one and
				// only the server knows which.
				add.call({ tenant: { key: { case: 'id', value: props.tenant } }, alias })
					.then(() => setAlias(''))
					.catch(() => {})
			}}
		>
			<input value={alias} placeholder="a name" onChange={(e) => setAlias(e.target.value)} />
			<button type="submit" disabled={add.state === 'pending'}>
				add
			</button>
			{add.state === 'error' && <span className="bad">{String(add.error)}</span>}
		</form>
	)
}

function Things(): React.ReactNode {
	// No filters, so the server answers with the first page of what this caller
	// may see -- and no `Watch` behind it, because a watch says which rows it
	// is about and one that says nothing is the whole table. This page re-reads
	// after its own writes; a page that wants other people's writes live names
	// a filter.
	const { state, data, error } = useQuery(ThingService.method.list, { filters: [] })

	if (state === 'error') return <p className="bad">{String(error)}</p>
	if (data === undefined) return <p>...</p>
	if (data.items.length === 0) return <p>nothing yet.</p>

	return (
		<table>
			<tbody>
				{data.items.map((v) => (
					<Row key={key(v.id)} thing={v} />
				))}
			</tbody>
		</table>
	)
}

function Row(props: { thing: Thing }): React.ReactNode {
	const erase = useCall(ThingService.method.erase)

	// The list did not ask for the tenant and the answer carries only its
	// identifier -- so this reads the row the `Get` above already put in the
	// store. One copy, two components, and nothing joining them up. If nothing
	// had it, this would answer with undefined, which is the cue to go and ask.
	const tenant = useRow<TenantMsg>(Tenant.typeName, props.thing.tenant?.id)

	return (
		<tr>
			<td>{props.thing.alias}</td>
			<td className="dim">{tenant?.alias ?? '-'}</td>
			<td>
				<button
					disabled={erase.state === 'pending'}
					onClick={() => {
						// The answer to an `Erase` says whether this call was
						// the one that erased it and names no row, so what says
						// which row is gone is the request -- and the row leaves
						// every list drawing it now, rather than a round trip
						// from now.
						erase.call({ key: { case: 'id', value: props.thing.id } }).catch(() => {})
					}}
				>
					erase
				</button>
			</td>
		</tr>
	)
}
