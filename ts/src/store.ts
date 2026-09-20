/**
 * The local store and the reads that keep themselves up to date.
 *
 * `entities` is generated -- one declaration per entity, and no behaviour. The
 * runtime reads those together with the protobuf descriptors and implements
 * normalizing, reconciling and applying a `Watch` once, for every entity there
 * is.
 *
 * `Queries` is the half above it. A read that goes through it is a read the
 * framework knows about, so when a row it answered with changes, everything
 * drawing that row is told -- and every place it appears changes at once, with
 * nothing declared anywhere. `@lesomnus/payday/react` is twenty lines of
 * `useSyncExternalStore` over this; Vue or Svelte is the same file, the same
 * length.
 *
 * @module
 */

import type { Transport } from '@connectrpc/connect'

import { Queries } from '@lesomnus/payday/query'
import type { App } from '@lesomnus/payday/react'
import { Store, identityOf } from '@lesomnus/payday/store'
import { openDisk } from '@lesomnus/payday/store/idb'

import { entities } from '../gen/entities.js'

/**
 * open answers with this app's store and queries, for one caller.
 *
 * The identity is a digest of the **credential** rather than a name for the
 * person -- see `identityOf`. What a caller may see is the actor and the scope
 * together, so the same person holding a narrowed credential is a different
 * store; keyed on the person, the narrowed session would draw the wide
 * session's rows. Nothing leaks either way, and the screen is wrong.
 *
 * The mirror is what makes a reload draw the page it had rather than a spinner
 * for it. Delete these two lines and everything still works -- the store is a
 * store either way, and it lives as long as the tab does.
 *
 * It expires after a week; `openDisk(entities, at, { keep })` is the knob, and
 * `Infinity` turns it off. What it bounds is both how large a mirror gets and
 * how old the answer a reload draws may be.
 */
export async function open(transport: Transport, credential: string): Promise<App> {
	const at = { name: 'shale', identity: await identityOf(credential) }

	const store = Store.open(entities, { ...at, disk: await openDisk(entities, at) })
	await store.hydrate()

	return { store, queries: new Queries(store, transport, entities) }
}
