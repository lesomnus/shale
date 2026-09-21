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
 * nothing declared anywhere.
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
 * open answers with this console's store and queries, for one caller on one
 * surface: the two APIs answer different rows to the same person, so each
 * gets a store of its own, and so does each credential (see `identityOf`).
 *
 * The mirror is what makes a reload draw the page it had rather than a
 * spinner for it; it expires after a week.
 */
export async function open(transport: Transport, credential: string): Promise<App> {
	const at = { name: 'shale', identity: await identityOf(credential) }

	const store = Store.open(entities, { ...at, disk: await openDisk(entities, at) })
	await store.hydrate()

	return { store, queries: new Queries(store, transport, entities) }
}
