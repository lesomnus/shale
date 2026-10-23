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
import { Store } from '@lesomnus/payday/store'
import { deleteOtherDisks, openDisk } from '@lesomnus/payday/store/idb'

import { entities } from '../gen/entities.js'
import type { Surface } from './session.js'

/**
 * open answers with this console's store and queries, for one caller on one
 * surface: the two APIs answer different rows to the same person, so each
 * surface has stores of its own (`shale.tenant`, `shale.cluster`), and each
 * caller one of those, named by `identity` -- what the session lets them see,
 * as the server names it (`GET /session`), so the same person narrowed is
 * another store and a session that ran out is the same store once they sign
 * in again. Every call the store makes says it, and the server serves the
 * call only as a session it names.
 *
 * The mirror is what makes a reload draw the page it had rather than a
 * spinner for it; it expires after a week. Once this caller's is open, every
 * other caller's of the surface goes (`deleteOtherDisks`): a mirror is swept
 * only when its own caller opens it again, so one left by somebody who
 * stopped using this browser would keep what they saw and the writes they
 * never sent for as long as the browser keeps it. So do the mirrors this
 * console kept before it named them by surface, which nobody opens again.
 * Neither is waited on: a database another page holds open answers late.
 */
export async function open(transport: Transport, surface: Surface, identity: string): Promise<App> {
	const at = { name: `shale.${surface}`, identity }

	const store = Store.open(entities, { ...at, disk: await openDisk(entities, at) })
	void deleteOtherDisks(at)
	void deleteOtherDisks(unnamed)
	await store.hydrate()

	return { store, queries: new Queries(store, transport, entities) }
}

/**
 * unnamed stands for the mirrors of before (`shale` and a digest of the
 * surface and the person): it is nobody's, so every one of them is another's.
 */
const unnamed = { name: 'shale', identity: '' }
