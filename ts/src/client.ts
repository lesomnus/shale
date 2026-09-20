/**
 * The client, which is this small because nothing here is generated per
 * service.
 *
 * A page does not read through this -- it reads through `store.ts`, so that a
 * row it drew redraws when the row changes. This is for what does not want
 * that: several writes as one transaction, a one-off call, a script.
 *
 * protobuf-es emits the service descriptors beside the messages and Connect's
 * `createClient` takes a descriptor, so adding an entity to the schema is one
 * line here and nothing to keep in step.
 *
 * The transport is the only thing that changes between a real server and the
 * sandbox, and nothing above this file knows which it got.
 *
 * # The import paths
 *
 * `gen/` mirrors `proto/`, so every path below begins with this app's **proto
 * package** -- `app` until the schema says otherwise. payday's own entities are
 * copied *into* that package rather than beside it, which is why a Tenant is at
 * `gen/app/payday/` and is `app.Tenant` on the wire.
 *
 * `gen/payday/` is a different directory that also exists, and it is not this
 * one: it holds `entity_pb.ts`, the descriptor for the `(payday.entity)` option.
 * An import that lands there resolves to a directory and fails to find the
 * module, which reads like a generation that did not happen.
 *
 * Rename the package and these move with it. `pd gen` reads it off the schema
 * and will not rewrite the TypeScript you wrote.
 *
 * @module
 */

import { createClient, type Client, type Transport } from '@connectrpc/connect'

import { ThingService } from '../gen/app/thing_svc_pb.js'
import { TenantService } from '../gen/app/payday/tenant_svc_pb.js'
import { HolderService } from '../gen/app/payday/holder_svc_pb.js'
import { BatchService } from '@lesomnus/payday/pdpb'

export interface App {
	readonly thing: Client<typeof ThingService>
	readonly tenant: Client<typeof TenantService>
	readonly holder: Client<typeof HolderService>

	/** Several writes as one transaction; see `payday/batch`. */
	readonly batch: Client<typeof BatchService>
}

export function app(transport: Transport): App {
	return {
		thing: createClient(ThingService, transport),
		tenant: createClient(TenantService, transport),
		holder: createClient(HolderService, transport),
		batch: createClient(BatchService, transport),
	}
}
