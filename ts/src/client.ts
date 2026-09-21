/**
 * The client: one `createClient` per service the console calls directly.
 *
 * A page mostly reads through `store.ts`, so that a row it drew redraws when
 * the row changes. This is for what does not want that: a sign-in check, a
 * `Live` call whose answer is tokens rather than rows, a script.
 *
 * The transport is the only thing that changes between a real server and the
 * sandbox, and nothing above this file knows which it got.
 *
 * `gen/` mirrors `proto/`: Shale's entities are under `gen/shale/`, and
 * payday's own, copied into the same package, under `gen/shale/payday/`.
 *
 * @module
 */

import { createClient, type Client, type Transport } from '@connectrpc/connect'

import { BatchService } from '@lesomnus/payday/pdpb'

import { NodeService, ProducerService, ReaderService, RelayService } from '../gen/shale/host_svc_pb.js'
import { LaminaService } from '../gen/shale/lamina_svc_pb.js'
import { HolderService } from '../gen/shale/payday/holder_svc_pb.js'
import { TenantService } from '../gen/shale/payday/tenant_svc_pb.js'
import { SetService, SourceService } from '../gen/shale/set_svc_pb.js'
import { SiteService } from '../gen/shale/site_svc_pb.js'
import { DeviceService, SinkService } from '../gen/shale/storage_svc_pb.js'

export interface App {
	readonly tenant: Client<typeof TenantService>
	readonly holder: Client<typeof HolderService>
	readonly site: Client<typeof SiteService>
	readonly set: Client<typeof SetService>
	readonly source: Client<typeof SourceService>
	readonly lamina: Client<typeof LaminaService>
	readonly producer: Client<typeof ProducerService>
	readonly reader: Client<typeof ReaderService>
	readonly node: Client<typeof NodeService>
	readonly relay: Client<typeof RelayService>
	readonly device: Client<typeof DeviceService>
	readonly sink: Client<typeof SinkService>

	/** Several writes as one transaction; see `payday/batch`. */
	readonly batch: Client<typeof BatchService>
}

export function app(transport: Transport): App {
	return {
		tenant: createClient(TenantService, transport),
		holder: createClient(HolderService, transport),
		site: createClient(SiteService, transport),
		set: createClient(SetService, transport),
		source: createClient(SourceService, transport),
		lamina: createClient(LaminaService, transport),
		producer: createClient(ProducerService, transport),
		reader: createClient(ReaderService, transport),
		node: createClient(NodeService, transport),
		relay: createClient(RelayService, transport),
		device: createClient(DeviceService, transport),
		sink: createClient(SinkService, transport),
		batch: createClient(BatchService, transport),
	}
}
