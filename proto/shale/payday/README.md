# Generated

Everything in this directory is written by `pd gen`, and editing it lasts
until the next one -- the past's lock apart, below.

- `*.proto` are payday's own entities, copied in whole. They are copied
  rather than imported because everything generated from them has to be one set
  of types in one Go package: an ent schema in another package cannot have an
  edge to one here, and the wall between tenants is an edge.
- `event.proto` and `fact.proto` are here when the app declares a past
  -- `option (payday.app) = {past: {event: 17, fact: 16}};` -- numbered as it
  declares, and go when the declaration does.
- `*_svc.g.proto` are the service contracts generated from them, the same
  as the ones beside your own entities.
- `past.lock`, with a past, is the record of every event message the app
  has declared, and the one file here that is not written over: `pd gen` only
  adds to it, and refuses a schema that lost or changed what it holds, since
  a row of an event is read back by name for as long as it is kept. Commit
  it. Taking something out of it by hand is right only for a type no
  deployment ever wrote a row of.

To change what payday's entities hold, write an overlay in `proto/ext/payday/`
-- one file per entity, named after it, and it is merged in here on the next
generation.
