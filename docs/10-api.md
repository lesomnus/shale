# Shale — API

## 33. API

### Control Plane

```text
POST /objects/allocate
POST /sets/{id}/allocate        (all members, up to a horizon)
POST /objects/{id}/reallocate
POST /objects/{id}/failure

GET  /objects/{id}
GET  /sources/{id}/objects?from=&to=
GET  /sets/{id}/objects?from=&to=

PUT  /sets/{id}                 (register a set and its members' ordinals)

POST /nodes/{id}/heartbeat
POST /sinks/{id}/gc-proposal
POST /sinks/{id}/deleted

PUT  /objects/{id}/hold
```

Commits arrive through a durable event queue (`ObjectStored`) rather than an
HTTP callback.

### Storage Node

```text
PUT    /objects/{object_key}     (Upload-Offset, Upload-Complete, Upload-Length
                                  or Shale-Size-Hint; resumable, may be chunked)
GET    /objects/{object_key}     (Range supported)
HEAD   /objects/{object_key}     (upload offset and completeness, or object metadata)
DELETE /objects/{object_key}     (CP-approved deletions only)
```
