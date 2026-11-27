# Shale — Security

## 33. Authentication and Authorization

### 33.1 Trust model

Two kinds of principal exist. **People** sign in and are payday Holders,
known by roster.
**Hosts** are machines the operator owns; each is a row of its own, identified
by its hardware and authenticated by a certificate the Control Plane issues
when an operator adopts it ([§33.4](#334-joining-and-adoption)). Nothing else
holds a credential.

| Principal | Kind | Authenticates to | With | May |
|---|---|---|---|---|
| **Producer** | host, tenant entity | tenant API | host certificate (mTLS) | negotiate and allocate for its set; upload with the access tokens it is given |
| **Reader** | host, tenant entity | tenant API | host certificate (mTLS) | query and read laminae of its sites |
| **Person** | person (Holder) | tenant API | a session, after roster vouched for them | read the sets, cameras, live video, laminae and sites of the sites it sees (*Sites*, below), and its tenant's hosts where roster does not say what people may change; where it does (below), the basics, and the rest -- the hosts' reads among it -- as far as roster grants them |
| **Person, by a key** | person (Holder) | tenant API | `Authorization: Bearer rt_…`, a key for Shale roster minted them (below) | what the person may, and of it only the methods the key names |
| **Tenant admin** | person (Holder) | tenant API | a session, after roster vouched for them | manage its tenant's sites, sets, sources, producers, readers, and retention dates: every person of the tenant where operators are a tenant; where roster says, a person it grants all of Shale (`/shale.*/*`) |
| **Storage Node** | host, global entity | cluster API | host certificate (mTLS) | report its own devices and sinks; push events; propose GC |
| **Relay** | host, global entity | cluster API | host certificate (mTLS) | report its load; serve producers and viewers that carry tokens ([§39](16-relay.md#39-relay)) |
| **Satellite** | host, global entity | cluster API | host certificate (mTLS) | report its memory; learn the producers assigned to it and the routes to nodes; forward those producers' uploads to the node each put token names ([§41](18-satellite.md#41-satellite)) |
| **Cluster operator** | person (Holder) | cluster API | a session, after roster vouched for them | adopt nodes; manage tenants, sinks, keys, policies: the people of `control.cluster_tenant`, or the people of the tenant `auth.operators` names, as far as roster grants them there |
| **Control Plane** | — | everyone | CP certificate, signing key | placement, authorization, metadata; calls nodes ([§35.7](12-api.md#357-storage-node-control-api)) |

- **The tenant wall.** Every tenant-owned entity is behind payday's wall: a
  caller sees and changes only its own tenant's rows. A single-organization
  deployment is a cluster with one tenant, so the wall is always on, and
  multi-tenancy needs no code change
  ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Sites.** Within a tenant, what a person or a reader sees of cameras is
  the sites it may see (payday's second axis, field 3), decided in one
  place, `Sites.Of` (`server/core/gatepolicy.go`):

  | caller | sees |
  |---|---|
  | a person who sees every site | every site, and every row in no site |
  | any other person | the sites said `EVERYBODY`; the sites said `COMPANY` if they are of the company; the sites whose `SiteMember` rows name them, whatever they say |
  | a reader | every site with `all_sites`; otherwise the sites it is a member of, and no site by its kind: a reader is a host and nobody |
  | a producer | the site of its set, or every site when its set has none |
  | a camera worker | the site it was adopted for, or none |
  | a node, a relay, a satellite | every site; they read no camera of a tenant's |

  **Who sees every site**: where roster says what people may change
  (`auth.operators`, below), whoever roster grants all of Shale
  (`/shale.*/*`), and nobody by their row; where it does not, whoever's row
  has `all_sites` (the tenant's first person, and whom `holder add` or a
  patch gives it). A site's kind is written when it is added and by a patch,
  by whoever may write a site; one said nothing, and every site from before
  the field, is `MEMBERS` ([§7](02-data-model.md#7-source-set-zone-epoch)).
  Who is of the company is said in [§7](02-data-model.md#7-source-set-zone-epoch).

  **Of the company** is roster's answer, asked as Shale in the tenant: the
  person's addresses (`EmailService.List` filtered to them, the address
  and `date_verified` alone selected) and the tenant's mail domains
  (`MailDomainService.List`). A mail domain is the tenant's own statement
  about its own people, which roster does not prove, and that is enough
  here: it only sorts people inside that tenant. Shale keeps the answer and
  never the address -- each is compared and dropped, in no log line, no
  error, nothing stored -- for `auth.company.ttl` (10 minutes), longer than
  a grant is kept because each reading of an address is written to
  roster's access record; the tenant's mail domains as long. It **fails
  closed**: an answer roster could not give is no, remembered 5 s and
  logged once, and a roster that refuses the two reads
  (`PERMISSION_DENIED`, Shale not allowed them there, *With a roster
  elsewhere* below) is no with a line naming them. It is asked only when
  it can matter: when the person's tenant has a site said `COMPANY`, not
  erased, that a membership does not already show them; a tenant with no
  such site has nobody's address read. Where roster runs inside Shale
  everybody of the tenant is of the company, and roster is not asked.

  **A row in no site** -- a set added without one, its sources, laminae,
  attempts and producer -- matches no site, so it is seen by whoever sees
  every site and by nobody else; no kind and no membership reaches it. A
  camera is shown to others by its set being in a site, which is said when
  the set is added; a set that exists cannot yet be moved into one.

  The answer is kept per caller for 10 seconds and not forgotten when a
  site or a membership is written, so a kind changed, a membership added
  or taken away, is seen within them (a grant at roster within
  `auth.operators.ttl` besides): it is an administrator's change, and
  somebody who loses a site reads it for those seconds at the most. A
  person's session, their keys without a list, and an app viewing in their
  name ([§33.8](#338-viewing-on-a-persons-behalf)) are the person, and see
  the same. A producer is issued *within* the site of the set it was
  adopted for, so it is useless anywhere else
  ([§7](02-data-model.md#7-source-set-zone-epoch)).
- **Two surfaces.** Anything that must span tenants (Storage Nodes, cluster
  operators) uses the cluster API, which is a separate entry point on the
  internal network. The tenant API contains no path that sees more than one
  tenant ([§35.2](12-api.md#352-two-api-surfaces)).
- **People are roster's.** Who somebody is, the tenant they are in, and how
  they sign in are rows of [roster](https://github.com/lesomnus/roster), the
  payday app that holds people: it verifies a password (with lockout and a
  second factor), links the identities an external provider knows them by,
  and owns `sub`. Shale keeps a `Tenant` and a `Holder` row of its own for
  what is Shale's about them, the sites they may see, **anchored on roster's
  identifiers**: payday's Tenant and Holder are the same entities in both
  apps, so `Holder.id` here is the `sub` every product knows the person by.
  The rows are made **on demand**, the first time a credential roster
  vouched for names the person, and no other way: `holder add` makes the
  person at roster and then here, and a tenant is made at roster and its
  row here follows its first person. Where roster does not say what people
  may change, the first person of a tenant's row has `all_sites`, so they
  see every site; everyone after sees what the sites' kinds and members
  give them (*Sites*, above).
- **Signing in with a password** is `POST /session {tenant, alias,
  password}` on the tenant API's HTTP listener: Shale asks roster
  (`VouchService.Verify`), and mints its own session cookie once roster said
  yes. The cluster API's HTTP listener takes the same request from an
  operator and nobody else (below).
- **A session** is sealed into its cookie under a key derived from the KEK,
  one per surface, and lives at most 24 hours; nothing on the server can
  end the cookie itself before then. What ends it sooner is its person:
  every call a session makes, on either surface and however it was minted,
  asks roster about the person it names (`HolderService.Get`, kept for
  `auth.operators.ttl`, 30 s, whether or not `auth.operators` is set) and
  is refused once they are suspended, erased or gone, or were **signed out
  everywhere** at or after the session began (a password roster issues
  afresh does that too). It fails
  closed like a grant: while roster cannot be asked no session is served,
  the call is refused as `UNAVAILABLE`, and the failure is remembered for
  5 s. Signing out (`DELETE /session`, `/sso/logout`) clears the cookie in
  that browser; a copy of it elsewhere lasts until the person is signed out
  everywhere at roster. Shale holds no password and no verifier. The `shale` CLI signs in the
  same way (`shale login`) and keeps the session.
- **Signing in through the issuer (single sign-on).** A company whose people
  have no roster password -- they arrive at roster through their own
  directory (an Entra `Connection`), and roster with Ory Hydra in front is
  the OIDC issuer -- signs them in to Shale through that issuer. With
  `auth.oidc` set (§36.1) each HTTP listener is a **relying party** of it,
  in the shape roster's `examples/product` is (roster's
  `docs/relying-party.md`): the control plane runs the authorization code
  flow itself and ends it in **the same session cookie** a password mints.

  | route (both listeners) | what it does |
  |---|---|
  | `GET /sso/login?next=` | a flow begins: a random `state`, a `nonce` and a PKCE verifier (S256), sealed under a key derived from the KEK into a short-lived `__Host-shale_sso_<surface>` cookie (so any control plane can finish what another began); the browser goes to the issuer's `authorization_endpoint` with the redirect `<origin>/sso/callback` |
  | `GET /sso/callback` | the cookie's `state` compared with the query's, the code exchanged with the verifier and the client secret (`client_secret_basic`), the `id_token` verified against the issuer's discovery document and JWKS -- signature, `iss`, `aud` (this client), expiry -- and its `nonce` compared. `sub` is the identity and nothing else: roster's `sub` is a `Holder.id`, looked up at roster in each tenant served here, with the key it serves it with (`HolderService.Get`); somebody in no tenant served here, or disabled, is refused. On the cluster listener the person must be an operator. Their rows are made or refreshed (`Provision`), and the session is minted, keeping the `id_token` for one thing: the logout hint |
  | `GET /sso/logout?next=` | **two hops**: this listener's session ends, then the browser goes to the issuer's `end_session_endpoint` (`https://sso.hday.dev/oauth2/sessions/logout`) with `id_token_hint` and `post_logout_redirect_uri` = the origin it came from plus `/`, so the issuer forgets the browser too and sends it back. Without a hint, nothing is asked back and the browser ends on the issuer's page |
  | `POST /sso/token {id_token}` | the CLI's sign-in, below |
  | `GET /session`, `GET /session/ways` | who this browser's cookie names here; whether this listener takes a password, and the issuer |

  `next` is followed only to an origin this deployment answers at or names
  (`auth.oidc.*_origin`, `http.origins`), so a sign-in is not an open
  redirect. A refusal shows a person one sentence and the log the reason,
  never the token. **`auth.sso_only`** turns the password off: `POST /session`
  with one is refused before it is read, `GET /session/ways` says so, and the
  console draws only *Sign in with SSO*. It is refused at start without an
  issuer, since then nobody could sign in.

  **The CLI** has no browser and holds no secret, so it uses the device
  grant (RFC 8628) against the issuer, with a public client
  (`auth.oidc.device_client_id`): `shale login --sso` asks the control plane
  for the issuer and the client (`GET /session/ways`), asks the issuer for a
  code, prints the address and the code to confirm in any browser, polls
  until somebody has, and posts the `id_token` it is handed **once** to
  `POST /sso/token` on each surface it is configured for. The control plane
  verifies it for the CLI's client, requires it to be at most 10 minutes old,
  maps `sub` as above, and mints the ordinary session, which the CLI keeps as
  it keeps a password's. It is **operators only** for now: anybody else is
  refused. A session rather than the bearer token on every call, because
  that is the design every other credential here follows -- the token is used
  once to learn who this is, which is roster's guidance, and Hydra's access
  tokens are opaque to anybody without its admin API -- and a session is
  what Shale already knows how to end.
- **A person's key for Shale** is for a script. roster mints it the
  person, **for the app Shale** (`ApiKey.app`), with some of Shale's
  methods they hold there and an end if they give one: on the console's
  *Tokens* page, on roster's account page (*keys*, *mint a key*, Shale as
  the app), by `roster me issue-key --app shale`, or by a terminal's
  sign-in naming the app (`roster sign-in --app shale`, where the
  deployment turned `account.terminal` on). It is ended at roster too: on
  the *Tokens* page, on the account page's *revoke*, by the person, or by
  Shale's administrators there. The tenant API takes it as `Authorization: Bearer rt_…`, read
  after the session cookie and before the certificate; it is told apart by
  its prefix before anything is asked of roster, and any other bearer goes
  the way it went, to the next credential or to nobody. The cluster API
  does not take one.

  | credential | minted by | ended by | checked |
  |---|---|---|---|
  | a person's key for Shale (`rt_`) | roster, as the person (`ApiKeyService/Issue`: the console's *Tokens* page, the account page, `roster me issue-key --app shale`, a terminal's sign-in) | roster: the *Tokens* page, the account page's *revoke*, `ApiKeyService/Erase` by the person or by Shale, its own end, the person suspended or erased | `TokenService/Introspect`, asked as Shale in the tenant |

  **Where a key is made, listed and ended from Shale, and by whom.** Four
  RPCs of Shale's own on the person's service, `HolderService`'s
  `BeginKey`, `PollKey`, `ListKeys` and `EndKey`, which the console's
  *Tokens* page calls. Each is about the caller and nobody else -- no
  request names a person -- so every person may call them for themselves
  on the tenant API whatever roles they hold, `reads: granted` included,
  and nobody calls them about another person: an operator ends somebody
  else's key at roster. The cluster API serves none of them.

  | | roster elsewhere | roster in this process |
  |---|---|---|
  | **made** (`BeginKey`, `PollKey`) | a terminal's sign-in for one key at roster's account app (`auth.roster.account`, with `account.terminal` on there): Shale's server posts `/device/begin` naming the app `shale`, the key's name, methods, end and scope, and answers the caller where their browser says yes at the identity service, and a handle; `PollKey` posts `/device/poll` and answers *not yet*, *refused*, *lapsed* or the token, once; *refused* is the person's no, or their yes that roster would not make a key of (a method they do not hold at Shale, a name they already use), which roster's poll answers at once (`refused`, with `error_description`) and `PollKey` passes on in roster's sentence. Nothing is made without the person's yes at roster's front door, held there to what they hold at Shale | `ApiKeyService/Issue` as the person, in this process, answered at once by `BeginKey`: the console is roster's front door here. Held to what they hold at Shale, as roster holds it |
  | **listed** (`ListKeys`) | `ApiKeyService/List` asked as Shale, by the caller and the app `shale`: name, methods, end, last used, made, scope; beside them what the caller holds at Shale (`HolderService/Reaches`' `everywhere`, as the tenant API's methods), which is all a key of theirs may be given, and of that what a key that carries a list of cameras may be given (`narrowed`) | the same |
  | **ended** (`EndKey`) | read as Shale and found the caller's and for Shale (anything else is `NOT_FOUND`), then `ApiKeyService/Erase` as Shale; answers how long what was opened with it lives on, `auth.operators.ttl` | the same |
  | **by a key** | lists and ends where its methods name `ListKeys` and `EndKey`, and begins one where they name `BeginKey` and `PollKey`, since the yes is the person's at roster's front door | lists and ends as elsewhere, and **does not begin one** |

  **A handle** carries nothing in the clear and Shale holds nothing of the
  person's between the two calls: what the account app answered -- the
  device code, which is the whole of what a poll is held to -- is sealed
  (AES-GCM, under a key derived from the KEK) with the person, their tenant,
  when the code lapses and a digest of the credential that began it (the
  session's cookie, or the key), and `PollKey` answers only a call made
  with the same: another session of the same person presenting it is
  `PERMISSION_DENIED`. The token is answered once and never kept, logged
  or put on the trail by Shale; ending one is on roster's trail as
  Shale's write.

  **Making a token needs a role of Shale's**, since a key is held to what
  its person holds at Shale, and so, on a deployment that carries its
  roster inside, `auth.operators`: without it no role of Shale's exists,
  and the *Tokens* page says so and what an operator sets. With
  `auth.operators` and its reads everybody's, everybody holds one: the
  basics, which Shale writes at roster and puts each person in the group of
  (*Everybody holds the basics*, below), so a person nobody bound anything
  makes a token of the basics, and a token made before a camera is theirs
  to see sees it once it is.

  **The one thing a key is refused that its person is not** is making a
  key where roster runs in this process. There the console is roster's
  front door, so a person's call to `BeginKey` *is* their yes; a key
  presenting the same call would be a second credential saying yes for
  them with nobody asked, and a key that mints keys is as wide as its
  person -- what it mints is held to what the person holds, not to the
  key's own list (roster's *a key that mints keys*). Where roster is
  elsewhere the yes is asked of the person at roster's front door whoever
  begins it, so a key may begin one there.

  **What roster's operator has run** for listing and ending: nothing new.
  `roster app install --serves '/shale.*/*'` binds Shale's holder
  `app-administrator` for itself, which names `ApiKeyService`'s `Get`,
  `List` and `Erase`, and the deployment key is answered with it under
  `roster-at`; the key's own `--allow` does not grow. A tenant installed
  before those verbs were among `app-administrator`'s holds one naming
  fewer, and Shale is answered `FAILED_PRECONDITION` saying so until that
  role names them. Making one from the console where roster is elsewhere
  needs the account app's address here and `account.terminal` there
  ([§34](11-deployment.md)).

  Shale asks roster who it is as itself (`/payday.TokenService/Introspect`,
  which `app install --allow` names already), in a tenant, since a key names
  none and Shale's deployment key holds nothing without `roster-at`: in
  each tenant this deployment serves, the way somebody is found by `sub`
  -- one asking where the configuration says the tenant (a tenant key, or
  one per tenant). roster answers a key **for Shale** with its person,
  their tenant, its methods and its end, and anything else -- a key of
  roster's own, a key for another app, one ended, one of somebody
  suspended -- as `no such token`, and so does Shale. The caller is then
  the person, resolved to their row as a sign-in resolves them (made at
  their first arrival, read from roster by identifier), and the call is
  held to **both**: payday refuses a method the key does not name
  (`PERMISSION_DENIED`, *this credential is not for that*), and the gate
  decides for the person as for a session, `auth.operators` and its
  `reads` included. Nothing is closed to a key that is open to the person
  but one: making a key where roster runs in this process (above) -- and,
  for a key that carries a list of cameras, everything a list does not
  confine (below).
  **A key that watches names `Get` beside `Watch`**: payday reads each row
  a stream sends as the entity's `Get` would (`/shale.SetService/Watch`
  and `/shale.SetService/Get`), so a key naming `Watch` alone opens a
  stream that sends nothing.
  What roster said is kept **per key**, under a digest of it, for
  `auth.operators.ttl` (30 s): the window in which a grant taken away at
  roster already lands here, so a key ended at roster is refused within it
  and no later; never beyond the key's end; and forgotten at once for a
  person roster's sync stream says was signed out everywhere, suspended or
  erased, where Shale's key may follow it (`/roster.SyncService/Watch`;
  the embedded roster has none, and then it is the TTL). A roster that
  cannot be asked is `UNAVAILABLE`, remembered for 5 s. **What is opened
  with a key ends with it**: a stream (`Watch`) is cut, `UNAUTHENTICATED`,
  at the key's end, by payday, which reads the end roster answered; when
  its person is forgotten as above; and when the key is **ended at
  roster**, within `auth.operators.ttl`: while a key has a stream open,
  roster is asked about it again each time what it said runs out, and
  `no such token` cuts that key's streams and no other key's of the same
  person. A roster that cannot be asked at that moment cuts nothing -- the
  stream is cut when roster answers that the key is none, or at the key's
  end -- and nothing is asked about a key with no stream open. The view tokens `Live` signs, the
  read tokens of `Timeline` and an export's ticket end no later than the
  key, and a relay ends a live view at its token's end
  ([§39.4](16-relay.md#394-viewers)).

  **A key narrowed to some cameras.** A key may carry a list of cameras: a
  `Reach`, a person's own row of a name, sites and sources
  ([§7](02-data-model.md#7-source-set-zone-epoch)), which they add, read,
  change and erase on `ReachService` for themselves and nobody else, of
  sites they are a member of and sources they can read -- each resolved
  through their own walled view when it is written, as a delegation's
  sets and sources are. The key carries it by its identifier, the UUID
  written out, as the scope roster keeps on a key (`ApiKey.scope`,
  `BeginKey`'s `scope`), and roster answers it back at every
  `Introspect`; payday puts it on the frame of every call made with the
  key. `BeginKey` refuses a scope that is no list of the caller's
  (`INVALID_ARGUMENT`, *no list of cameras of yours*), and a key whose
  scope is set and whose methods name one outside the table below,
  naming those methods. A key with no scope reaches what its person
  reaches, as before.

  What a call made with such a key may see is **what its person may see
  and, of that, what the list names**: every source of each site it names
  and each source it names -- sites and sources are one rule, so a list of
  a site and of a camera of another site reaches both -- and one that names
  nothing reaches nothing. It is a scope beside the wall and the sites
  (`cmd/serve.go`: `bare.Scopes{pd.Wall(), pd.Grouped(…), Reaches}`), not
  the tenant's second axis, so every generated read and stream honours
  it:

  | path | held by |
  |---|---|
  | `SourceService` `Get`, `List`, `Watch` | the scope: a source named, or of a site named; another is `NOT_FOUND`, as a row outside the wall |
  | `SetService` `Get`, `List`, `Watch` | the scope: a set of a site named, or with a source named among its members |
  | `LaminaService` `Get`, `List`, `Watch`; `AttemptService` `Get`, `List` | the scope, by the source each names (an attempt by its lamina's), or its site |
  | `CameraBindingService` | refused to such a key at the door, though it names a source: the scope covers a binding by its source too, but nothing is offered to a narrowed key without a test of its path |
  | `SetService/Live`, `SourceService/Live` | the sources are read through the scope, so a view token is signed for a source the list reaches and for no other; the relay trusts the token's `source` |
  | `LaminaService/Timeline` | the sources through the scope; the laminae, read raw, are cut to those sources in the query and again per row, and read tokens are signed for theirs alone; the nodes trust the token |
  | `LaminaService/Export` | the source through the scope, so a ticket is signed for a source the list reaches alone, and the download, which trusts its ticket, needs nothing more |
  | a `Watch` | a row the list does not reach is never sent; one that leaves it -- the list changed -- is said once, at its next write, as gone (an item with no value), as a row that leaves the caller's sight is, and then not again |

  **A key that carries a list calls only what the list can hold**: the
  methods below, and is refused every other at `BeginKey` and at the door
  (`PERMISSION_DENIED`, *this key carries a list of cameras*), whatever
  its methods say -- `HolderService`'s own keys and `ReachService`
  included. A key said to reach two cameras that could erase a third
  source, read a producer's inventory or change a policy would not be
  narrowed: the scope confines what it can be shown to confine, and
  nothing else is let through on its word.

  | service | methods |
  |---|---|
  | `SourceService` | `Get`, `List`, `Watch`, `Live` |
  | `SetService` | `Get`, `List`, `Watch`, `Live` |
  | `LaminaService` | `Get`, `List`, `Watch`, `Timeline`, `Export` |
  | `AttemptService` | `Get`, `List` |

  A method joins this table only with a test of its path.

  The list is read at the call, by its identifier, **never per person**,
  and remembered no longer than what roster said of the key is
  (`auth.operators.ttl`, 30 s); writing it here forgets it at once in the
  process that wrote it, so a list changed is what its keys reach from
  their next call, and within `auth.operators.ttl` at another control
  plane. It must be the key's person's at the time of the call: a list
  **erased**, or another's, leaves the key reaching nothing -- a list is
  empty, and a call that names a source is `FAILED_PRECONDITION`, *this
  key's list of cameras is gone* -- and nothing else of the key changes;
  its person ends it, or makes another with a list. Sessions, hosts, keys
  without a list and an app's delegated viewing
  ([§33.8](#338-viewing-on-a-persons-behalf)) are as they were.
- **roster runs in the control plane's process or elsewhere.** With nothing
  configured, `shale serve all` and `shale serve control` run roster inside
  the process on a database of its own, reachable from that process only,
  the way the node and the relay are run in one process ([§34.7](11-deployment.md#347-single-machine));
  `shale init` then makes the cluster operators' tenant and the first tenant
  there, each with its first person and a password shown once (inside
  Kubernetes, put into a Secret of its own instead of the Job's log,
  [§34.5](11-deployment.md#345-kubernetes)), and gives
  people passwords through `shale holder issue-password`. With
  `auth.roster.addr` set, roster is a deployment of its own and Shale acts on
  it as the holder each tenant it serves gave it there, with the key its
  operator minted for it (`auth.roster.key`): a **deployment key** (`rk_`)
  is answered in every tenant that **nominated** it -- Shale lists the
  key's own nominations (`NominationService.List`, which answers a key
  about itself and nobody else) and names the tenant beside every other
  call with `roster-at` -- so a tenant is served by `roster app install`
  at roster and not by anything here; a **tenant key** (`rt_`) is its own
  tenant alone. A deployment holding a tenant key from each of several
  tenants lists them by alias instead (`auth.roster.keys`). Tenants and
  people are made at roster, and a tenant this deployment has no way into
  is one it does not serve.
  Cluster operators are the people of the tenant `control.cluster_tenant`
  (`cluster`), which the cluster API's policy lets see every tenant, unless
  roster says what people may change (below). A
  deployment made before roster held its people runs `shale identity
  migrate` once after the upgrade: its tenants and people go into the
  embedded roster with the identifiers they have, and everyone gets a new
  password, shown once.
- **What people may change is what roster grants them.** A company that
  signs its people in through its own roster tenant has no second tenant to
  put operators in, and should not need one: roster already says who may do
  what, as **roles** naming methods, bound to a person or to a group they
  are in. What somebody may do at Shale is a **role of Shale's** at roster
  (`Role.app` is the holder Shale is answered as in the tenant), bound to
  the person or to a group of Shale's: roster's gate reads roster's roles
  alone, a role of roster's names roster's methods and nothing of
  Shale's, and a role of an app's is held in no team. With `auth.operators` set (§36.1) -- the tenant whose people
  operate the cluster, which is one this deployment serves -- a person may
  call a method **if and only if a pattern roster grants them across their
  tenant covers it on its own**: Shale asks roster's data plane
  (`HolderService.Reaches`, asked as Shale and so answered of Shale's
  roles alone) and reads `everywhere`, not `methods`, since
  `methods` is roster's gate's union, which roster's own wall narrows for
  a grant bound at one of its sites or held in a team, and Shale has no
  such narrowing to apply. A role of Shale's naming `/shale.*/*` is all of
  Shale; one naming `/shale.SourceService/*` is the cameras and nothing
  else. Shale
  keeps the answer for `auth.operators.ttl` (30 s), and **fails closed**:
  while roster cannot be asked, nobody changes anything, the call is
  refused as `UNAVAILABLE` with the reason, the log says why, and the
  failure is remembered for 5 s so a roster that is down is not asked once
  per call. Taking a role away ends it within the TTL.

  On the **cluster API** the person must be of the operators' tenant and
  granted **all of Shale** there (`/shale.*/*`), asked on every call and
  not only at sign-in: a customer's administrator granted all of Shale in
  their own tenant is not an operator of the cluster, which sees every
  tenant, and neither is a person of the operators' tenant granted one
  service -- the cluster API reads and writes across tenants, so a grant of
  `/shale.SetService/*` there would be every tenant's sets. A narrower
  grant is used on the tenant API, inside the person's own tenant.
  `control.cluster_tenant` is then not consulted at all: its people are
  not operators. Signing in to the cluster listener -- with a password or
  through the issuer -- and through the CLI takes the same: anybody else is
  refused at the sign-in, and a session the tenant listener minted does not
  open on the cluster listener, since each surface seals its sessions under
  a key of its own. What no caller writes on the tenant API -- laminae,
  attempts, and the general writes of hosts, devices, sinks and keys
  ([§35.2](12-api.md#352-two-api-surfaces)) -- is closed to operators on
  the cluster API too, and `DelegationService` is
  not served there at all ([§33.8](#338-viewing-on-a-persons-behalf)).

  **It is a grant and not a team on purpose.** This used to be the members
  of a team at roster. A team membership is organisation there and not
  permission -- one with no role grants nothing -- so roster lets anybody
  who may write memberships write one, and reading it as "may operate the
  cluster" was a permission roster did not know existed and could not
  guard. A grant is guarded: nobody binds a role naming methods they do not
  hold. `auth.operators.team` is refused at start with what to do instead.

  The same setting changes the **tenant API** for people. Every person of
  a tenant calls **the basics** -- the reads and views about cameras and
  sites: `Get`, `List` and `Watch` of `SetService`, `SourceService`,
  `LaminaService` and `SiteService`, `Live` of sets and sources, `Timeline`
  and `Export` of laminae, `Get` and `List` of `AttemptService`, eighteen
  methods (`BasicMethods`, `server/core/gatepolicy.go`: what a key that
  carries a list of cameras may call, and the sites) -- and anything else
  only as far as roster grants them: adopting a producer, a set, a source,
  a retention date, a site, a person; people, site membership, the trail,
  passwords; and **the hosts' reads**, `Get`, `List` and `Watch` of
  `ProducerService` and `ReaderService`, which the console's *Hosts* page
  and the producers' state on *Cameras* read. Those are not the basics: a
  producer's and a reader's rows show the machine's hardware identity, its
  hostname, the address it joined from, its certificate or request,
  version and status, and a reader's are not narrowed by site at all, so
  they are an operator's to hand out, a role of Shale's naming them as any
  other call is. With `auth.operators.reads: granted` the basics are as far
  as roster grants them too, so a person granted nothing reads nothing;
  that is how people who should see cameras only through an app beside
  Shale are kept to it
  ([§33.8](#338-viewing-on-a-persons-behalf); with
  `auth.delegation.persons: tenant` an app may view for any of them, and
  the grant decides the console alone). Hosts themselves -- a producer's,
  a reader's calls -- are unchanged. Without
  `auth.operators` the rule is the one before it, and it is what an
  embedded roster, `--dev` and the tests run by default: every person
  administers their own tenant, except where an act hands somebody a
  secret. Only a person who sees every site issues a password. **Nobody
  recovers a camera's credential** (`CameraCredentialService.Resolve` as a
  person), since its answer is the camera's administrator, management or
  streaming password in plaintext. That is an operator's act, and here
  nobody is named one. This holds for a site's member, a person who sees
  every site and the tenant's first person alike. They are refused with
  the reason, and a deployment that needs recovery names operators and
  grants the method
  ([camera credentials](camera-credentials.md#prepare-import-and-recover)).
  A camera worker and a producer still receive the credential their job or
  source pins. A new RPC whose answer hands a person a secret goes beside
  these two in `TenantPolicy.May`.

  **Sites, where roster says.** Whoever roster grants all of Shale sees
  every site, and nobody else does, whatever their row's `all_sites` says:
  it is not read here, and a person's row is made without it, the tenant's
  first person's too. Everybody else sees the sites a site's kind and its
  members give them (*Sites*, above), and the wall narrows their reads to
  those; it never widens what they may call. Before this, every person's
  row was made with `all_sites` on and read: such a person, not granted all
  of Shale, now sees what the kinds give them, and with no site said
  `EVERYBODY` or `COMPANY` and no membership, no camera
  ([§32.3](09-operations.md#323-upgrading-to-sites-that-say-who-sees-them)).

  **Everybody holds the basics, at roster too.** A token is made at roster,
  and roster mints a key only for methods its person holds at Shale, so
  what the gate lets everybody call is written down at roster as well:
  where `auth.operators` is set and its reads are everybody's, Shale makes,
  as its own holder in each tenant it serves,

  - **`shale-basics`**, a role of Shale's (`Role.app`) naming exactly the
    eighteen, and
  - **`shale-people`**, a group of Shale's (`Group.app`),
  - with the role **bound to the group**, in no site.

  Each says in its `desc` that Shale made it. **Shale makes them once and
  then leaves them to whoever operates the deployment**, as the roles an
  app ships are the tenant's once written (roster's `docs/apps.md`, *The
  roles an app ships*). They are looked at **at start**, in every tenant
  served, and **when a tenant is first served** after it: each read, and
  made if absent -- the role with the build's list -- unless Shale made them
  there before. Once Shale has made them in a tenant it says so on its own
  row of the tenant, the label `shale.basics`, and while that is there it
  makes none of the three again: one that is absent was removed by an
  operator. (roster cannot tell it: a role, a group or a binding erased is
  read by nobody after, its alias is free for an `Add` again, and an erased
  binding may be made again, so one removed and one never made answer
  alike.) A second process doing the same at once is no error. A role or a
  group of either name that is not Shale's is left as it is, and said.

  **The role's methods are not written once it is there.** An operator
  changes them at roster, with `roster role widen` and `roster role
  narrow` -- exports not for everybody here, say:

  ```sh
  roster --as <operator> role narrow @acme/shale-basics '{"methods":["/shale.LaminaService/Export"]}'
  ```

  Where they differ from the build's list Shale says so once at start, in
  one line that names what the role has that the list has not and what
  the list has that the role has not, with the commands that would bring
  it to the list, and changes nothing:

  ```text
  WARN basics: shale-basics at roster differs from this build's list of the basics, and Shale leaves it as it is; to bring it to the list, roster's operator runs: roster role widen @acme/shale-basics '{"methods":["/shale.LaminaService/Export"]}' tenant=acme not_in_the_list="" not_in_the_role=/shale.LaminaService/Export
  ```

  Equal, nothing is said. An upgrade that brings a method to the basics is
  said the same way, and is the operator's to widen. What Shale made and an
  operator removed is said once at start too (*… was removed at roster;
  Shale made it once and does not make it again*). Taking the label off
  Shale's row of the tenant (`TenantService` on the cluster API) makes
  Shale make what is absent at its next start.

  **Each person is put in `shale-people`** when a process first sees them,
  on every road -- a session's call, a sign-in by password or through the
  issuer, the CLI's token, a key's first arrival, a delegation's `Start`,
  the plain header of development mode -- with
  `GroupMembershipService/Add` as Shale, and remembered as put in for
  **ten minutes**; after them the next call asks again, and puts back
  somebody taken out. roster answers a second add of the same person
  `ALREADY_EXISTS` (its unique index on a place counts the places not
  erased, so somebody whose place was erased is put in again), and that is
  the ordinary answer and no error. It is never in the way of the call
  that brought them: bounded by two seconds, keeping the tenant's group
  included; a failure logged once and remembered for 30 s, then tried
  again; the call goes on with whatever the person holds. A roster that
  refuses -- Shale installed without `--serves`, which is what binds it
  roster's verbs for its own roles and groups -- is one line in the log
  saying so, said again every ten minutes, and until then a token offers
  only the roles an operator binds by hand. Nothing else changes.

  **Nobody is left out by Shale.** There is no exclusion: somebody taken out
  of `shale-people` at roster is put back when next seen. roster says why
  there is none (`docs/apps.md`, *Reading an app's users*): *taking somebody
  out of Loft is ending their place in its group, or their binding: there
  is no exclusion and no denial, and nothing for a reader to subtract* --
  and *an app that enrols people itself is the one that remembers whom it
  took out*, which Shale does not: somebody who should not use Shale is
  suspended at roster, or the deployment says `auth.operators.reads:
  granted`. **Taking the basics away from the group**, without changing
  `reads`, is removing the binding of `shale-basics` to `shale-people` at
  roster, which Shale does not make again: people are still put in the
  group, which then hands them nothing there, so a token offers them only
  what roles are bound by hand, while the gate still lets them call the
  basics with no role. **Taking them away from everybody** is `reads:
  granted` as well. With `granted` Shale writes none of it and puts nobody
  anywhere, since a group that hands the reads to everybody would undo the
  operator's word; what an earlier `everybody` left is left, since an
  operator may have come to rely on it, and said once at start, as
  roster's to remove. Without `auth.operators` nothing of it runs: every
  person administers their tenant, and roles of Shale's decide nothing.

  **With the embedded roster** Shale installs itself in each tenant as an
  app, as `roster app install` does (roster's `cli.InstallApp`): its holder
  `shale`, `shale-app`, a role of roster's naming what Shale asks roster,
  and `shale-everything`, a role of Shale's naming `/shale.*/*`, both bound
  to it. `shale init` binds the first operator `shale-operator`, a role of
  Shale's naming `/shale.*/*`. Where `auth.operators` is set and its reads
  are everybody's, `shale-basics` and `shale-people` are made in every
  tenant as above, through the same calls as Shale's holder there, which
  `install` bound `app-administrator` for itself.

  **With a roster elsewhere** Shale writes no role but its own two above:
  roster's operator installs it, at the box, once per tenant, and the
  tenant gives its people Shale's other roles. Writing the basics asks
  nothing new of the installation: `--serves '/shale.*/*'` binds Shale's
  holder `app-administrator` for itself, whose `ForAnApp` names
  `RoleService` `Get` and `Add`, `GroupService` `Get`
  and `Add`, `BindingService` `List` and `Add`, and
  `GroupMembershipService/Add`, each held to Shale's own rows; and
  `shale-everything` is what lets it hand the basics out at Shale. The
  key's `--allow` does not grow:

  ```sh
  roster --as <operator> control key add --allow /roster.NominationService/List shale
  roster --as <operator> app install --tenant acme \
    --allow /roster.HolderService/Get,/roster.HolderService/Reaches,/roster.TenantService/Get,/payday.TokenService/Introspect,/roster.EmailService/List,/roster.MailDomainService/List \
    --serves '/shale.*/*' --administer admin shale
  roster --as <operator> role add @acme/shale-operator \
    '{"app": {"slug": {"alias": "shale", "tenant": {"alias": "acme"}}}, "methods": ["/shale.*/*"]}'
  roster --as <operator> binding add \
    '{"role": {"slug": {"alias": "shale-operator", "tenant": {"alias": "acme"}}}, "holder": {"slug": {"alias": "olive", "tenant": {"alias": "acme"}}}}'
  ```

  The key holds, as itself, the list of the tenants that nominated it --
  and `/roster.SyncService/Watch` where apps view on people's behalf
  ([§33.8](#338-viewing-on-a-persons-behalf)), or where a person's keys
  for Shale are to be forgotten the moment they stop being in good
  standing rather than within `auth.operators.ttl` -- and nothing else; in each tenant it is answered as the
  holder `install` made, which may call the methods `--allow` names
  -- `/roster.VouchService/Verify` beside them where people sign in with a
  password, and `/roster.MeService/Get` for a tenant key (`rt_`), which is
  how Shale learns the key's tenant. The last two, `EmailService/List` and
  `MailDomainService/List`, tell who is of the company; without them
  nobody is, and a site said `COMPANY` is seen by the people it names. The
  tenant also names its mail domains at roster, each by name with the
  connection its people sign in through, as roster declares one
  (roster's `docs/operating.md`, *Declaring the rows that are
  configuration*):

  ```yaml
  resources:
    - kind: MailDomain
      tenant: acme
      name: acme.example
      routes: entra
  ``` `--serves` writes `shale-everything`,
  a role of Shale's, and `--administer` names the people of the tenant who
  hold it -- all of Shale, so they administer it there, and operate the
  cluster where theirs is the operators' tenant -- and who write Shale's
  roles and bind them, as `role add` and `binding add` do above. An
  operator is a role of Shale's naming `/shale.*/*`; somebody who looks
  after the hosts where the basics are everybody's, one naming `Get`,
  `List` and `Watch` of `ProducerService` and `ReaderService`; a viewer,
  where `auth.operators.reads: granted`, one naming what the console reads --
  `Get`, `List` and `Watch` of `SetService`, `SourceService`,
  `LaminaService`, `SiteService`, `ProducerService` and `ReaderService`,
  `Get` and `List` of `AttemptService`, `Live` of sets and sources, and
  `Timeline` and `Export` of laminae (`PersonMay`, `server/core/gatepolicy.go`),
  which is also what a key that *views and reads* names on the *Tokens*
  page -- or, wider, `/shale.SetService/*`, `/shale.SourceService/*`,
  `/shale.LaminaService/*` and `/shale.SiteService/*`. Each is bound to the
  person, or to a group of Shale's and the person put in it, and is not
  held through a team:

  ```sh
  roster --as <operator> group add @acme/shale-viewers '{"app": {"slug": {"alias": "shale", "tenant": {"alias": "acme"}}}}'
  roster --as <operator> binding add \
    '{"role": {"slug": {"alias": "shale-viewer", "tenant": {"alias": "acme"}}}, "group": {"slug": {"alias": "shale-viewers", "tenant": {"alias": "acme"}}}}'
  roster --as <operator> group-membership add \
    '{"group": {"slug": {"alias": "shale-viewers", "tenant": {"alias": "acme"}}}, "holder": {"slug": {"alias": "vic", "tenant": {"alias": "acme"}}}}'
  roster --as <operator> holder reaches @acme/vic '{"app": {"slug": {"alias": "shale", "tenant": {"alias": "acme"}}}}'   # what Shale is answered
  ```
- **Storage Nodes and Relays know nothing about tenants or people.** On
  their data planes they trust one thing: a valid CP signature on each
  request or stream. On its control API a node trusts one peer: the Control
  Plane.
- **Viewers** of live video are people in a browser or Reader hosts. Both
  get a view token from the tenant API and present it to a relay; the relay
  cannot tell them apart and need not ([§39.4](16-relay.md#394-viewers)).
- Adopted hosts are trusted to behave. Networks are not: a producer may sit
  behind a WAN or wireless link, so TLS is on everywhere by default
  ([§33.5](#335-tls)).

### 33.2 Access tokens

Every data-plane request a Storage Node accepts, and every stream or
session a Relay accepts, carries an **access token** signed by the CP. The
host follows a single rule: no valid CP signature, no service.

The signature is **Ed25519** over a compact set of claims:

```text
kid          signing key ID
exp          expiry
aud          node_id or relay_id  (a token for host A is useless on host B)
op           put | get            (either also allows HEAD on the key)
             publish | view       (relay: §39)
             feed                 (relay: a satellite's publish, §41.8)
sink_id
lamina_key
attempt_id   (put)
record       (put: the lamina's initial xattr record, §23.1; the node writes
              it as given and fills in what only it knows)
max_length   (put: upper bound on the upload's size, from the agreed profile)
mode, idle_timeout, abandon_timeout
             (put: the agreed upload profile, §12.6; enforced within node caps)
sources      (publish: the source IDs this producer may feed)
live         (publish: the producer's live policy, which the relay applies
              at Hello, §39.3)
rewind_seconds
             (publish: how much of each source the relay keeps for the
              recent window, sized by the CP from the segment duration,
              §39.4)
source       (view: the one source this viewer may watch)
feeder       (feed: the satellite it is for; the relay takes it only from
              that satellite's certificate, §39.7)
actor        the Producer, Reader, or person the token was issued to, and
             its tenant
```

- Carried in `Authorization: Shale <token>`, or in a `token=` query parameter
  for clients that can only pass a URL (e.g. a media server handing a GET URL
  to a player). The URL form is what the design calls a presigned URL. Nodes
  and proxies must not log query strings.
- **Verification** on the node is local and needs no call to the CP:
  signature, known `kid`, `exp` (with `token_skew`, default 1 minute), `aud`
  equals its own node ID, `op` matches the method (`HEAD` is allowed by
  either), `sink_id`/`lamina_key` match the path, and the upload does not
  exceed `max_length`. Ed25519 verification costs ~50 µs, which is negligible
  at hundreds of requests per second.
- **Issued by the tenant API**, inside the wall: `Allocate` issues put
  tokens, `Timeline` issues get tokens, `Live` issues view tokens, and
  `Negotiate`, the producer heartbeat, and `ProducerService.Relay` issue
  publish tokens, all only for the caller's own tenant's sources and
  laminae. The wall therefore extends to the data plane without a node or a
  relay knowing tenants exist. The one token the cluster API issues is a
  satellite's **feed** token, in its heartbeat's answer: a publish token
  for the sources a producer it serves hands it, in that producer's name
  ([§41.8](18-satellite.md#418-live-fan-out)).
- **Lifetime**: put tokens live as long as their allocation
  (`allocation_ttl`, [§12.1](04-write-path.md#121-flow)), and
  `LaminaService.Renew` issues a fresh one for an attempt still in progress.
  Get tokens live `read_token_ttl` (default 1 hour) and view tokens
  `view_token_ttl` (1 hour); the relay ends a session when its token
  expires, so a viewer still watching asks for a fresh one and renews the
  session with it, which takes a token for the same source and actor
  ([§39.4](16-relay.md#394-viewers)). A
  publish token lives `publish_token_ttl` (24 hours) and is checked when the
  producer attaches ([§39.3](16-relay.md#393-from-the-producer)); a feed
  token ten minutes at most, and every satellite heartbeat brings a fresh
  one.
- **Replay** gains nothing. A put token names one key and one attempt, uploads
  are idempotent ([§12.5](04-write-path.md#125-idempotent-uploads)), and a
  complete lamina cannot be overwritten.
- **Revocation** stops the CP from issuing new tokens. Tokens already issued
  stay valid until they expire: at most `allocation_ttl` for uploads (22
  minutes at the defaults, about 80 at the bounds), `read_token_ttl` for
  reads, and `view_token_ttl` for live viewing, sessions already open
  included. This lag is accepted.

`actor` is used by the node for two things only: **per-actor limits**
(`uploads_per_actor`, `sessions_per_actor`,
[§36.1](13-configuration.md#361-configuration-reference)), so one misbehaving
host cannot take a node's whole capacity, and logs and metrics, so it can be
traced. It grants nothing.

### 33.3 Signing keys and rotation

- Signing keys are `SigningKey` rows, global to the cluster. The private half
  is stored **in the DB, wrapped by a key-encryption key** (`KEK`) that lives
  in the CP's state directory or a Kubernetes Secret. Every CP replica reads
  the same row, so a rotation is one transaction and never leaves a replica
  signing with a key the others do not know. Only public halves are readable
  through the API.
- Nodes receive the key set when they are adopted, then **poll**
  `SigningKey` on the cluster API every 30 s, so a new key reaches them
  well before it signs anything: rotation waits until every live node
  reports holding it, and every live satellite, which verifies put tokens
  as a node does ([§41.3](18-satellite.md#413-the-upload-proxy)).
  They **cache the key set in their state directory**, so a node that
  restarts while the CP is down still verifies tokens.
- Each heartbeat reports the key IDs the node holds.
- Rotation (`SigningKeyService.Rotate`, run by the CP leader,
  [§34.9](11-deployment.md#349-events-and-directives)):
  1. add the new key,
  2. wait until every live node and satellite reports holding it,
  3. start signing with the new key,
  4. remove the old key after the longest token lifetime has passed
     (`allocation_ttl` at its bound, or `read_token_ttl`, whichever is longer).

### 33.4 Joining and adoption

The operator does three things over a cluster's life: create it, point each
host at it, and adopt hosts. Keys and certificates are the Control Plane's
business throughout.

**The cluster** is initialized once:

```text
shale init
  → CA, CP certificate, signing key, KEK
  → the first tenant (the organization, in a single-organization deployment)
  → the first cluster operator and the first tenant admin, with one-time
    sign-in credentials
```

That is with roster in the process. With an **external roster**
(`auth.roster.addr`) `shale init` makes the CA, the CP certificate, the KEK
and the first signing key -- and, with `--k8s-secret`, the Secret that
holds them -- and **makes nobody**: no tenant, person, password or role is
written at roster, which is the company's and already has them. It asks
roster for the tenant (the operators' tenant with `auth.operators`, the
cluster tenant otherwise) to make its row here early and to check the key,
and which tenants it serves, and prints what it found; a roster that does
not answer yet is a warning rather than a failure, since nothing it made
depends on roster, and the rows follow the first sign-in. With roster in
the process and `auth.operators` naming the first tenant, init also grants
the first admin all of Shale there (the role `shale-operator`, naming
`/shale.*/*`), so the deployment begins with an operator.

**A host joins on its first run.** Every host runs the Shale binary and is
given only the Control Plane's address:

```text
shale serve storage  --cp https://cp-cluster.internal:7401
shale serve producer --cp https://cp.example.com:7400   # and SHALE_TENANT=acme, with more than one tenant
shale serve reader   --cp https://cp.example.com:7400   # likewise
```

1. The host reads its **hardware identity**: the DMI product UUID
   (`/sys/class/dmi/id/product_uuid`), or on boards without DMI such as a
   Raspberry Pi the device-tree serial number
   (`/sys/firmware/devicetree/base/serial-number`). Both survive a reinstall.
   Only when neither exists does it fall back to `/etc/machine-id`, which does
   not, and it says so in its join request.
2. It generates a key pair in its state directory and connects to the CP. On
   first contact it **pins the CA it sees**; `ca_hash: sha256:<fingerprint>`
   (`SHALE_CA_HASH`) makes that a check instead of a pin, for operators who
   want to rule out a man in the middle on an untrusted network.
3. It calls `Join` with its role, hardware identity, hostname, and a CSR, and
   for a producer or reader the tenant it is meant for (`tenant`, implied in
   a single-organization cluster). A Storage Node adds its interfaces and its
   sinks. `Join` needs no credential and is rate-limited per source address.
4. The CP records a **pending host**: a `Node`, `Producer`, or `Reader` row in
   state `pending`. If a row with the same hardware identity already exists,
   the request is attached to that row instead, shown as "known host, new
   key". The host repeats `Join` every 10 seconds until it is answered with a
   certificate. A pending request nobody adopts expires after
   `join_pending_ttl` (24 hours); the host keeps asking, so it reappears.
   A new Producer gets a random alias from the server's normal name generator,
   with collisions retried by the store. Its hostname remains inventory
   metadata: two machines with the same hostname get different aliases, and
   changing the hostname during a pending join preserves the existing alias.
   Existing pending, adopted, and rejoining producers retain their aliases.
5. An operator **adopts** it, from the console or the CLI, after checking that
   it is theirs. The console shows the hostname, the hardware identity, the
   key fingerprint the host also prints in its log, and, for a re-join, the
   row it will continue:

   ```text
   shale node pending
   shale node adopt <node>
   shale producer adopt <producer> '{"set":{"alias":"<set>"}}'   # site follows the set
   shale reader adopt <reader> '{"sites":[{"id":"<site>"}]}'     # none: the whole tenant
   ```

   Producer adoption may include an explicit `alias`; when it is omitted,
   the producer keeps the alias the CP already assigned it.

6. The CP issues the **host certificate** from its built-in CA
   ([§33.5](#335-tls)) and the next `Join` answers with it, the CA bundle,
   and, for a node, its `node_id` and the key set. The host keeps it once it
   has checked that it is for its own key, names the row the answer names,
   and chains to the bundle, as it checks every renewal; one that does not
   is not stored, and the host asks again. The host is running.

**Renewal is automatic.** A host renews at two thirds of its certificate's
lifetime (`RenewCertificate`, over mTLS with the current certificate), and may
send a new key. It also renews when a heartbeat's answer asks, which the CP
does during a CA rollover ([§33.5](#335-tls)); the answer is the certificate
and the bundle, and the host verifies one against the other before it keeps
either. If a certificate has already expired, for instance a chassis
that sat in storage for a year, the host goes back to step 3 with the same
hardware identity, appears as "known host, new key", and one `adopt` puts it
back under its old name, `node_id`, and sinks; a reader keeps its sites
unless the `adopt` names others. A cluster that trusts its
hardware identities can set `readopt: auto` and skip that click; the default
is `manual`, because a hardware identity is a claim the host makes, not one
the CP can verify.

**Erasing a host** is soft (`shale node erase`, `shale producer erase`,
`shale reader erase`). The CP resolves every mTLS peer to its row and compares
the certificate serial the row records, so an erased host, or a host whose
certificate was replaced, is refused on its next call without a revocation
list. None of the three asks anything first: the row gets its
`date_erased`, and nothing else is written. An erased node leaves DNS, is
sent no directives, and placement skips its sinks. The sinks keep their rows
as they were, still the erased node's, so within `node_down_after` of its
last heartbeat (30 s) every lamina on them reads UNAVAILABLE, as on a node
that died; nothing is deleted or marked LOST, and the bytes stay on the
disks. Erasing does not reach the machine: one left running still answers a
read token issued before, until the token ends (`read_token_ttl`, an hour),
so stopping it is what ends that. Erasing is not undone. The same machine
joins again as a new node, with a new `node_id` (a join looks its hardware
identity up among the rows that are not erased; the erased node's alias is
free to take), and is adopted; the sinks it reports are then pending
adoption, and are adopted onto it by the leader once the erased node has been
silent for `sink_auto_adopt_after` (10 minutes), or at once with
`shale sink adopt` ([§28.3](09-operations.md#283-node-failure-and-device-re-homing)).

A **Reader** host is the machine that runs a media server, or the backend of
a viewer of its own. `shale serve reader` is an agent beside it that keeps
the reader's credential current in its state directory -- the key, the
certificate, and the CA bundle (`host.key`, `host.crt`, `ca.crt`) -- and holds
nothing else: no person's session, no signing key, no KEK. It joins and
waits for adoption as above, heartbeats over mTLS, and renews at two thirds
of the certificate's life or when the CP asks. The software beside it reads
with those files: it calls the
tenant API's gRPC listener with the certificate, and takes the tokens the
answers carry to the nodes and the relay itself.

- **Files.** A renewal keeps the key and replaces `ca.crt`, then
  `host.crt`, each whole by a rename, so they are never seen half-written;
  the CP refuses the old serial from then on, on connections already open
  too. The software loads the pair for each new connection, or when
  `host.crt` changes, and loads it again once a call is refused
  `UNAUTHENTICATED`.
- **Scope.** A reader reads its tenant within the sites it was adopted for
  (`SiteMember` rows), or all of it when it was adopted with none
  (`all_sites`). A set in no site, with its sources and laminae, is read
  only by a reader of the whole tenant, as by a person who sees every site.
  It calls the reads [§36.4](13-configuration.md#364-running-a-reader)
  lists and its own heartbeat and renewal; anything else is refused.
- **Expiry.** A reader that was off for longer than its certificate lives
  cannot renew it, and the CP's TLS refuses it. The agent then makes a new
  key and joins again without the expired certificate: "known host, new
  key", under its row, until an `adopt` takes it back with its sites. An
  erased reader is refused at once, and its agent joins again as a new
  reader.
- **Not built:** the tenant API on localhost without TLS, for a media server
  that manages no keys; `reader.addr` is refused.

Running one is [§36.4](13-configuration.md#364-running-a-reader).

### 33.5 TLS

- The CP runs a **built-in CA** by default. `shale init` creates it. No
  external PKI is needed on Kubernetes, plain Linux, or a single machine.
- **Host certificates** name their row: a URI SAN carrying the entity ID,
  whose domain byte says whether it is a node, a producer, or a reader
  ([§35.1](12-api.md#351-conventions)). A node certificate also carries every
  name and IP the active address resolver can hand out for that node, because
  producers and readers dial nodes directly and verify what they were given
  ([§34.10](11-deployment.md#3410-node-addresses)). When the `AddressPolicy`
  changes, a node or relay whose certificate lacks a name the active policy
  hands out for it is asked to renew in its heartbeat answer, and the
  renewal carries the policy's names. Activation does not wait for the
  renewals yet; issuing the certificates for the nodes' existing keys,
  installing them (`NodeControl.InstallCertificate`), and activating only
  after every live node has one is the design that would.
- Lifetimes: host certificates 90 days, renewed at 60; the CP certificate one
  year, renewed by the CP itself; the CA 10 years. The CP renews its own
  certificate in memory: at two thirds of its life, or when the CA that
  issues changes, each CP process issues itself one for the same key and
  names (those of `cp.crt`, and `control.names`) from the CA that issues
  now, and presents it from then on. A certificate from `control.cert_file`
  that no shale CA issued is presented as it is.
- **CA rollover is automatic.** One year before the CA expires
  (`control.ca_rollover_before`), the CP leader
  ([§34.9](11-deployment.md#349-events-and-directives)) begins a rollover;
  `shale ca rotate` begins the same one early. It is three steps, and the
  leader takes each once every live host has caught up with the one before
  and the step has stood for `control.ca_settle` (1 min), so that every CP
  process has read it:
  1. **Distributing.** The CP makes the next CA, and from then on every join
     and renewal hands out a bundle with both CAs. The old CA still issues.
     A host whose heartbeat reports another bundle is asked to renew, which
     hands it this one.
  2. **Replacing.** Once every live host reports that bundle, the new CA
     issues: host certificates, and the CP's own, which every CP process
     moves to. A host whose certificate the old CA issued is asked to renew.
  3. **Dropped.** Once every live host still reports the bundle and none
     holds a certificate from the old CA, the old CA is dropped: the bundle
     is the new CA alone, the CP trusts nothing the old one signed, and
     every host is asked to renew once more for the bundle without it.

  Every heartbeat carries the hash of the bundle the host holds (`ca_hash`,
  `sha256:` of the PEM), every kind of host's alike, and its row keeps it.
  A **live host** is an adopted, unerased host whose certificate has not
  expired: one that can still call the CP without being adopted again. A host
  that never comes back stops being waited for when its certificate expires
  -- at most 90 days after its last renewal, at once when it is erased -- and
  is adopted again when it does come back, as any host whose certificate
  expired ([§33.4](#334-joining-and-adoption)). `shale ca status` says where a
  rollover is, the bundle and its hash, and the live hosts it waits for, with
  the date each stops being live.

  The new CA is trusted everywhere before anything is issued from it, and the
  old one is dropped only when nothing live holds what it issued, so **no
  cross-signed certificate** bridges the two. None could: every CA `shale
  init` made has a path length of zero, so a chain from a leaf of the new CA
  through the new CA signed by the old one is refused by every verifier that
  follows RFC 5280, Go's included; a client holding only the old root would
  gain nothing from it. What holds only the old root is out of the
  rollover's reach, and takes the bundle `shale ca status` prints as soon
  as the rollover begins, since the CP's own certificate moves to the new
  CA at the second step, minutes later when every host is up: the CLI's
  `client.ca_file` (or the state directory's `ca.crt` it falls back to), a
  browser that was told to trust the shale CA, and a new host's `ca_hash`,
  which names the CA the CP's certificate comes from. A host that slept through a whole rollover has a
  pin the CP no longer satisfies: it is adopted again after its `ca.crt` is
  removed, so that it pins again on first contact, or checks a `ca_hash`
  naming the new CA.

  Until the first rollover the CA is the one `shale init` wrote into the
  state directory (`ca.crt`, `ca.key`), and nothing is asked of hosts about
  their bundles. A rollover records that CA in the database as a
  `CertificateAuthority` row beside the next one, its key wrapped under a
  key derived from the KEK for CA keys alone
  ([§33.3](#333-signing-keys-and-rotation)), and from then on the rows are
  the CA set every CP process reads; the files are the
  first CA and nothing more. A node or a producer of an earlier release
  takes the new bundle on renewal, but its listeners and its upload client
  read it only when they start: update hosts before `shale ca rotate`, or
  restart them once they renewed.

  A **compromised** CA cannot be rolled over this way, since nothing it signs
  can be trusted: the operator re-initializes the CA and every host is
  adopted again ([§33.7](#337-what-a-compromise-costs)).
- **External certificates** (cert-manager, ACME, a corporate CA) can replace
  the built-in CA's output for the CP (`control.cert_file`,
  `control.key_file`) and for a node's data plane (`storage.cert_file`,
  `storage.key_file`): configure certificate and key paths, and they are
  loaded instead. Host adoption still issues the client certificates
  producers and readers use, and the node still verifies them against the
  shale CA when they are presented.
- A node serves its external certificate to a client that asks for a name
  the certificate carries: a browser reading laminae for Playback and
  Export ([§40.5](17-console.md#405-playback)), and producers and readers,
  once the node is advertised by that name
  ([§34.10](11-deployment.md#3410-node-addresses)). A client that dials an
  IP asks for no name and gets the host certificate. HTTP/3 on the data
  port takes the same configuration at each handshake: it chooses alike,
  verifies a client certificate against the bundle held then, as TCP
  does, and a producer's HTTP/3 verifies a node against its bundle held
  then ([§35.6](12-api.md#356-storage-node-http-data-plane)). The control API keeps
  the host certificate: the CP dials it on an IP and verifies it against
  the shale CA by the node's identity ([§35.7](12-api.md#357-storage-node-control-api)).
- The relay's **WHEP listener** takes one too (`relay.whep_cert_file`,
  `relay.whep_key_file`), since browsers trust only the public CAs
  ([§39.4](16-relay.md#394-viewers)). Ingest keeps the host certificate.
- Either kind of file is read again when it changes, as a mounted Secret
  does when cert-manager renews it, and a pair that does not load leaves
  the last good one serving.
- **Hosts trust the public CAs as well as the shale CA** for the servers of
  other hosts: a producer or a reader verifies a node's data plane, and a
  producer a relay, against the pinned bundle and the system's roots
  (`/etc/ssl/certs/ca-certificates.crt` in the image and on Debian or
  Raspberry Pi OS; the bundle alone where there are none), and the CLI
  does the same toward a relay. That is a wider trust, and the same one a
  browser places: whoever can have a public CA issue a certificate for the
  name a node or relay is advertised by, in practice whoever controls that
  name's DNS, can answer as that host to producers and readers too, and
  receives what is uploaded to it and the access tokens sent with it.
  Advertise by names in a zone you control. With the shale CA alone such a
  host would fail the handshake; the token does not stop it, since it
  names the node and binds no key. The **CP connection is not widened**: a
  host pins the CP's CA on first contact (or checks `ca_hash`) and trusts
  that bundle alone from then on ([§33.4](#334-joining-and-adoption)), and
  the CP pins a node's control API to the shale CA and the node's
  identity. A reader is the one host an operator may widen it for, by
  naming the CA a CP's `control.cert_file` comes from (`reader.cp_ca`, the
  system's roots or a PEM file): the CP is then verified by its name like
  any server, nothing is pinned, and the bundle the join hands over is
  trusted beside it. A media server beside a reader that fetches laminae
  itself needs the system's roots beside the bundle the agent writes for
  it.
- TLS is on everywhere, the data-center LAN included. With AES-NI it costs
  about one core per several GB/s, which is small next to HDD-bound ingest.
- **Plaintext only in development mode** (`shale serve all --dev`). The
  reader agent's localhost listener would be the other place, and it is not
  built ([§33.4](#334-joining-and-adoption)).

### 33.6 Channels

| From → To | Protocol | Authentication |
|---|---|---|
| Producer / Reader → tenant API | gRPC over mTLS | host certificate |
| Person / CLI → tenant or cluster API | gRPC over TLS | a session; on the tenant API also a person's key for Shale (`Bearer rt_…`, [§33.1](#331-trust-model)) |
| Producer / Reader → Storage Node | HTTP/1.1, HTTP/2, or HTTP/3 over TLS | CP-signed access token |
| Producer → Satellite | HTTP/1.1 or HTTP/2 over mTLS ([§35.9](12-api.md#359-satellite-upload-proxy)) | producer certificate, and the CP-signed put token issued to it |
| Satellite → Storage Node | HTTP/1.1 or HTTP/2 over TLS | the producer's CP-signed put token, forwarded |
| Satellite → cluster API | gRPC over mTLS | satellite certificate |
| Storage Node → cluster API | gRPC over mTLS | node certificate |
| Control Plane → Storage Node | gRPC over mTLS (control API, [§35.7](12-api.md#357-storage-node-control-api)) | CP certificate |
| Producer → Relay | gRPC over TLS (ingest, [§35.8](12-api.md#358-relay-ingest-and-whep)) | CP-signed publish token |
| Viewer → Relay | HTTPS (WHEP) and WebRTC (DTLS-SRTP) | CP-signed view token |
| Relay → cluster API | gRPC over mTLS | relay certificate |
| Node → Node, Relay → Relay, Relay → Node | none | — |

### 33.7 What a compromise costs

| Leaked | Attacker can | Response |
|---|---|---|
| One access token | one operation on one lamina until it expires; a view token, one camera for an hour, and the relay ends the session when the token does; one issued on a person's behalf, for `auth.delegation.token_ttl` (5 min) | none needed |
| An app's delegation handle | nothing alone: every use needs a fresh exchange token from roster for that app ([§33.8](#338-viewing-on-a-persons-behalf)) | the app revokes it, or the person is signed out everywhere at roster |
| An app's key at roster | mint exchange tokens and, with a handle the app holds, view what the app's delegations name, within what roster grants the app; a new delegation also needs a person's fresh access token for Shale, and that person's own grant unless `auth.delegation.persons: tenant` | end the app's nomination at roster, or unbind its role; effective at the next renewal |
| A publish token | feed false video for that producer's cameras to viewers, for up to a day: the relay ends a stream when its token expires, and learns nothing else about a producer | erase the producer; the token expires within `publish_token_ttl`, or rotate the signing key at once ([§33.3](#333-signing-keys-and-rotation)) |
| A relay's key | serve any stream it carries to anyone, and feed viewers anything; it holds no token for any Storage Node, so recordings are out of reach | erase the relay and adopt the machine again; its producers are reassigned |
| A satellite's key | take its producers' uploads and read or alter them on their way, and write with the put tokens they carry, which name keys the CP allocated: what a node holding those uploads could do; nothing of anyone's reads | erase the satellite and adopt the machine again; its producers are assigned another, or upload directly ([§41.7](18-satellite.md#417-security)) |
| A producer's key | negotiate and allocate for that producer's set, and write laminae into it up to the set's ceilings ([§12.6](04-write-path.md#126-upload-profile-negotiation)) | erase the producer; mTLS refuses it at once, tokens in flight expire within `allocation_ttl` |
| A reader's key | read the laminae of its sites | erase the reader; effective at once for new tokens, within `read_token_ttl` for issued ones |
| A person's session | anything that person may do, inside **their tenant only**; the wall holds; for up to 24 h, unless its person stops being in good standing at roster | sign the person out everywhere at roster, or suspend them: every session minted before it is refused within `auth.operators.ttl` (30 s) ([§33.1](#331-trust-model)); reset the password |
| A person's key for Shale | what that person may do, of the methods the key names, inside **their tenant only**, on the tenant API alone, until the key ends; a key that carries a list of cameras, the reads and views of the cameras it names, of those its person sees, and nothing else | its person ends it on the console's *Tokens* page, or it is revoked at roster (the account page, or Shale's administrators there), or the person is suspended: refused within `auth.operators.ttl` (30 s), which *end* says, at once where Shale follows roster's sync stream; streams opened with it end with it ([§33.1](#331-trust-model), [§32](09-operations.md#32-cli--processes)) |
| A node's key | act as that node: serve or drop its own sinks' data, report its own devices and sinks. A report about a sink or a device another live node holds changes nothing, its SMART and pressure included, and a sink's capacity is stored clamped to `max_sink_capacity` ([§27](09-operations.md#27-node--device--sink-health-and-quarantine)) | erase the node and adopt the machine again under a new key |
| A hardware identity | request adoption as a known host | nothing until an operator adopts it; with `readopt: auto`, impersonate that host, which is why the default is `manual` |
| A cluster operator's session | manage the cluster and read across tenants, **from wherever the cluster API's sign-in listener answers**: the API binds to localhost unless `cluster.addr` names an interface, until the cluster tenant (or, with `auth.operators`, the operators' tenant) is known it serves no person at all, and a deployment that exposes the sign-in listener for the console ([§40.4](17-console.md#404-serving-it)) exposes exactly that — a person with a password, behind the gate | sign the operator out everywhere at roster, or suspend them: refused within `auth.operators.ttl`; take their grant of all of Shale away, which ends it on the cluster API within the same; keep the listener where the operators are |
| The CP signing key | read and write any lamina on any node | rotate the key at once ([§33.3](#333-signing-keys-and-rotation)); hosts drop the retired key within their next poll, 30 s |
| The CA key | impersonate nodes or the CP to clients | re-initialize the CA and adopt every host again; protect it accordingly |

### 33.8 Viewing on a person's behalf

An app beside Shale -- a viewing portal with its own pages and its own
idea of who may see which cameras -- signs its people in at the same
roster and shows them Shale's video, live and recorded, with the bytes
going from the browser to the relay and the nodes as they always do. What
it needs from Shale is credentials for those bytes on behalf of the person
in front of it, narrowed to what the portal says and never wider than what
roster grants the portal or the person. This is roster's contract for *an
app acting as a person at another app* (roster's `docs/apps.md`, *As a
person*), and Shale is the receiving app.

```text
portal ─ authorize, audience = Shale's API ─▶ issuer          (the person's browser)
       ◀─ JWT {sub: person, aud: Shale's API, client_id: portal}   minutes old
portal ─ DelegationService/Exchange {app: @<tenant>/shale} ─▶ roster
       ◀─ rd_…   (about the portal, issued to Shale, 15 min)
portal ─ DelegationService.Start {access_token, live, recordings, sets,
         sources, recordings_from/to}   authorization: Bearer rd_… ─▶ Shale
       ◀─ handle, until date_ends
portal ─ DelegationService.Live / Timeline {handle, …}
         authorization: Bearer rd_… (fresh) ─▶ Shale
       ◀─ view tokens and WHEP URLs / laminae and signed URLs,
          for the person, naming the portal
browser ─ WHEP, Range ─▶ relay, nodes                          (unchanged)
portal ─ DelegationService.Revoke {handle} ─▶ Shale            (its own sign-out)
```

- **Two proofs, bound.** `Start` takes the person's access token -- a JWT
  the issuer signed, unexpired, whose audience is `auth.delegation.audience`
  -- and the portal's exchange token, which roster answers only to Shale's
  holder in the tenant it was issued in. The token's `client_id` must be a
  client `auth.delegation.clients` maps to an app, the exchange token must
  name that app's holder (by its alias, the app's name at roster), and the
  person must be of that tenant. Either proof alone is nothing; the person's
  token is accepted at `Start` and nowhere else, so it is not the person's
  whole credential at Shale in the portal's hands.
- **The handle** is Shale's session of "this app, acting for this person":
  a `Delegation` row, written as the person, and a secret only the portal's
  server is told. It ends at `date_ends` -- at most `auth.delegation.max_ttl`,
  12 h, and renewing does not move it -- when the portal revokes it, or when
  roster says the person was signed out everywhere, suspended or erased.
- **Every use asks again.** `Live` and `Timeline` take the handle and a
  fresh exchange token for the same app, ask roster again what it grants the
  app's holder and the person (kept `auth.operators.ttl`, 30 s), and serve
  the person as the gate would -- their tenant, their sites -- narrowed to
  the handle. Asking `Live` again before its tokens lapse is the renewal, and
  an open WHEP session takes the fresh token in place
  ([§39.4](16-relay.md#394-viewers)).
- **What the portal may do** is what a tenant binds to the app's holder at
  roster: a **role of Shale's** naming `/shale.DelegationService/Live`,
  `/shale.DelegationService/Timeline`, or both, bound to the portal's
  holder -- Shale asks `HolderService/Reaches` about that holder, which is
  answered of Shale's roles. A role of roster's names neither, and one of
  the portal's own is the portal's. Live and recordings are granted apart.
  Nothing the portal says widens it.
- **What the person may do** is what they may read themselves: every
  person of the tenant, by default. With `auth.operators.reads: granted`,
  reads are as far as roster grants them like any other call, and a role
  naming the delegated methods (`/shale.DelegationService/*`) is as good as
  one naming the reads -- which is how somebody views through a portal and
  reads nothing here with a session of their own, since `DelegationService`
  serves no session and no certificate: on the tenant API its four entries
  take an app's exchange token alone and its generated verbs are refused to
  everybody, and the cluster API does not serve it at all.
  With `auth.delegation.persons: tenant`, any person of the tenant in good
  standing may be viewed for, whatever `auth.operators.reads` says, and
  nobody needs a grant to be: the app is the authority on which of its
  people sees which cameras, within its ceiling. What a person gets is
  still narrowed by everything else here -- what roster grants the app,
  the handle's sets, sources and window, the person's tenant and sites,
  and their standing at every use -- and only to the reads: a person is
  never let do anything but view through it. The console is unchanged: with
  `reads: granted` the same person still reads nothing with a session of
  their own. Set it where an app keeps its own list of who may watch what
  (anonymous, every signed-in person, some groups), and leave it out where
  roster's grants should be the list.
- **What the handle names**, at `Start`: live, recordings, or both; sets
  and sources, none meaning all the person sees; and a window of recordings.
  A request naming a camera outside it is refused at the CP. The window is
  kept to the lamina: a lamina crossing its edge comes whole, since a lamina
  is what a read token reads.
- **The tokens** are the person's -- `actor`, `actor_tenant` -- and name
  the portal in `delegator`; they live at most `auth.delegation.token_ttl`,
  5 min, so a live view or a signed URL outlives the delegation by no more
  than that. They grant nothing more than the person's would.
- **What is kept.** The trail has each `Delegation` row as the person
  wrote it: started, last used (at most once a minute) and ended. The CP
  logs every hand-out (`delegation: issued`, with the person, the app and
  the method) and the relay logs `delegator` beside `actor` when a session
  opens.

**How soon an ending takes effect.**

| What happens | Stops new tokens | Stops what was issued |
|---|---|---|
| The portal revokes the handle | at once | within `token_ttl` |
| The app's role unbound, its nomination ended, or the person's grant removed at roster (which, with `auth.delegation.persons: tenant`, is not what lets them be viewed for) | at the next renewal, within `auth.operators.ttl` | within `token_ttl` |
| The person signed out everywhere, suspended or erased | as fast as roster's sync stream says it | within `token_ttl` |
| roster cannot be asked | at once: a renewal is refused | within `token_ttl` |

Roster's sync stream (`SyncService.Watch`) is what makes a sign-out reach a
delegation, since it changes nothing roster grants. Shale follows it on its
key, which needs `/roster.SyncService/Watch` allowed; where it cannot -- a
key not allowed it, or the roster in Shale's own process, which has no
broker -- it says so at start, and a sign-out ends a delegation only when
it runs its time. A suspension or an erasure still ends it at the next
renewal, which asks roster about the person.

**At the issuer**, the portal's client asks for Shale's API among its
audiences, gets access tokens that live minutes
(`authorization_code_grant_access_token_lifespan`), and no refresh token:
no `offline` scope, no `refresh_token` grant. A top-level redirect with
`prompt=none` when the portal's page loads is how it gets the minutes-old
token `Start` takes; after that it renews with the handle.

A separate permission to export cannot be enforced: a person who may read a
recording has its bytes, and saving them is the browser's.
