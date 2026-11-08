# Per-operation camera adapters

`cameraworker.LeasedDevices.Factory` constructs a `DeviceAdapters` bundle for one
operation from its fresh CP Work envelope. The constructor receives the current
network and the job's pinned inputs after `Execution.Step` checks its grant and
refreshes the selected device's transport. It performs no LAN I/O and grants no
authority. Verify also checks the current enrollment scope and media prefix
before calling the constructor.

Each operation owns its selected adapters. `Release`, when present, runs once
when the operation finishes, including driver errors, cancellation, missing
capabilities, or a factory error returned with partial adapters. Cancellation
during construction prevents the driver call and still releases the bundle.
Factory errors retain their classification through the Worker's private error
wrapper without exposing response text.

Factory and static adapter fields are mutually exclusive; a mixed configuration
returns `camera.ErrInvalid` before requesting a grant or invoking either driver.
A nil Factory preserves the existing static behavior. With Factory configured,
an absent selected Bootstrapper reports unknown/manual activation, an absent
Initializer reports unsupported, and absent Inspector/Configurator/Verifier
reports invalid configuration after fresh grant validation and construction.
These results never fall back to static adapters.

This seam does not select a vendor or enable a concrete driver by default. The
composition root supplies the constructor and its scoped transport settings.

`camera/drivers.New` supplies the standard ONVIF implementation and the installed
TP-Link compatibility adapter. It accepts the current network ID/CIDRs and an
optional pinned bootstrap selection; it imports no CP or Producer code. A nil
selection keeps standard ONVIF media operations and does not infer an activation
protocol. Unknown compatibility profiles return unsupported before LAN I/O.

Bootstrap settings contain an explicit web scheme and port. The web host is
derived only from the current authorized device endpoint. Vendor initialization
re-reads authenticated device information before selecting its firmware-specific
web protocol; historical checkpoint evidence cannot qualify an anonymous read.
Standard ONVIF media configuration uses its existing fresh capability and plan
precondition checks independently of that vendor pin.

The owned HTTP transport accepts only IP literals in the supplied prefixes,
refuses redirects and environment proxies, and retains certificate validation.
Optional camera trust roots do not carry a CP machine identity. Public hints
exclude credential queries, userinfo and vendor session paths; transient vendor
session paths remain private to the scoped HTTP exchange. Close the driver after
each operation, normally through the bundle's `Release` callback.
