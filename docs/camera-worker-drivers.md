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
