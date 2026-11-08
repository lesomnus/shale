# TP-Link compatibility boundary

The default extension targets **EasyCam C420, hardware 1.0, firmware 1.2.2
Build260624** (reported by ONVIF as `1.2.2 Build 260624 Rel.39352n`). Both
spellings of this build are recognized. It supports first-password activation
and explicit 50/60 Hz image-frequency initialization. A manufacturer name is not permission to obtain credentials or to
activate every member of that manufacturer's product family. For a fresh device
whose identity is not yet available, an enrollment administrator may explicitly
pin `ProfileC420V122` and the managed web endpoint. Unknown protocols return
`camera.ErrManualActivation`.

The protocol fixture is based on the device-distributed public UI assets
`index.1670d28b.js` (modules 3459, 4390 and 7572), inspected without logging in or
writing to the installed camera on 2026-11-06:

- An anonymous JSON POST at `/` sends `method: do` and
  `user_management: {get_encrypt_info: null}`. HTTP 401 is expected; `data.code`
  `-40405` (`ESYSRESET`) means the initial password is unset. Existing/locked
  account states never permit another first-password write.
- The initial operation is `method: do`, `set_password`, with administrator
  username, RSA PKCS#1 v1.5 encrypted password as URL-encoded base64 `ciphertext`,
  `pwd_sec_type: "1"`, language, and empty optional recovery fields. The RSA
  public key is supplied in the public firmware UI and is not a credential.
- The UI separately calls `system.boot_set_date`, sets an alias, time settings
  and optional services. This adapter invokes **none** of those operations. It
  does not use the web login helper that performs those writes.
- A response timeout after the password write is ambiguous. The caller retains
  the prepared immutable credential reference, rechecks activation, and verifies
  access using authenticated ONVIF inspection. Replaying activation never
  replaces an initialized administrator password.

The simulator exercises this wire protocol, including RSA decryption, response
loss and replay. First-password activation was also verified after an explicitly
authorized physical reset in an isolated lab on 2026-11-07. Factory reset remains
outside the adapter and is never an enrollment retry operation. Another firmware
needs its own verified compatibility entry; a simulator does not establish
support for unknown devices.

## Image-frequency initialization

The C420's factory 50 Hz image setting exposes at most 25 fps; 60 Hz exposes
30 fps. Frequency is an explicit preset setting, never inferred from a requested
frame rate. A zero value preserves the device and opens no web session.

`PlanInitialization` authenticates, reads `image.switch.flicker`, and returns a
before/after plan without changing camera configuration. `ApplyInitialization`
rechecks that value, skips an already-applied target, sets only this one field,
and verifies the read-back. The enrollment engine must inspect media capabilities
again before planning the desired frame rate.

The authenticated protocol was verified using the camera's management UI assets
and an explicit manual frequency restoration during that physical lab test:

- Read `get_encrypt_info` without invoking a clock-setting login helper. For
  this compatibility entry, the challenge selects encryption type `2` and
  password type `md5`. `key_2` (or `key`) contains URL-encoded base64 DER, decoded
  before parsing the RSA public key.
- Login encrypts `UPPERCASE_HEX(MD5("TPCQ75NF2Y:" + password)) + ":" + nonce`
  with that challenge key using RSA PKCS#1 v1.5. `keyType: "1"` is sent when
  using `key_2`. The resulting session token exists only for the bounded operation.
- Read frequency with `method: get, image: {name: "switch"}`. The only permitted
  setting write is `method: set, image: {switch: {flicker: "50hz" | "60hz"}}`.
- Logout uses `method: do, system: {logout: "null"}`. No clock, sensor frame-rate,
  network, service, account or factory-reset write is performed by initialization.

An authenticated supported model/hardware/firmware identity and a matching web
endpoint are required for frequency writes; an anonymous activation compatibility
pin alone does not authorize them. Unknown authentication exchanges, login
failures and ambiguous transport errors do not become setting fallbacks. After a
lost write response, re-inspection can observe the target without another write.

The new staged initializer is covered by a combined web/Media2 fixture: 50 Hz
rejects a 30 fps media plan, initialization changes to 60 Hz, and a fresh Media2
inspection permits that plan. Fixtures also cover read-back mismatch, lost
responses, login/protocol failures, preserving existing settings and idempotent
replay. They do not claim a new hardware run of this staged implementation.

Standard inspection/configuration is delegated to ONVIF. Media2 is preferred:
the observed C420 Media1 main profile claimed H.264 while Media2 and actual RTSP
reported HEVC. ONVIF observations remain advertised/read-back facts; independent
stream qualification must confirm the actual codec and decode validity before
enrollment is active. No optical, storage, PTZ or speaker capability is inferred
from a shared family API document.

Protocol references:

- [ONVIF Device WSDL](https://www.onvif.org/ver10/device/wsdl/devicemgmt.wsdl)
- [ONVIF Media2 WSDL](https://www.onvif.org/ver20/media/wsdl/media.wsdl)
- [ONVIF Media1 WSDL](https://www.onvif.org/ver10/media/wsdl/media.wsdl)
- [C420 support and model specifications](https://www.tp-link.com/kr/business-networking/vigi-network-camera/easycam-c420/)
- [C420 50/60 Hz frame-rate specification](https://static.tp-link.com/upload/product-overview/2025/202504/20250409/EasyCam%20C420%28UN%291.0%20Datasheet.pdf)

The firmware web RPC is distinct from the separately enabled OpenAPI service on
port 20443. This implementation does not enable that service.
