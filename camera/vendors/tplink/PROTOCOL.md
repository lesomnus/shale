# TP-Link compatibility boundary

The default extension targets **EasyCam C420, hardware 1.0, firmware 1.2.2
Build260624** (reported by ONVIF as `1.2.2 Build 260624 Rel.39352n`). Both
spellings of this build are recognized. A manufacturer name is not permission to obtain credentials or to
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
- The UI separately logs in and calls `system.boot_set_date`, sets an alias,
  time settings and optional services. This adapter invokes **none** of those
  operations. It does not use the web login helper.
- A response timeout after the password write is ambiguous. The caller retains
  the prepared immutable credential reference, rechecks activation, and verifies
  access using authenticated ONVIF inspection. Replaying activation never
  replaces an initialized administrator password.

The simulator exercises this wire protocol, including RSA decryption, response
loss and replay. No factory reset or fresh activation was performed on the
installed production device. Another firmware needs its own verified
compatibility entry; a simulator does not establish support for unknown devices.

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

The firmware web RPC is distinct from the separately enabled OpenAPI service on
port 20443. This implementation does not enable that service.
