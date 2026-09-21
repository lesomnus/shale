/**
 * The worker the sandbox's Go program runs in.
 *
 * Two lines, and both have to be here rather than anywhere else: the two
 * imports must land in the **same realm**, and neither package can put the
 * other there. `sqlite3-wasm-go` installs the global the Go driver looks for
 * and `@lesomnus/grpc-dgram` runs the module that looks for it -- so importing
 * the driver on the main thread installs it where the program cannot see it.
 *
 * The symptom is the instance exiting before it publishes its entry point,
 * with the driver's own diagnostic in the worker's console:
 *
 *     sqlite3-wasm: globalThis["sqlite3-wasm-go"] is not installed;
 *     import the sqlite3-wasm-go package from the same worker that runs this program
 *
 * @module
 */

import 'sqlite3-wasm-go'
import '@lesomnus/grpc-dgram/wasm/worker'
