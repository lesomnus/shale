import { readdir, readFile, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { brotliCompress, constants } from 'node:zlib'
import { promisify } from 'node:util'

import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'

const compress = promisify(brotliCompress)

/**
 * brotli writes a `.br` beside every `.wasm` in the build.
 *
 * The sandbox module is tens of megabytes and compresses about sixfold, which
 * is the difference between a demo link somebody opens and one they close. It
 * runs on `vite build` only: `npm run dev` reads the file off disk, so there is
 * nothing to save and a compression pass would be time spent on every restart.
 *
 * # It is not enough on its own
 *
 * Something has to serve it. A static host that does content negotiation --
 * nginx with `brotli_static`, Netlify, Cloudflare Pages, most CDNs -- picks the
 * `.br` up for a request that says `Accept-Encoding: br`. `vite preview` does
 * not, so the file is there and unused when previewing locally, which is
 * correct and worth knowing before wondering why nothing changed.
 *
 * # Why quality 9
 *
 * Measured on payday's own sandbox -- the whole app as `GOOS=js GOARCH=wasm go
 * build ./wasm` leaves it, 67.8 MB, through this plugin's own brotli:
 *
 *	q5   11.1 MB    1.2s
 *	q9   10.7 MB    4.7s
 *	q11   9.2 MB  131.5s
 *
 * The last one buys 1.5 MB for two minutes. That is a fine trade for an
 * artifact built once and downloaded many times, and a bad one for anything
 * built in a loop -- so it is not the default, and this comment is here so the
 * decision can be made with the numbers rather than by trying it.
 */
function brotli(): Plugin {
	let dir = 'dist'

	return {
		name: 'sandbox-brotli',
		apply: 'build',

		configResolved(c) {
			dir = c.build.outDir
		},

		// `closeBundle` rather than `writeBundle`, because what is being
		// compressed is in `public/` -- vite copies those after the bundle is
		// written, so the earlier hook would find nothing.
		async closeBundle() {
			for (const at of await walk(dir)) {
				if (!at.endsWith('.wasm')) continue

				const b = await readFile(at)
				const v = await compress(b, {
					params: {
						[constants.BROTLI_PARAM_QUALITY]: 9,
						[constants.BROTLI_PARAM_SIZE_HINT]: b.length,
					},
				})

				await writeFile(at + '.br', v)

				const mb = (n: number) => (n / 1048576).toFixed(1)
				console.log(`  ${at}.br  ${mb(b.length)} MB -> ${mb(v.length)} MB`)
			}
		},
	}
}

async function walk(at: string): Promise<string[]> {
	const vs: string[] = []
	for (const name of await readdir(at)) {
		const p = join(at, name)
		if ((await stat(p)).isDirectory()) {
			vs.push(...(await walk(p)))
		} else {
			vs.push(p)
		}
	}

	return vs
}

/**
 * `npm run dev` serves this on 5173 and the app answers on 8080, which is two
 * origins -- so the server has to say the page may call it. That is the
 * `origins:` list under `server.http` in the app's configuration, and it is a
 * list rather than a wildcard on purpose.
 *
 * A deployment that serves `dist/` from the app itself is one origin and needs
 * none of it.
 *
 * The rest of this file is what it takes to serve the sandbox, which is two
 * things and neither is payday's to fix. Both fail confusingly, which is why
 * they are written out rather than left to a README -- and `pd doctor` checks
 * that they are still here.
 */
export default defineConfig({
	plugins: [react(), brotli()],

	// The worker `@lesomnus/grpc-dgram` starts is
	// `new URL("./wasm/worker.mjs", import.meta.url)`, and dependency
	// pre-bundling rewrites the module into `.vite/deps/` -- where that
	// relative URL resolves to nothing. The failure is "the worker itself
	// failed", which does not mention bundling.
	optimizeDeps: { exclude: ['@lesomnus/grpc-dgram'] },

	server: {
		// SQLite in a Worker cancels work with a `SharedArrayBuffer`, which
		// does not exist without cross-origin isolation. The symptom is "it
		// works on the other dev server".
		headers: {
			'Cross-Origin-Opener-Policy': 'same-origin',
			'Cross-Origin-Embedder-Policy': 'require-corp',
		},
	},
})
