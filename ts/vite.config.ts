import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

/**
 * `npm run dev` serves this on 5173 and the app answers on 8080, which is two
 * origins -- so the server has to say the page may call it. That is the
 * `origins:` list under `server.http` in the app's configuration, and it is a
 * list rather than a wildcard on purpose.
 *
 * A deployment that serves `dist/` from the app itself is one origin and needs
 * none of it.
 */
export default defineConfig({
	plugins: [react()],
})
