# The console

The page an operator works from (§40, `docs/17-console.md`): hosts waiting to
be adopted, the quarantine queue, cameras and their state, segments arriving,
live view.

```sh
npm install
npm run gen               # ts/gen from ../proto, with protoc-gen-es from node_modules
npm run check             # tsc
npm run dev               # http://localhost:5173, against a server named by
                          #   VITE_TENANT_ADDR / VITE_CLUSTER_ADDR (http://localhost:7402 / :7403)
npm run build             # ../web/console/dist, which the control plane embeds and
                          #   serves at the root of its tenant HTTP listener (§40.4)

npm run sandbox:build     # the control plane compiled into the page (public/app.wasm)
npm run dev               # then http://localhost:5173/?sandbox
npm run test:sandbox      # headless Chromium through every page (needs `npx playwright install chromium`)
npm run build:sandbox     # dist/, the page with the sandbox in it, for a static host

BASE=https://cp:7402 TENANT_PW=… CLUSTER_PW=… node test/server.mjs
                          # the same walk against a deployment (§40.4); ADOPT=1 adopts
                          #   what waits, a producer for SET
```

`npm run build` is what the Dockerfile and `deploy/deb/build.sh` run before
`go build`; a binary built without it serves a line saying so where the
console would be.

`vendor/` holds payday's TypeScript package built from the commit `go.mod`
pins, until that version is on npm; `package.json` points at it.
