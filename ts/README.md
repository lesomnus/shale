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
npm run build             # dist/, a static page

npm run sandbox:build     # the control plane compiled into the page (public/app.wasm)
npm run dev               # then http://localhost:5173/?sandbox
npm run test:sandbox      # headless Chromium through every page (needs `npx playwright install chromium`)
```

`vendor/` holds payday's TypeScript package built from the commit `go.mod`
pins, until that version is on npm; `package.json` points at it.
