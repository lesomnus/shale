// test/sandbox.mjs: the console against the sandbox in headless Chromium,
// every page drawn and a host adopted on the way, with a screenshot of each
// under test/shots/. Needs `npm run sandbox:build` and a dev server on 5173
// (`npm run dev`), or BASE pointing at one.
import { chromium } from 'playwright'
import { mkdirSync } from 'node:fs'

const OUT = process.env.OUT ?? 'test/shots'
const BASE = process.env.BASE ?? 'http://127.0.0.1:5173/?sandbox'
mkdirSync(OUT, { recursive: true })

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } })
const logs = []
page.on('console', (m) => {
	const t = `[${m.type()}] ${m.text()}`
	logs.push(t)
	if (m.type() === 'error' || m.type() === 'warning') console.log(t.slice(0, 300))
})
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)))

const t0 = Date.now()
await page.goto(BASE + '#/cameras')
await page.waitForSelector('form.sign-in', { timeout: 240_000 })
console.log(`sandbox up after ${((Date.now() - t0) / 1000).toFixed(1)} s`)

async function signIn(tenant, alias) {
	await page.fill('form.sign-in input[autocomplete=organization]', tenant)
	await page.fill('form.sign-in input[autocomplete=username]', alias)
	await page.click('form.sign-in button[type=submit]')
}

// Cameras, as the tenant's admin.
await signIn('acme', 'admin')
await page.waitForSelector('.cards .card', { timeout: 60_000 })
await page.waitForTimeout(6000)
await page.screenshot({ path: `${OUT}/cameras.png`, fullPage: true })

// Hosts: the tenant half draws at once; the cluster half asks to sign in.
await page.goto(BASE + '#/hosts')
await page.waitForSelector('form.sign-in', { timeout: 30_000 })
await signIn('cluster', 'ops')
await page.waitForSelector('table', { timeout: 30_000 })
await page.waitForTimeout(3000)
await page.screenshot({ path: `${OUT}/hosts.png`, fullPage: true })

// Adopt the pending node from the page.
const adopt = page.locator('tr', { hasText: 'jack' }).locator('button', { hasText: 'adopt' })
if (await adopt.count()) {
	await adopt.first().click()
	await page.waitForTimeout(8000)
	await page.screenshot({ path: `${OUT}/hosts-adopted.png`, fullPage: true })
}

await page.goto(BASE + '#/devices')
await page.waitForSelector('table, .card', { timeout: 30_000 })
await page.waitForTimeout(2000)
await page.screenshot({ path: `${OUT}/devices.png`, fullPage: true })

await page.goto(BASE + '#/segments')
await page.click('a[href*="#/segments/"]')
await page.waitForSelector('table', { timeout: 30_000 })
await page.waitForTimeout(4000)
await page.screenshot({ path: `${OUT}/segments.png`, fullPage: true })

await page.goto(BASE + '#/live')
await page.click('a[href*="#/live/"]')
await page.waitForSelector('.player', { timeout: 30_000 })
await page.waitForTimeout(4000)
await page.screenshot({ path: `${OUT}/live.png`, fullPage: true })

console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
