// test/server.mjs: the console as a deployment serves it (§40.4), in headless
// Chromium: signs in to both surfaces with real passwords, draws every page
// and takes a screenshot of each under test/shots-server/. The server's
// certificate is the deployment's own CA, so TLS errors are ignored.
//
//   BASE=https://10.1.2.74:30402 TENANT_PW=... CLUSTER_PW=... node test/server.mjs
//
// TENANT (acme), ADMIN (admin), OPS (ops), CLUSTER (cluster) and OUT can be
// set too. With ADOPT=1 every host waiting for adoption is adopted from the
// page, a producer for the set named SET (the first set otherwise): §40's
// acceptance, an operator adopting a host and watching a camera without the
// CLI.
import { chromium } from 'playwright'
import { mkdirSync } from 'node:fs'

const OUT = process.env.OUT ?? 'test/shots-server'
const BASE = process.env.BASE ?? 'https://127.0.0.1:7402'
const TENANT = process.env.TENANT ?? 'acme'
const ADMIN = process.env.ADMIN ?? 'admin'
const CLUSTER = process.env.CLUSTER ?? 'cluster'
const OPS = process.env.OPS ?? 'ops'
const TENANT_PW = process.env.TENANT_PW ?? ''
const CLUSTER_PW = process.env.CLUSTER_PW ?? ''
mkdirSync(OUT, { recursive: true })

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, ignoreHTTPSErrors: true })
const logs = []
page.on('console', (m) => {
	const t = `[${m.type()}] ${m.text()}`
	logs.push(t)
	if (m.type() === 'error' || m.type() === 'warning') console.log(t.slice(0, 300))
})
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)))
page.on('response', (r) => {
	if (r.status() >= 400) console.log(`[http ${r.status()}] ${r.url().slice(0, 120)}`)
})

async function signIn(tenant, alias, password) {
	await page.fill('form.sign-in input[autocomplete=organization]', tenant)
	await page.fill('form.sign-in input[autocomplete=username]', alias)
	await page.fill('form.sign-in input[type=password]', password)
	await page.click('form.sign-in button[type=submit]')
}

const t0 = Date.now()
const r = await page.goto(BASE + '/#/cameras')
console.log(`GET / -> ${r?.status()} ${r?.headers()['content-type'] ?? ''}`)
await page.waitForSelector('form.sign-in', { timeout: 30_000 })
console.log(`page up after ${((Date.now() - t0) / 1000).toFixed(1)} s`)

// Cameras, as the tenant's admin.
await signIn(TENANT, ADMIN, TENANT_PW)
await page.waitForSelector('.cards .card, .empty', { timeout: 60_000 })
await page.waitForTimeout(6000)
await page.screenshot({ path: `${OUT}/cameras.png`, fullPage: true })
console.log('cameras:', await page.locator('.cards .card').count(), 'cards')

// Hosts: the tenant half draws at once; the cluster half asks to sign in.
await page.goto(BASE + '/#/hosts')
await page.waitForSelector('form.sign-in', { timeout: 30_000 })
await signIn(CLUSTER, OPS, CLUSTER_PW)
await page.waitForSelector('table, .empty', { timeout: 30_000 })
await page.waitForTimeout(3000)
await page.screenshot({ path: `${OUT}/hosts.png`, fullPage: true })
console.log('hosts: pending adopt buttons:', await page.locator('button', { hasText: 'adopt' }).count())

if (process.env.ADOPT === '1') {
	const rows = page.locator('tr', { has: page.locator('button', { hasText: 'adopt' }) })
	const n = await rows.count()
	for (let i = 0; i < n; i++) {
		const row = rows.nth(i)
		const select = row.locator('select')
		if (await select.count()) {
			await select.selectOption(process.env.SET !== undefined ? { label: process.env.SET } : { index: 0 })
		}
		console.log('adopting:', (await row.innerText()).replace(/\s+/g, ' ').slice(0, 100))
		await row.locator('button', { hasText: 'adopt' }).click()
		await page.waitForTimeout(1500)
	}
	if (n > 0) {
		await page.waitForTimeout(8000)
		await page.screenshot({ path: `${OUT}/hosts-adopted.png`, fullPage: true })
		console.log('after adopting: pending adopt buttons:', await page.locator('button', { hasText: 'adopt' }).count())
	}
}

// Both signed in: the sessions survive a reload.
await page.reload()
await page.waitForSelector('table, .empty', { timeout: 30_000 })
console.log('after reload: sign-in forms shown:', await page.locator('form.sign-in').count())

await page.goto(BASE + '/#/devices')
await page.waitForSelector('table, .card, .empty', { timeout: 30_000 })
await page.waitForTimeout(2000)
await page.screenshot({ path: `${OUT}/devices.png`, fullPage: true })

await page.goto(BASE + '/#/segments')
await page.click('a[href*="#/segments/"]')
await page.waitForSelector('table, .empty', { timeout: 30_000 })
await page.waitForTimeout(4000)
await page.screenshot({ path: `${OUT}/segments.png`, fullPage: true })
console.log('segments rows:', await page.locator('tbody tr').count())

await page.goto(BASE + '/#/live')
await page.click('a[href*="#/live/"]')
await page.waitForSelector('.player', { timeout: 30_000 })
await page.waitForTimeout(12000)
await page.screenshot({ path: `${OUT}/live.png`, fullPage: true })
const videos = await page.evaluate(() =>
	Array.from(document.querySelectorAll('video')).map((v) => ({ w: v.videoWidth, h: v.videoHeight, t: Math.round(v.currentTime * 10) / 10, ready: v.readyState })),
)
console.log('live videos:', JSON.stringify(videos))

console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
