// test/server.mjs: the console as a deployment serves it (§40.4), in headless
// Chromium: signs in to both surfaces with real passwords, draws every page
// and takes a screenshot of each under test/shots-server/. The server's
// certificate is the deployment's own CA, so TLS errors are ignored.
//
//   BASE=https://10.1.2.74:30402 TENANT_PW=... CLUSTER_PW=... node test/server.mjs
//
// TENANT (acme), ADMIN (admin), OPS (ops), CLUSTER (cluster) and OUT can be
// set too. With ADOPT=1 every host waiting for adoption is adopted from the
// page, a producer for the set SET names -- one alias for all of them, or
// `hostname=alias,...` -- and the first set otherwise: §40's acceptance, an
// operator adopting a host and watching a camera without the CLI. LIVE names
// the set the segments and live pages open (the first set otherwise).
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
// Every peer connection the page opens, so the live wall can be asked
// whether video bytes arrived and not only whether a player was drawn.
await page.addInitScript(() => {
	window.__pcs = []
	const Real = window.RTCPeerConnection
	window.RTCPeerConnection = function (...args) {
		const pc = new Real(...args)
		window.__pcs.push(pc)

		return pc
	}
	window.RTCPeerConnection.prototype = Real.prototype
})
const logs = []
page.on('console', (m) => {
	const t = `[${m.type()}] ${m.text()}`
	logs.push(t)
	if (m.type() === 'error' || m.type() === 'warning') console.log(t.slice(0, 300))
})
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)))
page.on('response', (r) => {
	if (r.status() >= 400 || r.url().includes('/whep')) console.log(`[http ${r.status()}] ${r.request().method()} ${r.url().slice(0, 120)}`)
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

/** setFor is the set a pending producer's row is adopted for, from SET. */
function setFor(row) {
	const spec = process.env.SET
	if (spec === undefined) return { index: 0 }
	if (!spec.includes('=')) return { label: spec }
	for (const pair of spec.split(',')) {
		const [host, alias] = pair.split('=')
		if (host !== undefined && alias !== undefined && row.includes(host)) return { label: alias }
	}

	return { index: 0 }
}

if (process.env.ADOPT === '1') {
	// An adopted row leaves the pending table, so the first row is asked
	// for again each time rather than walked by index.
	const rows = page.locator('tr', { has: page.locator('button', { hasText: 'adopt' }) })
	let adopted = 0
	for (let i = 0; i < 8 && (await rows.count()) > 0; i++) {
		const row = rows.first()
		const text = (await row.innerText()).replace(/\s+/g, ' ')
		const select = row.locator('select')
		if (await select.count()) {
			await select.selectOption(setFor(text))
		}
		console.log('adopting:', text.slice(0, 100))
		await row.locator('button', { hasText: 'adopt' }).click()
		adopted++
		await page.waitForTimeout(3000)
	}
	if (adopted > 0) {
		await page.waitForTimeout(8000)
		await page.screenshot({ path: `${OUT}/hosts-adopted.png`, fullPage: true })
		console.log('adopted', adopted, '- pending adopt buttons now:', await page.locator('button', { hasText: 'adopt' }).count())
		await page.goto(BASE + '/#/cameras')
		await page.waitForSelector('.cards .card', { timeout: 60_000 })
		await page.waitForTimeout(20_000)
		await page.screenshot({ path: `${OUT}/cameras-adopted.png`, fullPage: true })
		console.log('cameras after adopting:', await page.locator('.cards .card').count(), 'cards')
	}
}

// Both signed in: the sessions survive a reload.
await page.goto(BASE + '/#/hosts')
await page.reload()
await page.waitForSelector('table, .empty', { timeout: 30_000 })
console.log('after reload: sign-in forms shown:', await page.locator('form.sign-in').count())

await page.goto(BASE + '/#/devices')
await page.waitForSelector('table, .card, .empty', { timeout: 30_000 })
await page.waitForTimeout(2000)
await page.screenshot({ path: `${OUT}/devices.png`, fullPage: true })

/** pick opens one set's page from the list: LIVE by alias, else the first. */
async function pick(route) {
	const links = page.locator(`a[href*="#/${route}/"]`)
	await links.first().waitFor({ timeout: 30_000 })
	const named = process.env.LIVE !== undefined ? links.filter({ hasText: process.env.LIVE }) : links
	await ((await named.count()) > 0 ? named : links).first().click()
}

await page.goto(BASE + '/#/segments')
await pick('segments')
await page.waitForSelector('table, .empty', { timeout: 30_000 })
await page.waitForTimeout(4000)
await page.screenshot({ path: `${OUT}/segments.png`, fullPage: true })
console.log('segments rows:', await page.locator('tbody tr').count())

await page.goto(BASE + '/#/live')
await pick('live')
await page.waitForSelector('.player', { timeout: 30_000 })
await page.waitForTimeout(12000)
await page.screenshot({ path: `${OUT}/live.png`, fullPage: true })
const videos = await page.evaluate(() =>
	Array.from(document.querySelectorAll('video')).map((v) => ({ w: v.videoWidth, h: v.videoHeight, t: Math.round(v.currentTime * 10) / 10, ready: v.readyState })),
)
console.log('live videos:', JSON.stringify(videos))
const pcs = await page.evaluate(async () => {
	const out = []
	for (const pc of window.__pcs ?? []) {
		const st = { ice: pc.iceConnectionState, conn: pc.connectionState, video: 0, packets: 0, candidate: '' }
		for (const r of (await pc.getStats()).values()) {
			if (r.type === 'inbound-rtp' && r.kind === 'video') {
				st.video += r.bytesReceived ?? 0
				st.packets += r.packetsReceived ?? 0
			}
			if (r.type === 'candidate-pair' && r.state === 'succeeded') st.candidate = r.id
		}
		out.push(st)
	}

	return out
})
console.log('peer connections:', JSON.stringify(pcs))

console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
