// test/playback.mjs: the Playback page against a running deployment (§40),
// in headless Chromium: signs in, opens the first set's first camera, picks
// a moment two minutes back on the axis and checks the video plays from
// there; asks for ten seconds ago and checks the relay answered; marks a
// range and downloads it as an MP4, which is left under OUT for ffprobe.
//
//   BASE=http://127.0.0.1:7402 TENANT_PW=... node test/playback.mjs
//
// TENANT, ADMIN, SET (the set's alias) and OUT can be set too.
import { chromium } from 'playwright'
import { mkdirSync } from 'node:fs'

const OUT = process.env.OUT ?? 'test/shots-playback'
const BASE = process.env.BASE ?? 'http://127.0.0.1:7402'
const TENANT = process.env.TENANT ?? 'acme'
const ADMIN = process.env.ADMIN ?? 'admin'
const TENANT_PW = process.env.TENANT_PW ?? ''
mkdirSync(OUT, { recursive: true })

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1280, height: 900 }, ignoreHTTPSErrors: true, acceptDownloads: true })
const logs = []
page.on('console', (m) => {
	logs.push(`[${m.type()}] ${m.text()}`)
	if (m.type() === 'error') console.log(`[console error] ${m.text().slice(0, 300)}`)
})
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)))
page.on('response', (r) => {
	if (r.status() >= 400) console.log(`[http ${r.status()}] ${r.request().method()} ${r.url().slice(0, 120)}`)
})

await page.goto(BASE + '/#/playback')
await page.waitForSelector('form.sign-in', { timeout: 30_000 })
await page.fill('form.sign-in input[autocomplete=organization]', TENANT)
await page.fill('form.sign-in input[autocomplete=username]', ADMIN)
await page.fill('form.sign-in input[type=password]', TENANT_PW)
await page.click('form.sign-in button[type=submit]')

const links = page.locator('a[href*="#/playback/"]')
await links.first().waitFor({ timeout: 30_000 })
const named = process.env.SET !== undefined ? links.filter({ hasText: process.env.SET }) : links
await ((await named.count()) > 0 ? named : links).first().click()
await page.waitForSelector('.deck', { timeout: 30_000 })
// Laminae on the axis.
await page.waitForSelector('.axis rect.lamina', { timeout: 120_000 })
await page.waitForTimeout(500)
await page.screenshot({ path: `${OUT}/playback-axis.png`, fullPage: true })

/** state reads the deck: the status badge, the playhead and the video. */
const state = () =>
	page.evaluate(() => {
		const v = document.querySelector('.deck video')
		return {
			status: document.querySelector('.deck .badge')?.textContent ?? '',
			head: document.querySelector('.deck .under .mono')?.textContent ?? '',
			w: v.videoWidth,
			h: v.videoHeight,
			t: Math.round(v.currentTime * 10) / 10,
			paused: v.paused,
			ready: v.readyState,
		}
	})

// A moment two minutes back, picked on the axis (a one-hour span: two
// minutes is 1/30 of the width from the right).
const axis = page.locator('.axis svg')
const box = await axis.boundingBox()
await axis.click({ position: { x: box.width * (1 - 2 / 60), y: box.height / 2 } })
await page.waitForFunction(() => (document.querySelector('.deck video')?.currentTime ?? 0) > 0, null, { timeout: 30_000 })
const t1 = await state()
await page.waitForTimeout(3000)
const t2 = await state()
console.log('two minutes back:', JSON.stringify(t1), '→', JSON.stringify(t2))
await page.screenshot({ path: `${OUT}/playback-past.png`, fullPage: true })
if (!(t2.t > t1.t && t2.w > 0)) {
	console.log('FAIL: the recording did not play')
}

// Ten seconds ago: the relay's window.
await page.click('button:has-text("10 s ago")')
await page.waitForFunction(() => /relay|live/.test(document.querySelector('.deck .badge')?.textContent ?? ''), null, { timeout: 30_000 })
await page.waitForTimeout(2500)
const r1 = await state()
console.log('ten seconds ago:', JSON.stringify(r1))
await page.screenshot({ path: `${OUT}/playback-recent.png`, fullPage: true })
if (!/relay|live/.test(r1.status)) console.log('FAIL: the relay did not answer')

// Live, by the button, then back to the past.
await page.click('button:has-text("Live")')
await page.waitForFunction(() => /^live/.test(document.querySelector('.deck .badge')?.textContent ?? ''), null, { timeout: 30_000 })
console.log('live:', JSON.stringify(await state()))

// An export: from two minutes back to one minute back.
await axis.click({ position: { x: box.width * (1 - 2 / 60), y: box.height / 2 } })
await page.waitForFunction(() => (document.querySelector('.deck video')?.currentTime ?? 0) > 0, null, { timeout: 30_000 })
await page.click('button:has-text("mark start")')
await axis.click({ position: { x: box.width * (1 - 1.5 / 60), y: box.height / 2 } })
await page.waitForTimeout(1000)
await page.click('button:has-text("mark end")')
const [download] = await Promise.all([page.waitForEvent('download', { timeout: 120_000 }), page.click('button:has-text("download MP4")')])
const path = `${OUT}/${download.suggestedFilename()}`
await download.saveAs(path)
console.log('export:', path)
await page.screenshot({ path: `${OUT}/playback-export.png`, fullPage: true })

console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
