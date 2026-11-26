// test/tokens.mjs: the Tokens page against a running deployment with roster
// inside it (§40, §33.1), in headless Chromium: signs in, makes a token
// that views and reads, sees it shown once with the line a script writes,
// reads with it at the tenant API as a script would, sees it listed, ends it
// and sees it gone. Nothing of the token is in the address or the
// browser's storage afterwards. Exits non-zero on the first thing that is
// not so.
//
//   BASE=http://127.0.0.1:7402 TENANT_PW=... node test/tokens.mjs
//
// TENANT (acme), ADMIN (admin), NAME (the token's) and OUT can be set too.
// With NO_OPERATORS=1 it is a deployment that names no operators
// (auth.operators unset, roster inside): the page says nobody can hold a
// role of Shale's and what an operator sets, offers no form, and stops.
import { chromium } from 'playwright'
import { mkdirSync } from 'node:fs'

const OUT = process.env.OUT ?? 'test/shots-tokens'
const BASE = process.env.BASE ?? 'http://127.0.0.1:7402'
const TENANT = process.env.TENANT ?? 'acme'
const ADMIN = process.env.ADMIN ?? 'admin'
const TENANT_PW = process.env.TENANT_PW ?? ''
const NAME = process.env.NAME ?? `page-${Date.now().toString(36)}`
mkdirSync(OUT, { recursive: true })

const browser = await chromium.launch(process.env.CHROMIUM_EXECUTABLE ? { executablePath: process.env.CHROMIUM_EXECUTABLE } : {})
const context = await browser.newContext({ viewport: { width: 1280, height: 900 }, ignoreHTTPSErrors: true })
await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: new URL(BASE).origin })
const page = await context.newPage()
const logs = []
page.on('console', (m) => logs.push(`[${m.type()}] ${m.text()}`))
page.on('pageerror', (e) => console.log('[pageerror]', String(e).slice(0, 300)))

async function fail(why) {
	console.log(`FAIL: ${why}`)
	await page.screenshot({ path: `${OUT}/tokens-failed.png`, fullPage: true }).catch(() => {})
	for (const l of logs.slice(-10)) console.log(l.slice(0, 300))
	process.exit(1)
}

/** until is a selector drawn, or the test failed saying which. */
async function until(selector, why) {
	await page.waitForSelector(selector, { timeout: 30_000 }).catch(() => fail(why))
}

await page.goto(BASE + '/#/tokens')
await until('form.sign-in', 'no sign-in on the Tokens page')
await page.fill('form.sign-in input[autocomplete=organization]', TENANT)
await page.fill('form.sign-in input[autocomplete=username]', ADMIN)
await page.fill('form.sign-in input[type=password]', TENANT_PW)
await page.click('form.sign-in button[type=submit]')
if (process.env.NO_OPERATORS === '1') {
	await until('.none-held', 'no sentence saying why no token can be made')
	const why = (await page.textContent('.none-held')) ?? ''
	if (!why.includes('names no operators') || !why.includes('auth.operators')) await fail(`the page does not say why: ${why}`)
	if (why.includes('Ask an administrator')) await fail('the page sends the person to an administrator who has nothing to give')
	if ((await page.locator('.make').count()) !== 0) await fail('a form to make a token nobody can make')
	await page.screenshot({ path: `${OUT}/tokens-no-operators.png`, fullPage: true })
	console.log(`ok: ${why}`)
	await browser.close()
	process.exit(0)
}
await until('.make', 'no form to make a token')

// A token that views and reads, ending in 90 days, the default.
await page.fill('.make input:not([type])', NAME)
await page.check('.make input[type=radio] >> nth=0')
if ((await page.inputValue('.make select')) !== '90') await fail('90 days is not the default')
await page.screenshot({ path: `${OUT}/tokens-make.png`, fullPage: true })
await page.click('.make button.primary')

// Shown once: the token, the line, the sentence.
await until('.token code', 'no token shown after make')
const token = (await page.textContent('.token code')) ?? ''
if (!token.startsWith('rt_')) await fail(`the token shown is not a key: ${token.slice(0, 3)}…`)
const shown = (await page.textContent('.token')) ?? ''
if (!shown.includes(`export SHALE_CLIENT_TOKEN=${token}`)) await fail('the line a script writes is not shown')
if (!shown.includes('only time it is shown')) await fail('the page does not say it is not shown again')
await page.click('.token button.primary')
if ((await page.evaluate(() => navigator.clipboard.readText())) !== token) await fail('copy did not copy the token')
await page.screenshot({ path: `${OUT}/tokens-made.png`, fullPage: true })

// It reads, at the tenant API, as a script's would.
const listed = await fetch(`${BASE}/shale.SetService/List`, {
	method: 'POST',
	headers: { 'content-type': 'application/json', 'connect-protocol-version': '1', authorization: `Bearer ${token}` },
	body: '{}',
})
if (listed.status !== 200) await fail(`the token does not read: ${listed.status} ${await listed.text()}`)

// Listed, by its name, as viewing and reading.
const row = page.locator('tbody tr', { hasText: NAME })
await row.waitFor({ timeout: 30_000 }).catch(() => fail('the token is not listed'))
if (!((await row.textContent()) ?? '').includes('View and read')) await fail('the token is not listed as View and read')

// Nothing of it in the address or the browser's storage.
const kept = await page.evaluate(() => JSON.stringify([location.href, { ...localStorage }, { ...sessionStorage }]))
if (kept.includes(token)) await fail('the token is kept where the page can read it again')

// Ended, with the confirmation saying when scripts stop.
await page.click('.token button:has-text("done")')
let asked = ''
page.once('dialog', (d) => {
	asked = d.message()
	void d.accept()
})
await row.locator('button:has-text("end")').click()
await row.waitFor({ state: 'detached', timeout: 30_000 }).catch(() => fail('the token is still listed after end'))
if (!/stop within \d+ seconds/.test(asked)) await fail(`the confirmation does not say when scripts stop: ${asked}`)
await page.screenshot({ path: `${OUT}/tokens-ended.png`, fullPage: true })

console.log(`ok: made ${NAME}, read with it, listed it, ended it`)
console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
