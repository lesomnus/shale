// test/tokens.mjs: the Tokens page against a running deployment with roster
// inside it (§40, §33.1), in headless Chromium: signs in, makes a token
// that views and reads, sees it shown once with the line a script writes,
// reads with it at the tenant API as a script would, sees it listed, ends it
// and sees it gone. Nothing of the token is in the address or the
// browser's storage afterwards. Then, of two cameras it adds as the signed
// in person, a token made with a new list of one of them (§33.1, *A key
// narrowed to some cameras*): the page offers it only what a list
// confines and says why, the token lists that camera alone at the tenant
// API, the token's list is shown beside it and the list with the token that
// carries it, and erasing the list says that token will reach nothing, which
// it then does. Exits non-zero on the first thing that is not so.
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
await page.fill('.make > label input', NAME)
await page.check('.make fieldset:not(.which) input[type=radio] >> nth=0')
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

// Two cameras, added as the signed-in person; a token of a list of one.
const api = (path, body, token) =>
	page.evaluate(
		async ([path, body, token]) => {
			const r = await fetch(path, {
				method: 'POST',
				// A token alone, as a script's: beside the session's cookie the
				// session is who calls.
				credentials: token ? 'omit' : 'include',
				headers: { 'content-type': 'application/json', 'connect-protocol-version': '1', ...(token ? { authorization: `Bearer ${token}` } : {}) },
				body: JSON.stringify(body),
			})
			return { status: r.status, body: await r.json().catch(() => ({})) }
		},
		[path, body, token],
	)
const tag = Date.now().toString(36)
const set = await api('/shale.SetService/Add', { tenant: { alias: TENANT }, alias: `gate-${tag}` })
if (set.status !== 200) await fail(`could not add a set: ${JSON.stringify(set.body)}`)
const cams = []
for (const name of ['north door', 'south door']) {
	const v = await api('/shale.SourceService/Add', { set: { id: set.body.id }, alias: `${name.replace(' ', '-')}-${tag}`, name })
	if (v.status !== 200) await fail(`could not add a camera: ${JSON.stringify(v.body)}`)
	cams.push(v.body)
}
await page.reload()
await until('.make', 'no form to make a token')
const NARROW = `${NAME}-cams`
const LIST = `the north door ${tag}`
await page.fill('.make > label input', NARROW)
await page.check('.make fieldset.which input[type=radio] >> nth=1')
await until('.make .why-narrowed', 'choosing a list does not say why only some methods are offered')
const why = (await page.textContent('.make .why-narrowed')) ?? ''
if (!why.includes('would not be narrowed')) await fail(`the page does not say why: ${why}`)
const offered = (await page.textContent('.make fieldset:not(.which)')) ?? ''
if (offered.includes('Everything I may do here')) await fail('a token of a list is offered everything')
if ((await page.inputValue('.make fieldset.which select')) !== 'new') await page.selectOption('.make fieldset.which select', 'new')
await page.fill('.make .new-list > label input', LIST)
await page.check(`.make .cameras label.camera:has-text("north door") input`)
await page.check('.make fieldset:not(.which) input[type=radio] >> nth=0')
await page.screenshot({ path: `${OUT}/tokens-make-cams.png`, fullPage: true })
await page.click('.make button.primary')
await until('.token code', 'no token shown after make, with a list')
const narrow = (await page.textContent('.token code')) ?? ''
if (!narrow.startsWith('rt_')) await fail(`the token shown is not a key: ${narrow.slice(0, 3)}…`)
await page.click('.token button:has-text("done")')

// It lists that camera alone.
const seen = await api('/shale.SourceService/List', {}, narrow)
if (seen.status !== 200) await fail(`the token of a list does not list: ${seen.status} ${JSON.stringify(seen.body)}`)
const ids = (seen.body.items ?? []).map((v) => v.id)
if (ids.length !== 1 || ids[0] !== cams[0].id) await fail(`the token of a list of one camera lists ${JSON.stringify((seen.body.items ?? []).map((v) => v.name))}`)
const wider = await api('/shale.SetService/Patch', { ref: { id: set.body.id }, dateUpdatedForce: true }, narrow)
if (wider.status === 200) await fail('a token of a list may write')

// Listed with its list; the list with the token that carries it.
const narrowRow = page.locator('table.tokens tbody tr', { hasText: NARROW })
await narrowRow.waitFor({ timeout: 30_000 }).catch(() => fail('the token of a list is not listed'))
if (!((await narrowRow.textContent()) ?? '').includes(LIST)) await fail('the token is not listed with its list')
const listRow = page.locator('table.lists tbody tr', { hasText: LIST })
await listRow.waitFor({ timeout: 30_000 }).catch(() => fail('the list is not listed'))
if (!((await listRow.textContent()) ?? '').includes(NARROW)) await fail('the list does not say which token carries it')
await page.screenshot({ path: `${OUT}/tokens-cams.png`, fullPage: true })

// Erased: the confirmation names the token, which then reaches nothing.
let erasing = ''
page.once('dialog', (d) => {
	erasing = d.message()
	void d.accept()
})
await listRow.locator('button:has-text("erase")').click()
await listRow.waitFor({ state: 'detached', timeout: 30_000 }).catch(() => fail('the list is still listed after erase'))
if (!erasing.includes(NARROW) || !erasing.includes('reach nothing')) await fail(`the confirmation does not say the token will reach nothing: ${erasing}`)
await until('table.tokens .gone', 'the token does not say its list is gone')
const after = await api('/shale.SourceService/List', {}, narrow)
if (after.status !== 200 || (after.body.items ?? []).length !== 0) await fail(`a token whose list is gone lists ${JSON.stringify(after.body)}`)
await page.screenshot({ path: `${OUT}/tokens-list-erased.png`, fullPage: true })

console.log(`ok: made ${NAME}, read with it, listed it, ended it; made ${NARROW} of one camera, which listed it alone, and erased its list`)
console.log('errors:', logs.filter((l) => l.startsWith('[error]')).length)
await browser.close()
