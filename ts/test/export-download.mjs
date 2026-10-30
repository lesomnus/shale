// Independently play the Go remuxer's files, including tail-moov seeking, and
// download an attachment through the browser's normal download manager.
//
// MP4EXPORT_ARTIFACTS=/tmp/shale-export MP4EXPORT_LARGE=1 \
//   go test ./internal/mp4export -run 'Test(ValidationArtifacts|LargeValidationArtifact)$'
// cd ts && node test/export-download.mjs /tmp/shale-export
//
// Needs Playwright's Chromium (`npx playwright install chromium`). Set
// CHROMIUM_EXECUTABLE for an existing compatible browser. No camera is contacted.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { createReadStream, existsSync, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { chromium } from 'playwright'

const dir = process.argv[2] ?? process.env.MP4EXPORT_ARTIFACTS
assert.ok(dir, 'usage: node test/export-download.mjs <MP4EXPORT_ARTIFACTS directory>')
const cases = [
	{ name: 'full-av-export.mp4', duration: 6.066667, seeks: [0.1, 3, 5] },
	{ name: 'gap-av-export.mp4', duration: 16.066667, seeks: [0.1, 5, 10.5, 15] },
	{ name: 'clipped-av-export.mp4', duration: 4.066667, seeks: [0.1, 2, 3.5] },
]
for (const item of [
	{ name: 'full-bframes-export.mp4', duration: 6.2, seeks: [0.3, 3, 5.5] },
	{ name: 'large-av-export.mp4', duration: 2744.966667, seeks: [0.5, 1372.5, 2743.5] },
]) if (existsSync(resolve(dir, item.name))) cases.push(item)
for (const item of cases) item.file = resolve(dir, item.name)

const requests = []
const server = createServer((req, res) => {
	if (req.url === '/') return res.writeHead(200, { 'Content-Type': 'text/html' }).end('<video id="v" muted controls></video><a id="download">Download</a>')
	const url = new URL(req.url, 'http://localhost')
	const attachment = url.pathname.startsWith('/download/')
	const item = cases.find((c) => url.pathname === `${attachment ? '/download/' : '/files/'}${c.name}`)
	if (item === undefined) return res.writeHead(404).end()
	const size = statSync(item.file).size
	const range = req.headers.range?.match(/^bytes=(\d+)-(\d*)$/)
	requests.push({ name: item.name, attachment, range: req.headers.range ?? null })
	if (attachment && req.headers.range !== undefined) return res.writeHead(400).end()
	const begin = range ? Number(range[1]) : 0
	const end = range && range[2] ? Math.min(Number(range[2]), size - 1) : size - 1
	if (begin >= size || end < begin) return res.writeHead(416, { 'Content-Range': `bytes */${size}` }).end()
	const headers = { 'Content-Type': 'video/mp4', 'Content-Length': String(end - begin + 1), 'Accept-Ranges': attachment ? 'none' : 'bytes' }
	if (range) headers['Content-Range'] = `bytes ${begin}-${end}/${size}`
	if (attachment) headers['Content-Disposition'] = `attachment; filename="${item.name}"`
	res.writeHead(range ? 206 : 200, headers)
	const stream = createReadStream(item.file, { start: begin, end, highWaterMark: 64 * 1024 })
	stream.on('error', () => res.destroy())
	res.on('close', () => stream.destroy())
	stream.pipe(res)
})

async function hash(file) {
	const sum = createHash('sha256')
	for await (const chunk of createReadStream(file)) sum.update(chunk)
	return sum.digest('hex')
}

await new Promise((ok) => server.listen(0, '127.0.0.1', ok))
let browser
try {
	browser = await chromium.launch(process.env.CHROMIUM_EXECUTABLE ? { executablePath: process.env.CHROMIUM_EXECUTABLE } : {})
	const context = await browser.newContext({ acceptDownloads: true })
	const page = await context.newPage()
	await page.goto(`http://127.0.0.1:${server.address().port}`)
	const results = []
	for (const item of cases) {
		const result = await page.evaluate(async (item) => {
			const v = document.getElementById('v')
			const event = (name) => new Promise((ok, fail) => {
				const timer = setTimeout(() => fail(Error(`timeout ${name} at ${v.currentTime}`)), 15_000)
				v.addEventListener(name, () => { clearTimeout(timer); ok() }, { once: true })
				v.addEventListener('error', () => { clearTimeout(timer); fail(Error(v.error?.message)) }, { once: true })
			})
			const loaded = event('loadeddata')
			v.src = '/files/' + item.name
			await loaded
			const out = { name: item.name, duration: v.duration, width: v.videoWidth, height: v.videoHeight, seeks: [] }
			for (const point of item.seeks) {
				const sought = event('seeked')
				v.currentTime = point
				await sought
				await v.play()
				const start = v.currentTime
				await new Promise((ok) => setTimeout(ok, 300))
				v.pause()
				out.seeks.push({ requested: point, start, reached: v.currentTime, frames: v.getVideoPlaybackQuality().totalVideoFrames, error: v.error?.message ?? null })
			}
			if (item.name === 'gap-av-export.mp4') {
				const sought = event('seeked')
				v.currentTime = 5.5
				await sought
				v.playbackRate = 8
				await v.play()
				const began = performance.now()
				while (v.currentTime < 10.5 && !v.ended && !v.error && performance.now() - began < 10_000) await new Promise((ok) => setTimeout(ok, 50))
				out.gapReached = v.currentTime
				v.pause()
				v.playbackRate = 1
			}
			return out
		}, item)
		assert.ok(Math.abs(result.duration - item.duration) < 0.01, `${item.name}: duration ${result.duration}, expected ${item.duration}`)
		assert.ok(result.width > 0 && result.height > 0)
		for (const seek of result.seeks) assert.ok(seek.reached > seek.start && seek.frames > 0 && seek.error === null, JSON.stringify(seek))
		if (item.name === 'gap-av-export.mp4') assert.ok(result.gapReached >= 10.5, `playback stopped in recording gap at ${result.gapReached}`)
		results.push(result)
	}
	const target = cases.find((c) => c.name === 'large-av-export.mp4') ?? cases[0]
	await page.locator('#download').evaluate((a, name) => { a.href = '/download/' + name }, target.name)
	const [download] = await Promise.all([page.waitForEvent('download'), page.click('#download')])
	assert.equal(await download.failure(), null)
	const file = await download.path()
	assert.equal(statSync(file).size, statSync(target.file).size)
	const digest = await hash(file)
	assert.equal(digest, await hash(target.file))
	assert.equal(requests.filter((r) => r.attachment).length, 1)
	assert.ok(requests.filter((r) => r.attachment).every((r) => r.range === null))
	console.log(JSON.stringify({ browser: browser.version(), results, download: { name: download.suggestedFilename(), bytes: statSync(file).size, sha256: digest }, requests }, null, 2))
	await download.delete()
	await context.close()
} finally {
	await browser?.close()
	await new Promise((ok) => server.close(ok))
}
