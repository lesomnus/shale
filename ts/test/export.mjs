// test/export.mjs: the export's joining of laminae (src/export.ts) against
// recordings ffmpeg makes here, read back by ffprobe and played by headless
// Chromium from the file, as a download would be (§40):
//
//   node test/export.mjs
//
// Needs ffmpeg and ffprobe on PATH and `npx playwright install chromium`.
// The joined files are left under OUT.
import { execFileSync } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join as path } from 'node:path'
import { chromium } from 'playwright'
import { createServer } from 'vite'

const OUT = process.env.OUT ?? 'test/shots-export'
mkdirSync(OUT, { recursive: true })

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom', logLevel: 'error' })
const { join, joins, Changed } = await vite.ssrLoadModule('/src/export.ts')
const { boxes } = await vite.ssrLoadModule('/src/fmp4.ts')

/** record is 15 s of a picture and a tone as the producer writes them (§38.1). */
function record(name, size = '320x180') {
	const file = path(OUT, name)
	execFileSync('ffmpeg', [
		'-loglevel', 'error', '-y',
		'-f', 'lavfi', '-i', `testsrc2=size=${size}:rate=10`,
		'-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
		'-t', '15',
		'-c:v', 'libx264', '-profile:v', 'baseline', '-pix_fmt', 'yuv420p', '-g', '10', '-bf', '0',
		'-c:a', 'aac',
		'-movflags', '+frag_keyframe+empty_moov+default_base_moof',
		file,
	])
	const b = new Uint8Array(readFileSync(file))
	const all = boxes(b)
	const at = all.findIndex((x) => x.type === 'moof')
	// Where each fragment begins: frag_keyframe makes every one a keyframe's.
	const frags = all.filter((x) => x.type === 'moof').map((x) => x.off)

	return { init: b.subarray(0, all[at].off), b, frags }
}

/** piece is a lamina of a recording, fragments `from` to `to`, its session's zero at `zero` ms. */
function piece(r, zero, from = 0, to = r.frags.length) {
	const end = to < r.frags.length ? r.frags[to] : r.b.length
	return { init: r.init, fragments: r.b.slice(r.frags[from], end), zero }
}

/** probe is the video and audio packets of a file, by ffprobe, and its duration. */
function probe(file) {
	const packets = (stream) =>
		execFileSync('ffprobe', ['-v', 'error', '-select_streams', stream, '-show_entries', 'packet=dts_time,duration_time', '-of', 'csv=p=0', file])
			.toString()
			.trim()
			.split('\n')
			.map((l) => l.split(',').map(Number))
			.map(([dts, duration]) => ({ dts, duration }))
	const duration = Number(execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', file]).toString())
	const created = execFileSync('ffprobe', ['-v', 'error', '-show_entries', 'format_tags=creation_time', '-of', 'csv=p=0', file]).toString().trim()

	return { video: packets('v:0'), audio: packets('a:0'), duration, created }
}

let failed = 0
function check(what, ok, detail = '') {
	console.log(`${ok ? 'ok  ' : 'FAIL'} ${what}${detail === '' ? '' : `: ${detail}`}`)
	if (!ok) failed++
}
const near = (a, b, e = 0.15) => Math.abs(a - b) <= e

/** rising is whether decode times only go up. */
const rising = (ps) => ps.every((p, i) => i === 0 || p.dts > ps[i - 1].dts)

/** jump is the largest step between decode times and where it is. */
function jump(ps) {
	let at = 0
	let gap = 0
	for (let i = 1; i < ps.length; i++) {
		const d = ps[i].dts - ps[i - 1].dts
		if (d > gap) [gap, at] = [d, ps[i].dts]
	}
	return { gap, at }
}

const T0 = Date.parse('2026-10-05T01:00:00Z')
const a = record('a.mp4')
// ffmpeg's own first step (the encoder's first frame, AAC's priming) is the
// largest a recording has before any joining.
const own = { video: jump(probe(path(OUT, 'a.mp4')).video).gap, audio: jump(probe(path(OUT, 'a.mp4')).audio).gap }
const files = {}
function write(name, pieces) {
	const r = join(pieces)
	files[name] = path(OUT, name)
	writeFileSync(files[name], r.bytes)
	return { ...probe(files[name]), start: r.start }
}

// An encoder restarted with the same settings, 15 s on: decode times start
// again at zero in the second lamina.
{
	const p = write('restart.mp4', [piece(a, T0), piece(a, T0 + 15_000)])
	check('restart: video decode times rise', rising(p.video))
	check('restart: audio decode times rise', rising(p.audio))
	check('restart: 30 s long', near(p.duration, 30), `${p.duration}`)
	const j = jump(p.video)
	check('restart: no gap', j.gap <= own.video + 1e-6, `largest step ${j.gap.toFixed(3)} s at ${j.at}`)
	check('restart: creation time is the first sample', p.created === '2026-10-05T01:00:00.000000Z', p.created)
}

// The same, and 10 s with nothing recorded between them.
{
	const p = write('gap.mp4', [piece(a, T0), piece(a, T0 + 25_000)])
	check('gap: video decode times rise', rising(p.video))
	check('gap: audio decode times rise', rising(p.audio))
	const j = jump(p.video)
	check('gap: the second lamina begins at 25 s', near(j.at, 25) && near(j.gap, 10.1), `step ${j.gap.toFixed(3)} s at ${j.at}`)
	check('gap: 40 s long', near(p.duration, 40), `${p.duration}`)
	const ja = jump(p.audio)
	check('gap: audio begins again at 25 s', near(ja.at, 25), `at ${ja.at}`)
}

// One session cut into two laminae, the second's date_started 300 ms late:
// the session's own decode times go on, the wall clock's error does not.
{
	const half = Math.floor(a.frags.length / 2)
	const split = Number(execFileSync('ffprobe', ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'packet=dts_time', '-of', 'csv=p=0', path(OUT, 'a.mp4')]).toString().trim().split('\n')[half * 10])
	const p = write('session.mp4', [piece(a, T0, 0, half), piece(a, T0 + 300, half)])
	check('session: split at a keyframe', near(split, half), `${split}`)
	check('session: decode times rise', rising(p.video) && rising(p.audio))
	check('session: no gap', jump(p.video).gap <= own.video + 1e-6 && jump(p.audio).gap <= own.audio + 1e-6, `${jump(p.video).gap} ${jump(p.audio).gap}`)
	check('session: as recorded', p.video.length === probe(path(OUT, 'a.mp4')).video.length && p.video.every((x, i) => near(x.dts, probe(path(OUT, 'a.mp4')).video[i].dts, 1e-6)))
	check('session: 15 s long', near(p.duration, 15), `${p.duration}`)
}

// A range that begins at a keyframe in the middle of a lamina begins at zero.
{
	const p = write('middle.mp4', [piece(a, T0, 5), piece(a, T0 + 15_000)])
	check('middle: begins at zero', near(p.video[0].dts, 0, 0.01), `${p.video[0].dts}`)
	check('middle: the second lamina at 10 s', near(jump(p.video).at, 10), `${jump(p.video).at}`)
	check('middle: creation time is the first sample', p.created === '2026-10-05T01:00:05.000000Z', p.created)
	// The first sample is the keyframe at 5 s, or the audio beside it.
	check('middle: start is the first sample', near(p.start, T0 + 5000, 50), `${p.start - T0}`)
}

// Another picture size cannot share the init segment.
{
	const b = record('b.mp4', '640x360')
	let caught
	try {
		join([piece(a, T0), piece(b, T0 + 15_000)])
	} catch (err) {
		caught = err
	}
	check('changed: refused at the second lamina', caught instanceof Changed && caught.piece === 1, String(caught))
	check('changed: known from the init segments', !joins(a.init, b.init) && joins(a.init, record('c.mp4').init))
}

// Chromium plays the files as they are, through the gap.
{
	const browser = await chromium.launch()
	const page = await browser.newPage()
	await page.route('http://export.test/**', (route) => {
		const name = new URL(route.request().url()).pathname.slice(1)
		if (name === '') return route.fulfill({ contentType: 'text/html', body: '<video id=v muted></video>' })
		return route.fulfill({ contentType: 'video/mp4', body: readFileSync(files[name]) })
	})
	await page.goto('http://export.test/')
	for (const [name, until] of [['restart.mp4', 29], ['gap.mp4', 39]]) {
		const r = await page.evaluate(
			async ([name, until]) => {
				const v = document.getElementById('v')
				v.src = '/' + name
				await new Promise((ok, fail) => {
					v.onloadedmetadata = ok
					v.onerror = () => fail(new Error(String(v.error?.message)))
				})
				const duration = v.duration
				// Past the restart and the gap, quickly.
				v.playbackRate = 16
				await v.play()
				const t0 = performance.now()
				while (v.currentTime < until && !v.ended && performance.now() - t0 < 20_000) await new Promise((r) => setTimeout(r, 100))
				return { duration, reached: v.currentTime, error: v.error?.message ?? null }
			},
			[name, until],
		)
		check(`chromium: ${name} plays through`, r.error === null && r.reached >= until, JSON.stringify(r))
		check(`chromium: ${name} is as long as it is`, near(r.duration, until + 1, 0.2), `${r.duration}`)
	}
	await browser.close()
}

await vite.close()
console.log(failed === 0 ? 'all passed' : `${failed} failed`)
process.exit(failed === 0 ? 0 : 1)
