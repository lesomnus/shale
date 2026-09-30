// A lamina, or the relay's recent window, through a browser's Media Source
// Extensions as it is (§38.1, §39.4): headless Chromium appends each file
// to one SourceBuffer and reports what it buffered and whether it played.
//
//   node test/mse.mjs <file.mp4>...
//
import { chromium } from 'playwright';
import { readFileSync } from 'node:fs';
const files = process.argv.slice(2);
const b = await chromium.launch();
const page = await b.newPage();
page.on('console', (m) => { if (m.type() === 'error') console.log('page:', m.text()); });
await page.route('http://mse.test/**', (route) => {
  const u = new URL(route.request().url());
  if (u.pathname === '/') return route.fulfill({ contentType: 'text/html', body: '<video id=v muted></video>' });
  const i = Number(u.pathname.slice(1));
  return route.fulfill({ contentType: 'video/mp4', body: readFileSync(files[i]) });
});
await page.goto('http://mse.test/');
for (let i = 0; i < files.length; i++) {
  const r = await page.evaluate(async (i) => {
    const v = document.getElementById('v');
    const mime = 'video/mp4; codecs="avc1.640028,mp4a.40.2,opus"';
    if (!MediaSource.isTypeSupported(mime)) return { err: 'unsupported ' + mime };
    const ms = new MediaSource();
    v.src = URL.createObjectURL(ms);
    await new Promise((r) => ms.addEventListener('sourceopen', r, { once: true }));
    const sb = ms.addSourceBuffer(mime);
    const bytes = new Uint8Array(await (await fetch('/' + i)).arrayBuffer());
    const errs = [];
    sb.addEventListener('error', () => errs.push('sourcebuffer error'));
    v.addEventListener('error', () => errs.push('video error ' + (v.error && v.error.message)));
    sb.appendBuffer(bytes);
    await new Promise((r) => sb.addEventListener('updateend', r, { once: true }));
    const ranges = [];
    for (let k = 0; k < sb.buffered.length; k++) ranges.push([sb.buffered.start(k), sb.buffered.end(k)]);
    ms.endOfStream();
    v.currentTime = ranges.length ? ranges[0][0] : 0;
    await v.play().catch((e) => errs.push('play: ' + e.message));
    await new Promise((r) => setTimeout(r, 1500));
    return { ranges, width: v.videoWidth, height: v.videoHeight, time: v.currentTime, audioTracks: v.audioTracks ? v.audioTracks.length : -1, errs };
  }, i);
  console.log(files[i].split('/').pop(), JSON.stringify(r));
}
await b.close();
