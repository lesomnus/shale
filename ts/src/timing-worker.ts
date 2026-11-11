/**
 * The worker that reads timed frames (§39.4): each encoded video frame a
 * receiver gets passes through here untouched, and the timing SEI in it, if
 * any, goes to the page with the frame's RTP timestamp, which is how the
 * page knows it again when it is shown.
 *
 * @module
 */

import { readTiming, type Timed } from './sei.js'

// The page's tsconfig types the DOM, not a worker's scope: the little of it
// used here, as the WebRTC Encoded Transform spec has it.
interface Transformer {
	readable: ReadableStream<RTCEncodedVideoFrame>
	writable: WritableStream<RTCEncodedVideoFrame>
	options?: { id?: number }
}
const scope = self as unknown as {
	onrtctransform: ((ev: { transformer: Transformer }) => void) | null
	postMessage: (m: Timed) => void
}

scope.onrtctransform = (ev) => {
	const t = ev.transformer
	const id = t.options?.id ?? 0
	void t.readable
		.pipeThrough(
			new TransformStream<RTCEncodedVideoFrame, RTCEncodedVideoFrame>({
				transform(frame, out) {
					const timing = readTiming(new Uint8Array(frame.data))
					const rtp = frame.getMetadata().rtpTimestamp
					if (timing !== undefined && rtp !== undefined) scope.postMessage({ id, rtp, ...timing })
					out.enqueue(frame)
				},
			}),
		)
		.pipeTo(t.writable)
}
