package relay

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"
)

// A viewer that reports a video packet lost gets it again (§39.4): the
// answer offers NACK feedback for the video, and the relay's NACK
// responder sends a packet a NACK names once more.
func TestWhepNack(t *testing.T) {
	r := &Relay{m: newMetrics(context.Background())}
	w, err := newWhepServer(r)
	require.NoError(t, err)

	// The relay's side, as post sets it up.
	pc, err := w.api.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer pc.Close()
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000}, "video", "shale")
	require.NoError(t, err)
	sender, err := pc.AddTrack(track)
	require.NoError(t, err)
	go w.readRTCP(sender)

	// The viewer's, as a browser's: receive only.
	viewer, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	defer viewer.Close()
	_, err = viewer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
	require.NoError(t, err)

	type got struct {
		seq  uint16
		ssrc webrtc.SSRC
	}
	packets := make(chan got, 4096)
	viewer.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			p, _, err := tr.ReadRTP()
			if err != nil {
				return
			}
			select {
			case packets <- got{p.SequenceNumber, tr.SSRC()}:
			default:
			}
		}
	})

	offer, err := viewer.CreateOffer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(viewer)
	require.NoError(t, viewer.SetLocalDescription(offer))
	<-gathered
	require.NoError(t, pc.SetRemoteDescription(*viewer.LocalDescription()))
	answer, err := pc.CreateAnswer(nil)
	require.NoError(t, err)
	gathered = webrtc.GatheringCompletePromise(pc)
	require.NoError(t, pc.SetLocalDescription(answer))
	<-gathered
	sdp := pc.LocalDescription().SDP
	h264 := regexp.MustCompile(`a=rtpmap:(\d+) H264/90000`).FindStringSubmatch(sdp)
	require.NotNil(t, h264, "the answer has H.264")
	require.Contains(t, sdp, "a=rtcp-fb:"+h264[1]+" nack\r\n", "and NACK feedback for it")
	require.NoError(t, viewer.SetRemoteDescription(*pc.LocalDescription()))

	// Frames of a few packets each, until the test is done.
	done := make(chan struct{})
	defer close(done)
	go func() {
		frame := append([]byte{0, 0, 0, 1, 0x41}, make([]byte, 3000)...)
		for {
			select {
			case <-done:
				return
			case <-time.After(30 * time.Millisecond):
				track.WriteSample(media.Sample{Data: frame, Duration: 33 * time.Millisecond})
			}
		}
	}()

	var lost got
	for i := 0; i < 20; i++ {
		select {
		case p := <-packets:
			if i == 5 {
				lost = p
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no video")
		}
	}
	require.NoError(t, viewer.WriteRTCP([]rtcp.Packet{&rtcp.TransportLayerNack{
		MediaSSRC: uint32(lost.ssrc),
		Nacks:     rtcp.NackPairsFromSequenceNumbers([]uint16{lost.seq}),
	}}))
	deadline := time.After(5 * time.Second)
	for {
		select {
		case p := <-packets:
			if p == lost {
				return
			}
		case <-deadline:
			t.Fatalf("packet %d was not sent again", lost.seq)
		}
	}
}

// What NACKs ask for, counted.
func TestNackedPackets(t *testing.T) {
	b, err := rtcp.Marshal([]rtcp.Packet{
		&rtcp.ReceiverReport{SSRC: 1},
		&rtcp.TransportLayerNack{MediaSSRC: 2, Nacks: rtcp.NackPairsFromSequenceNumbers([]uint16{10, 11, 12, 40})},
		&rtcp.PictureLossIndication{MediaSSRC: 2},
	})
	require.NoError(t, err)
	require.EqualValues(t, 4, nackedPackets(b))
	require.Zero(t, nackedPackets([]byte{1, 2}))
}
