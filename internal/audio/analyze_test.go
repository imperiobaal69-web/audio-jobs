package audio

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func speech(durationMs int) SynthSpec {
	return SynthSpec{Seed: 7, SampleRate: 16000, Channels: 1, DurationMs: durationMs, PauseEvery: 2000, PauseMs: 700}
}

func TestParseHeaderSkipsUnknownChunks(t *testing.T) {
	wav := Synthesize(speech(100))
	// Insert a LIST chunk (odd size, so it needs a pad byte) between fmt and data.
	list := append([]byte("LIST\x03\x00\x00\x00abc"), 0)
	patched := append(append(append([]byte{}, wav[:36]...), list...), wav[36:]...)
	h, err := ParseHeader(bytes.NewReader(patched))
	if err != nil {
		t.Fatal(err)
	}
	if h.SampleRate != 16000 || h.Channels != 1 || h.DataOffset != 44+int64(len(list)) || h.Frames() != 1600 {
		t.Fatalf("unexpected header %+v", h)
	}
}

func TestParseHeaderRejectsNonPCM16(t *testing.T) {
	wav := Synthesize(speech(10))
	wav[34] = 24 // bits per sample
	if _, err := ParseHeader(bytes.NewReader(wav)); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("want ErrUnsupported, got %v", err)
	}
	if _, err := ParseHeader(bytes.NewReader([]byte("not a wav file at all"))); err == nil {
		t.Fatal("want error for garbage input")
	}
}

func TestAnalyzeFindsPausesInSpeech(t *testing.T) {
	// 10 s of speech with a 700 ms pause after every 2 s: pauses start at
	// 2.0, 4.7, 7.4 s; the last cycle (10.1 s) is past the end.
	res, err := Analyze(bytes.NewReader(Synthesize(speech(10_000))), DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	if res.DurationMs != 10_000 || res.SampleRate != 16000 || len(res.Waveform) != 200 {
		t.Fatalf("unexpected result shape: %+v", res)
	}
	want := []Segment{{2000, 2700}, {4700, 5400}, {7400, 8100}}
	if len(res.Silences) != len(want) {
		t.Fatalf("silences = %v, want %v", res.Silences, want)
	}
	for i, s := range res.Silences {
		// Window granularity is 50 ms; the envelope can make a window around
		// the boundary quiet too.
		if abs(s.StartMs-want[i].StartMs) > 100 || abs(s.EndMs-want[i].EndMs) > 100 {
			t.Fatalf("silence %d = %v, want about %v", i, s, want[i])
		}
	}
	if res.SpeechRatio < 0.75 || res.SpeechRatio > 0.82 {
		t.Fatalf("speech ratio %.3f, want about 0.79", res.SpeechRatio)
	}
	if res.PeakDBFS > 0 || res.PeakDBFS < -12 || res.RMSDBFS >= res.PeakDBFS {
		t.Fatalf("levels peak=%.1f rms=%.1f", res.PeakDBFS, res.RMSDBFS)
	}
}

func TestStereoIsDownmixedLikeMono(t *testing.T) {
	mono := speech(3000)
	stereo := mono
	stereo.Channels = 2
	a, err := Analyze(bytes.NewReader(Synthesize(mono)), DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Analyze(bytes.NewReader(Synthesize(stereo)), DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	if b.Channels != 2 || a.DurationMs != b.DurationMs || math.Abs(a.RMSDBFS-b.RMSDBFS) > 1e-9 {
		t.Fatalf("mono %+v vs stereo %+v", a, b)
	}
}

func TestEmptyAndDigitalSilence(t *testing.T) {
	var buf bytes.Buffer
	if err := EncodePCM16(&buf, Format{SampleRate: 8000, Channels: 1, BitsPerSample: 16}, nil); err != nil {
		t.Fatal(err)
	}
	res, err := Analyze(bytes.NewReader(buf.Bytes()), DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	if res.DurationMs != 0 || res.PeakDBFS != -120 || len(res.Silences) != 0 {
		t.Fatalf("empty: %+v", res)
	}

	buf.Reset()
	_ = EncodePCM16(&buf, Format{SampleRate: 8000, Channels: 1, BitsPerSample: 16}, make([]int16, 8000))
	res, err = Analyze(bytes.NewReader(buf.Bytes()), DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	if res.SpeechRatio != 0 || len(res.Silences) != 1 || res.Silences[0] != (Segment{0, 1000}) {
		t.Fatalf("digital silence: %+v", res)
	}
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// The streaming analyzer must agree with the reference implementation.
func TestAnalyzeMatchesReference(t *testing.T) {
	specs := []SynthSpec{
		speech(10_000),
		{Seed: 3, SampleRate: 16000, Channels: 2, DurationMs: 7_321, PauseEvery: 1200, PauseMs: 650},
		{Seed: 4, SampleRate: 44100, Channels: 1, DurationMs: 61_000, PauseEvery: 5000, PauseMs: 2000},
		{Seed: 5, SampleRate: 8000, Channels: 1, DurationMs: 37, PauseEvery: 10, PauseMs: 10}, // fewer frames than buckets
	}
	for _, spec := range specs {
		wav := Synthesize(spec)
		want, err := AnalyzeReference(bytes.NewReader(wav), DefaultParams)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Analyze(bytes.NewReader(wav), DefaultParams)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(got.RMSDBFS-want.RMSDBFS) > 1e-9 || got.PeakDBFS != want.PeakDBFS ||
			got.DurationMs != want.DurationMs || got.SpeechRatio != want.SpeechRatio {
			t.Fatalf("%+v: levels differ: got %+v want %+v", spec, got, want)
		}
		if len(got.Waveform) != len(want.Waveform) || len(got.Silences) != len(want.Silences) {
			t.Fatalf("%+v: shape differs", spec)
		}
		for i := range want.Waveform {
			if got.Waveform[i] != want.Waveform[i] {
				t.Fatalf("%+v: waveform[%d] %v != %v", spec, i, got.Waveform[i], want.Waveform[i])
			}
		}
		for i := range want.Silences {
			if got.Silences[i] != want.Silences[i] {
				t.Fatalf("%+v: silence[%d] %v != %v", spec, i, got.Silences[i], want.Silences[i])
			}
		}
	}
}

func TestTruncatedDataIsAnError(t *testing.T) {
	wav := Synthesize(speech(1000))
	if _, err := Analyze(bytes.NewReader(wav[:len(wav)-100]), DefaultParams); err == nil {
		t.Fatal("want error for truncated data")
	}
}
