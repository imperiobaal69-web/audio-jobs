package audio

import (
	"bytes"
	"sync"
	"testing"
)

var (
	tenMinutesOnce sync.Once
	tenMinutes     []byte
)

// A 10-minute mono 16 kHz recording (19.2 MB), roughly a short meeting.
func tenMinuteWAV() []byte {
	tenMinutesOnce.Do(func() {
		tenMinutes = Synthesize(SynthSpec{Seed: 1, SampleRate: 16000, Channels: 1,
			DurationMs: 10 * 60 * 1000, PauseEvery: 4000, PauseMs: 900})
	})
	return tenMinutes
}

func BenchmarkAnalyze10Min(b *testing.B) {
	wav := tenMinuteWAV()
	b.SetBytes(int64(len(wav)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Analyze(bytes.NewReader(wav), DefaultParams); err != nil {
			b.Fatal(err)
		}
	}
}
