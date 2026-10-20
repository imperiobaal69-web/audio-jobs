package audio

import (
	"bytes"
	"math"
	"math/rand/v2"
)

// SynthSpec describes a deterministic speech-like test recording: alternating
// voiced segments (amplitude-modulated harmonics plus noise) and pauses.
type SynthSpec struct {
	Seed       uint64
	SampleRate int
	Channels   int
	DurationMs int
	PauseEvery int // ms of speech between pauses
	PauseMs    int // ms of each pause
}

// Synthesize renders the spec as 16-bit PCM WAV bytes.
func Synthesize(s SynthSpec) []byte {
	rng := rand.New(rand.NewPCG(s.Seed, s.Seed^0x9e3779b97f4a7c15))
	frames := s.SampleRate * s.DurationMs / 1000
	samples := make([]int16, frames*s.Channels)
	cycle := s.PauseEvery + s.PauseMs
	f0 := 110 + rng.Float64()*120 // fundamental, Hz
	for i := 0; i < frames; i++ {
		t := float64(i) / float64(s.SampleRate)
		ms := i * 1000 / s.SampleRate
		var v float64
		if cycle == 0 || ms%cycle < s.PauseEvery {
			envelope := 0.5 + 0.5*math.Sin(2*math.Pi*3.5*t) // syllable rate
			v = envelope * (0.30*math.Sin(2*math.Pi*f0*t) +
				0.15*math.Sin(2*math.Pi*2*f0*t) +
				0.08*math.Sin(2*math.Pi*3*f0*t))
			v += 0.02 * (rng.Float64()*2 - 1)
		} else {
			v = 0.001 * (rng.Float64()*2 - 1) // room noise, about -66 dBFS
		}
		q := int16(math.Max(-32768, math.Min(32767, v*32767)))
		for c := 0; c < s.Channels; c++ {
			samples[i*s.Channels+c] = q
		}
	}
	var buf bytes.Buffer
	_ = EncodePCM16(&buf, Format{SampleRate: s.SampleRate, Channels: s.Channels, BitsPerSample: 16}, samples)
	return buf.Bytes()
}
