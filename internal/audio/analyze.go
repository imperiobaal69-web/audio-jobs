package audio

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// Version identifies the analysis algorithm. It is part of the result cache
// key, so changing the algorithm invalidates cached results.
const Version = "v1"

// Params control the analysis.
type Params struct {
	WaveformBuckets  int     // points in the waveform overview
	WindowMs         int     // RMS window for silence detection
	SilenceDBFS      float64 // windows quieter than this are silent
	MinSilenceMs     int     // shorter quiet runs are not reported
	DigitalSilenceDB float64 // floor for dBFS of an all-zero signal
}

// DefaultParams are tuned for speech recordings.
var DefaultParams = Params{
	WaveformBuckets:  200,
	WindowMs:         50,
	SilenceDBFS:      -45,
	MinSilenceMs:     300,
	DigitalSilenceDB: -120,
}

// Segment is a time range in milliseconds.
type Segment struct {
	StartMs int64 `json:"start_ms"`
	EndMs   int64 `json:"end_ms"`
}

// Result is the output of one analysis.
type Result struct {
	Version     string    `json:"version"`
	SampleRate  int       `json:"sample_rate"`
	Channels    int       `json:"channels"`
	DurationMs  int64     `json:"duration_ms"`
	PeakDBFS    float64   `json:"peak_dbfs"`
	RMSDBFS     float64   `json:"rms_dbfs"`
	Waveform    []float32 `json:"waveform"`
	Silences    []Segment `json:"silences"`
	SpeechRatio float64   `json:"speech_ratio"`
}

// Analyze reads a WAV stream and computes the Result.
func Analyze(r io.ReadSeeker, p Params) (Result, error) {
	h, err := ParseHeader(r)
	if err != nil {
		return Result{}, err
	}
	if _, err := r.Seek(h.DataOffset, io.SeekStart); err != nil {
		return Result{}, err
	}
	raw := make([]byte, h.DataSize)
	if _, err := io.ReadFull(r, raw); err != nil {
		return Result{}, fmt.Errorf("read samples: %w", err)
	}

	// Decode to mono float64 in [-1, 1].
	var samples []float64
	frameBytes := h.Channels * 2
	for i := 0; i+frameBytes <= len(raw); i += frameBytes {
		var sum float64
		for c := 0; c < h.Channels; c++ {
			v := int16(binary.LittleEndian.Uint16(raw[i+2*c:]))
			sum += float64(v) / 32768.0
		}
		samples = append(samples, sum/float64(h.Channels))
	}

	res := Result{
		Version:    Version,
		SampleRate: h.SampleRate,
		Channels:   h.Channels,
		DurationMs: int64(len(samples)) * 1000 / int64(h.SampleRate),
		Waveform:   []float32{},
		Silences:   []Segment{},
	}
	if len(samples) == 0 {
		res.PeakDBFS, res.RMSDBFS = p.DigitalSilenceDB, p.DigitalSilenceDB
		return res, nil
	}

	var peak, sumSq float64
	for _, s := range samples {
		if math.Abs(s) > peak {
			peak = math.Abs(s)
		}
		sumSq += math.Pow(s, 2)
	}
	res.PeakDBFS = toDB(peak, p.DigitalSilenceDB)
	res.RMSDBFS = toDB(math.Sqrt(sumSq/float64(len(samples))), p.DigitalSilenceDB)

	// Waveform: max absolute amplitude per bucket.
	buckets := p.WaveformBuckets
	if buckets > len(samples) {
		buckets = len(samples)
	}
	for b := 0; b < buckets; b++ {
		start := b * len(samples) / buckets
		end := (b + 1) * len(samples) / buckets
		bucket := samples[start:end]
		var m float64
		for _, s := range bucket {
			if math.Abs(s) > m {
				m = math.Abs(s)
			}
		}
		res.Waveform = append(res.Waveform, float32(m))
	}

	// Silence: per-window RMS, then runs of quiet windows.
	window := h.SampleRate * p.WindowMs / 1000
	type win struct {
		startMs, endMs int64
		db             float64
	}
	var wins []win
	for start := 0; start < len(samples); start += window {
		end := start + window
		if end > len(samples) {
			end = len(samples)
		}
		var ss float64
		for _, s := range samples[start:end] {
			ss += math.Pow(s, 2)
		}
		db := toDB(math.Sqrt(ss/float64(end-start)), p.DigitalSilenceDB)
		wins = append(wins, win{
			startMs: int64(start) * 1000 / int64(h.SampleRate),
			endMs:   int64(end) * 1000 / int64(h.SampleRate),
			db:      db,
		})
	}
	var silentMs int64
	runStart := int64(-1)
	var runEnd int64
	flush := func() {
		if runStart >= 0 && runEnd-runStart >= int64(p.MinSilenceMs) {
			res.Silences = append(res.Silences, Segment{StartMs: runStart, EndMs: runEnd})
			silentMs += runEnd - runStart
		}
		runStart = -1
	}
	for _, w := range wins {
		if w.db < p.SilenceDBFS {
			if runStart < 0 {
				runStart = w.startMs
			}
			runEnd = w.endMs
		} else {
			flush()
		}
	}
	flush()
	if res.DurationMs > 0 {
		res.SpeechRatio = 1 - float64(silentMs)/float64(res.DurationMs)
	}
	return res, nil
}

func toDB(amplitude, floor float64) float64 {
	if amplitude <= 0 {
		return floor
	}
	db := 20 * math.Log10(amplitude)
	if db < floor {
		return floor
	}
	return db
}
