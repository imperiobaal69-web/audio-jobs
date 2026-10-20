package audio

import (
	"bufio"
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

// Analyze computes the Result in one streaming pass with constant memory:
// samples are decoded from a fixed read buffer and folded into running
// accumulators (global peak and sum of squares, the current waveform bucket,
// the current silence window), so a two-hour recording needs no more memory
// than a two-second one.
func Analyze(r io.ReadSeeker, p Params) (Result, error) {
	h, err := ParseHeader(r)
	if err != nil {
		return Result{}, err
	}
	if _, err := r.Seek(h.DataOffset, io.SeekStart); err != nil {
		return Result{}, err
	}
	frames := h.Frames()
	res := Result{
		Version:    Version,
		SampleRate: h.SampleRate,
		Channels:   h.Channels,
		DurationMs: frames * 1000 / int64(h.SampleRate),
		Waveform:   []float32{},
		Silences:   []Segment{},
	}
	if frames == 0 {
		res.PeakDBFS, res.RMSDBFS = p.DigitalSilenceDB, p.DigitalSilenceDB
		return res, nil
	}

	buckets := int64(p.WaveformBuckets)
	if buckets > frames {
		buckets = frames
	}
	res.Waveform = make([]float32, 0, buckets)
	window := int64(h.SampleRate * p.WindowMs / 1000)
	rate := int64(h.SampleRate)
	channels := h.Channels
	frameBytes := channels * 2
	invChannels := 1 / float64(channels)

	var (
		peak, sumSq   float64
		bucket        int64 // current waveform bucket
		bucketEnd     = frames / buckets
		bucketMax     float64
		winStart      int64 // first frame of the current silence window
		winSumSq      float64
		runStart      = int64(-1)
		runEnd        int64
		silentMs      int64
		minSilence    = int64(p.MinSilenceMs)
		silenceThresh = p.SilenceDBFS
	)
	closeWindow := func(end int64) {
		db := toDB(math.Sqrt(winSumSq/float64(end-winStart)), p.DigitalSilenceDB)
		startMs, endMs := winStart*1000/rate, end*1000/rate
		if db < silenceThresh {
			if runStart < 0 {
				runStart = startMs
			}
			runEnd = endMs
		} else if runStart >= 0 {
			if runEnd-runStart >= minSilence {
				res.Silences = append(res.Silences, Segment{StartMs: runStart, EndMs: runEnd})
				silentMs += runEnd - runStart
			}
			runStart = -1
		}
		winStart, winSumSq = end, 0
	}

	br := bufio.NewReaderSize(r, 64<<10)
	buf := make([]byte, 64<<10-(64<<10)%frameBytes) // whole frames only
	var frame int64
	for remaining := frames * int64(frameBytes); remaining > 0; {
		chunk := buf[:min(int64(len(buf)), remaining)]
		if n, err := io.ReadFull(br, chunk); err != nil {
			return Result{}, fmt.Errorf("read samples: got %d of %d frames: %w",
				frame+int64(n/frameBytes), frames, io.ErrUnexpectedEOF)
		}
		remaining -= int64(len(chunk))
		for i := 0; i < len(chunk); i += frameBytes {
			var s float64
			if channels == 1 {
				s = float64(int16(binary.LittleEndian.Uint16(chunk[i:]))) / 32768.0
			} else {
				l := float64(int16(binary.LittleEndian.Uint16(chunk[i:]))) / 32768.0
				rr := float64(int16(binary.LittleEndian.Uint16(chunk[i+2:]))) / 32768.0
				s = (l + rr) * invChannels
			}
			a := math.Abs(s)
			if a > peak {
				peak = a
			}
			sq := s * s
			sumSq += sq
			winSumSq += sq
			if a > bucketMax {
				bucketMax = a
			}
			frame++
			if frame == bucketEnd {
				res.Waveform = append(res.Waveform, float32(bucketMax))
				bucketMax = 0
				bucket++
				bucketEnd = (bucket + 1) * frames / buckets
			}
			if frame-winStart == window {
				closeWindow(frame)
			}
		}
	}
	if winStart < frames {
		closeWindow(frames)
	}
	if runStart >= 0 && runEnd-runStart >= minSilence {
		res.Silences = append(res.Silences, Segment{StartMs: runStart, EndMs: runEnd})
		silentMs += runEnd - runStart
	}

	res.PeakDBFS = toDB(peak, p.DigitalSilenceDB)
	res.RMSDBFS = toDB(math.Sqrt(sumSq/float64(frames)), p.DigitalSilenceDB)
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
