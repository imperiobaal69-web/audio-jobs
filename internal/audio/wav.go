// Package audio parses PCM WAV files and computes the analysis a recording
// pipeline needs before transcription: duration, levels, a waveform for the
// player, and silence / speech segments.
package audio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Format describes a PCM stream.
type Format struct {
	SampleRate    int
	Channels      int
	BitsPerSample int
}

// ErrUnsupported is returned for WAV files that are valid but not 16-bit PCM.
var ErrUnsupported = errors.New("unsupported wav format")

// Header is a parsed WAV header plus the location of the sample data.
type Header struct {
	Format
	DataOffset int64 // byte offset of the first sample
	DataSize   int64 // bytes of sample data
}

// Frames is the number of sample frames (one sample per channel) in the data.
func (h Header) Frames() int64 {
	return h.DataSize / int64(h.Channels*h.BitsPerSample/8)
}

// ParseHeader reads RIFF chunks until the data chunk. It accepts 16-bit PCM
// with one or two channels, which is what the recorders upload.
func ParseHeader(r io.ReadSeeker) (Header, error) {
	var riff [12]byte
	if _, err := io.ReadFull(r, riff[:]); err != nil {
		return Header{}, fmt.Errorf("read riff header: %w", err)
	}
	if string(riff[0:4]) != "RIFF" || string(riff[8:12]) != "WAVE" {
		return Header{}, errors.New("not a RIFF/WAVE file")
	}
	var h Header
	var haveFmt bool
	offset := int64(12)
	for {
		var chunk [8]byte
		if _, err := io.ReadFull(r, chunk[:]); err != nil {
			return Header{}, fmt.Errorf("read chunk header: %w", err)
		}
		id := string(chunk[0:4])
		size := int64(binary.LittleEndian.Uint32(chunk[4:8]))
		offset += 8
		switch id {
		case "fmt ":
			if size < 16 {
				return Header{}, errors.New("fmt chunk too short")
			}
			buf := make([]byte, size)
			if _, err := io.ReadFull(r, buf); err != nil {
				return Header{}, fmt.Errorf("read fmt chunk: %w", err)
			}
			audioFormat := binary.LittleEndian.Uint16(buf[0:2])
			h.Channels = int(binary.LittleEndian.Uint16(buf[2:4]))
			h.SampleRate = int(binary.LittleEndian.Uint32(buf[4:8]))
			h.BitsPerSample = int(binary.LittleEndian.Uint16(buf[14:16]))
			if audioFormat != 1 || h.BitsPerSample != 16 || h.Channels < 1 || h.Channels > 2 || h.SampleRate <= 0 {
				return Header{}, fmt.Errorf("%w: format=%d channels=%d bits=%d rate=%d",
					ErrUnsupported, audioFormat, h.Channels, h.BitsPerSample, h.SampleRate)
			}
			haveFmt = true
		case "data":
			if !haveFmt {
				return Header{}, errors.New("data chunk before fmt chunk")
			}
			h.DataOffset = offset
			h.DataSize = size
			return h, nil
		default:
			if _, err := r.Seek(size+size%2, io.SeekCurrent); err != nil { // chunks are word-aligned
				return Header{}, fmt.Errorf("skip %q chunk: %w", id, err)
			}
		}
		offset += size + size%2
	}
}

// EncodePCM16 writes a canonical 44-byte-header WAV file.
func EncodePCM16(w io.Writer, f Format, samples []int16) error {
	dataSize := uint32(len(samples) * 2)
	blockAlign := uint16(f.Channels * 2)
	hdr := make([]byte, 44)
	copy(hdr[0:4], "RIFF")
	binary.LittleEndian.PutUint32(hdr[4:8], 36+dataSize)
	copy(hdr[8:12], "WAVE")
	copy(hdr[12:16], "fmt ")
	binary.LittleEndian.PutUint32(hdr[16:20], 16)
	binary.LittleEndian.PutUint16(hdr[20:22], 1)
	binary.LittleEndian.PutUint16(hdr[22:24], uint16(f.Channels))
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(f.SampleRate))
	binary.LittleEndian.PutUint32(hdr[28:32], uint32(f.SampleRate)*uint32(blockAlign))
	binary.LittleEndian.PutUint16(hdr[32:34], blockAlign)
	binary.LittleEndian.PutUint16(hdr[34:36], 16)
	copy(hdr[36:40], "data")
	binary.LittleEndian.PutUint32(hdr[40:44], dataSize)
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, samples)
}
