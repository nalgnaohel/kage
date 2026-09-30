package bench_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"

	"github.com/nalgnaohel/kage/api/rawdata"
	"github.com/nalgnaohel/kage/storage"
)

type legacyFetchRecord struct {
	Offset    int64  `json:"offset"`
	Value     []byte `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type legacyFetchResponse struct {
	Records       []legacyFetchRecord `json:"records"`
	HighWatermark int64               `json:"high_watermark"`
}

var benchCases = []struct {
	name string
	recs int
	size int
}{
	{"Small_100Bx200recs", 200, 100},
	{"Large_64KBx4recs", 4, 64 * 1024},
	{"Huge_1MBx8recs", 8, 1024 * 1024},
}

func setupBenchSegment(b *testing.B, numRecords, size int) (*storage.Segment, []uint64) {
	b.Helper()

	seg, err := storage.NewSegment(b.TempDir(), 0, storage.SegmentConfig{
		MaxLogSize:         uint64(numRecords*(size+12)) + 4096,
		MaxIndexSize:       256 * 1024,
		IndexIntervalBytes: 4096,
	})
	if err != nil {
		b.Fatalf("NewSegment: %v", err)
	}

	value := bytes.Repeat([]byte("x"), size)
	offsets := make([]uint64, numRecords)
	for i := 0; i < numRecords; i++ {
		off, err := seg.Append(value)
		if err != nil {
			b.Fatalf("Append: %v", err)
		}
		offsets[i] = off
	}
	return seg, offsets
}

func setupBenchLoopback(b *testing.B) net.Conn {
	b.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}

	acceptedCh := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		acceptedCh <- conn
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatalf("dial: %v", err)
	}

	server := <-acceptedCh
	go io.Copy(io.Discard, server)

	b.Cleanup(func() {
		client.Close()
		server.Close()
		ln.Close()
	})

	return client
}

func BenchmarkFetch_LegacyDecode(b *testing.B) {
	for _, c := range benchCases {
		b.Run(c.name, func(b *testing.B) {
			seg, offsets := setupBenchSegment(b, c.recs, c.size)
			conn := setupBenchLoopback(b)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp := legacyFetchResponse{HighWatermark: int64(len(offsets))}
				for _, off := range offsets {
					data, err := seg.Read(off)
					if err != nil {
						b.Fatalf("Read: %v", err)
					}
					valCopy := make([]byte, len(data))
					copy(valCopy, data)
					resp.Records = append(resp.Records, legacyFetchRecord{Offset: int64(off), Value: valCopy})
				}

				payload, err := json.Marshal(resp)
				if err != nil {
					b.Fatalf("Marshal: %v", err)
				}

				var lenBuf [4]byte
				binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
				if _, err := conn.Write(lenBuf[:]); err != nil {
					b.Fatalf("write length: %v", err)
				}
				if _, err := conn.Write(payload); err != nil {
					b.Fatalf("write payload: %v", err)
				}
			}
		})
	}
}

func BenchmarkFetch_ZeroCopy(b *testing.B) {
	for _, c := range benchCases {
		b.Run(c.name, func(b *testing.B) {
			seg, offsets := setupBenchSegment(b, c.recs, c.size)
			rng, err := seg.LocateRange(offsets[0], 1<<30)
			if err != nil {
				b.Fatalf("LocateRange: %v", err)
			}
			conn := setupBenchLoopback(b)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, err := seg.OpenReader()
				if err != nil {
					b.Fatalf("OpenReader: %v", err)
				}
				if _, err := f.Seek(rng.Pos, io.SeekStart); err != nil {
					b.Fatalf("Seek: %v", err)
				}
				if err := rawdata.EncodeFetchResponseHeader(conn, uint32(i), rawdata.ErrNone, uint64(len(offsets)), rng.NextOffset, uint32(rng.Length)); err != nil {
					b.Fatalf("EncodeFetchResponseHeader: %v", err)
				}
				if _, err := io.CopyN(conn, f, rng.Length); err != nil {
					b.Fatalf("CopyN: %v", err)
				}
				f.Close()
			}
		})
	}
}
