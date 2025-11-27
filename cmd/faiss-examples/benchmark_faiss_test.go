package main

import (
	"achillesdb/pkgs/faiss"
	"math/rand"
	"testing"
	"time"
)

func benchFaissFlatIVF(b *testing.B, desc string) {
	service := faiss.FAISS()
	dim := 512
	nb := 3000
	nq := 1
	k := 5
	xb := make([]float32, nb*dim)
	xq := make([]float32, nq*dim)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			xb[dim*i+j] = rand.Float32()
		}
		xb[dim*i] += float32(i) / 1000.0
	}
	for i := 0; i < nq; i++ {
		for j := 0; j < dim; j++ {
			xq[dim*i+j] = rand.Float32()
		}
		xq[dim*i] += float32(i) / 1000.0
	}
	xb = service.NormalizeBatch(xb, dim)
	xq = service.NormalizeBatch(xq, dim)
	idx, err := service.IndexFactory(dim, desc, faiss.MetricL2)
	if err != nil {
		b.Fatalf("IndexFactory failed: %v", err)
	}
	b.ResetTimer()
	start := time.Now()
	if desc[:3] == "IVF" {
		if tr, ok := interface{}(idx).(interface {
			Train(x []float32, n int) error
		}); ok {
			if err := tr.Train(xb[:1000*dim], 1000); err != nil {
				b.Fatalf("Index train failed: %v", err)
			}
		}
	}
	b.Logf("Train took %.3fms", float64(time.Since(start).Milliseconds()))
	start = time.Now()
	if err := idx.Add(xb, nb); err != nil {
		b.Fatalf("Add failed: %v", err)
	}
	b.Logf("Add took %.3fms", float64(time.Since(start).Milliseconds()))
	start = time.Now()
	D, I, err := idx.Search(xq, nq, k)
	if err != nil {
		b.Fatalf("Search failed: %v", err)
	}
	b.Logf("Search took %.3fms", float64(time.Since(start).Milliseconds()))
	_ = D
	_ = I
}

func BenchmarkFlat(b *testing.B) { benchFaissFlatIVF(b, "Flat") }
func BenchmarkIVF(b *testing.B)  { benchFaissFlatIVF(b, "IVF256,Flat") }
