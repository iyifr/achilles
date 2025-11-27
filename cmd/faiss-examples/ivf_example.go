package main

import (
	"achillesdb/pkgs/faiss"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func IvfIndexExample() {
	service := faiss.FAISS()

	dim := 1536
	nb := 5000
	nq := 1
	k := 5
	indexName := "ivf.index"
	indexDesc := "IVF36,Flat"

	fmt.Printf("Generating %d x %d database vectors...\n", nb, dim)
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
	fmt.Println("Normalizing database vectors...")
	xb = service.NormalizeBatch(xb, dim)
	fmt.Println("Normalizing query vectors...")
	xq = service.NormalizeBatch(xq, dim)

	fmt.Println("First 3 vectors in xb:")
	for i := 0; i < 3 && i < nb; i++ {
		fmt.Printf("xb[%d]: [", i)
		for j := 0; j < dim; j++ {
			fmt.Printf("%.3f", xb[i*dim+j])
			if j < dim-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("]")
	}
	fmt.Println("First 3 vectors in xq:")
	for i := 0; i < 3 && i < nq; i++ {
		fmt.Printf("xq[%d]: [", i)
		for j := 0; j < dim; j++ {
			fmt.Printf("%.3f", xq[i*dim+j])
			if j < dim-1 {
				fmt.Print(", ")
			}
		}
		fmt.Println("]")
	}

	var idx *faiss.Index
	loaded := false
	if _, errStat := os.Stat(indexName); errStat == nil {
		idxLoaded, err := service.ReadIndex(indexName)
		if err != nil {
			fmt.Printf("Failed to read index from disk (%s): %v\n", indexName, err)
		} else if idxLoaded != nil {
			idx = idxLoaded
			loaded = true
			fmt.Printf("Loaded IVF index from disk: %s\n", indexName)
		}
	}
	if !loaded || idx == nil {
		fmt.Println("No existing IVF index found or failed to load; creating new index...")
		var err error
		idx, err = service.IndexFactory(dim, indexDesc, faiss.MetricL2)
		if err != nil {
			fmt.Printf("IndexFactory failed: %v\n", err)
			return
		}
		fmt.Println("Training the index...")
		start := time.Now()
		sub := 3000
		if sub > nb {
			sub = nb
		}
		if err := idx.Train(xb[:sub*dim], sub); err != nil {
			fmt.Printf("Index training failed: %v\n", err)
			return
		}
		fmt.Printf("Training took %.3fs\n", time.Since(start).Seconds())
		fmt.Println("Adding vectors to index...")
		start = time.Now()
		if err := idx.Add(xb, nb); err != nil {
			fmt.Printf("Index add failed: %v\n", err)
			return
		}
		fmt.Printf("Adding took %.3fs\n", time.Since(start).Seconds())
		if err := idx.WriteToFile(indexName); err != nil {
			fmt.Printf("WriteToFile failed: %v\n", err)
			return
		}
		if st, err := os.Stat(indexName); err == nil {
			fmt.Printf("Wrote %s (%.2f MB)\n", indexName, float64(st.Size())/1024.0/1024.0)
		}
		fmt.Println("IVF index built and saved!")
	}
	defer idx.Free()
	istrained, trainErr := idx.IsTrained()
	if trainErr != nil {
		fmt.Printf("IsTrained error: %v\n", trainErr)
	}
	fmt.Printf("is_trained = %v\n", istrained)
	fmt.Printf("ntotal = %d\n", func() int64 { n, _ := idx.NTotal(); return n }())
	fmt.Println("Searching...")
	D, I, err := idx.Search(xb[:5*dim], 5, k)
	if err != nil {
		fmt.Printf("Index search (sanity) failed: %v\n", err)
		return
	}
	fmt.Println("Sanity check (1st 5 db vectors):")
	for i := 0; i < 5; i++ {
		for j := 0; j < k; j++ {
			fmt.Printf("%5d (d=%.4f)  ", I[i*k+j], D[i*k+j])
		}
		fmt.Println()
	}
	D, I, err = idx.Search(xq, nq, k)
	if err != nil {
		fmt.Printf("Index search (xq) failed: %v\n", err)
		return
	}
	fmt.Println("Query search (first result):")
	for i := 0; i < nq; i++ {
		for j := 0; j < k; j++ {
			fmt.Printf("%5d (d=%.4f)  ", I[i*k+j], D[i*k+j])
		}
		fmt.Println()
	}
	fmt.Println("Done.")
}
