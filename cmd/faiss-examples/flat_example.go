package main

import (
	"achillesdb/pkgs/faiss"
	"fmt"
	"math/rand"
	"os"
)

func FlatIndex() {
	service := faiss.FAISS()
	version, err := service.GetVersion()
	if err != nil {
		fmt.Printf("Error getting FAISS version: %v\n", err)
		return
	}
	fmt.Printf("FAISS version: %s\n", version)
	dim := 1536
	nb := 5000
	nq := 1
	k := 5

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

	fmt.Println("Creating Flat index (L2)...")
	idx, err := service.IndexFactory(dim, "Flat", faiss.MetricL2)
	if err != nil {
		fmt.Printf("IndexFactory failed: %v\n", err)
		return
	}
	defer idx.Free()

	trained, err := idx.IsTrained()
	if err != nil {
		fmt.Println("Error: ", err)
	}
	fmt.Printf("is_trained = %v\n", trained)

	fmt.Println("Adding database vectors...")
	if err := idx.Add(xb, nb); err != nil {
		fmt.Printf("Index add failed: %v\n", err)
		return
	}
	tl, _ := idx.NTotal()
	fmt.Printf("ntotal = %d\n", tl)

	fmt.Println("Searching...")
	D, I, err := idx.Search(xb[:5*dim], 5, k)
	if err != nil {
		fmt.Printf("Index-search (sanity) failed: %v\n", err)
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
		fmt.Printf("Index-search (xq) failed: %v\n", err)
		return
	}
	fmt.Println("Query search (first result):")
	for i := 0; i < nq; i++ {
		for j := 0; j < k; j++ {
			fmt.Printf("%5d (d=%.4f)  ", I[i*k+j], D[i*k+j])
		}
		fmt.Println()
	}
	fname := "flat.index"
	fmt.Printf("Writing index to disk as %s...\n", fname)
	if err := idx.WriteToFile(fname); err != nil {
		fmt.Printf("WriteToFile failed: %v\n", err)
		return
	}
	if st, err := os.Stat(fname); err == nil {
		fmt.Printf("Wrote %s (%.2f MB)\n", fname, float64(st.Size())/1024.0/1024.0)
	}
	fmt.Println("Done.")
}
