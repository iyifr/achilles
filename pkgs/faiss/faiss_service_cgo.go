//go:build cgo

package faiss

/*
#cgo CFLAGS: -I/usr/local/include
#cgo darwin CFLAGS: -I/usr/local/include/
#cgo darwin LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lfaiss_c
#cgo linux CFLAGS: -I/usr/local/include/
#cgo linux LDFLAGS: -L/usr/local/lib -Wl,-rpath,/usr/local/lib -lfaiss_c
#include <stdlib.h>
#include <string.h>
#include <faiss/c_api/Index_c.h>
#include <faiss/c_api/index_factory_c.h>
#include <faiss/c_api/index_io_c.h>
#include <faiss/c_api/error_c.h>
#include <faiss/c_api/utils/utils_c.h>
#include <faiss/c_api/utils/distances_c.h>
// Add C declaration:
// int faiss_Index_train(FaissIndex*, idx_t n, const float *x);
// FaissIndex *faiss_read_index_fname(const char* fname, int io_flags);

// Provide a local version string to avoid relying on optional API symbols
static const char* gs_faiss_version_str() { return "faiss-c-api"; }

// helpers to adapt types across cgo boundary (kept for future use)
static inline int metric_to_c(int m) { return m; }
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"
)

type cgoService struct{}

func FAISSServiceImpl() FAISSService { return &cgoService{} }

func (s *cgoService) GetVersion() (string, error) {
	v := C.faiss_get_version()
	if v == nil {
		return "", fmt.Errorf("failed to get FAISS version")
	}
	return C.GoString(v), nil
}

type indexImpl struct{ ptr *C.FaissIndex }

func (s *cgoService) IndexFactory(dimension int, description string, metric MetricType) (*Index, error) {
	cdesc := C.CString(description)
	defer C.free(unsafe.Pointer(cdesc))
	var idx *C.FaissIndex
	if rc := C.faiss_index_factory(&idx, C.int(dimension), cdesc, C.FaissMetricType(metric)); rc != 0 {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return nil, fmt.Errorf("faiss_index_factory: %s", C.GoString(perr))
		}
		return nil, fmt.Errorf("faiss_index_factory failed: %d", int(rc))
	}
	return &Index{_impl: &indexImpl{ptr: idx}}, nil
}

func indexIsTrained(idx *Index) (bool, error) {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return false, fmt.Errorf("nil index")
	}
	trained := C.faiss_Index_is_trained(impl.ptr)
	return trained != 0, nil
}

func indexAdd(idx *Index, xb []float32, nb int) error {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return fmt.Errorf("nil index")
	}
	if nb <= 0 {
		return nil
	}
	if len(xb) == 0 {
		return fmt.Errorf("xb empty")
	}
	if rc := C.faiss_Index_add(impl.ptr, C.idx_t(nb), (*C.float)(&xb[0])); rc != 0 {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return fmt.Errorf("%s", C.GoString(perr))
		}
		return fmt.Errorf("faiss_Index_add rc=%d", int(rc))
	}
	return nil
}

func indexNTotal(idx *Index) (int64, error) {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return 0, fmt.Errorf("nil index")
	}
	n := C.faiss_Index_ntotal(impl.ptr)
	return int64(n), nil
}

func indexSearch(idx *Index, xq []float32, nq int, k int) ([]float32, []int64, error) {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return nil, nil, fmt.Errorf("nil index")
	}
	if nq <= 0 || k <= 0 {
		return []float32{}, []int64{}, nil
	}
	dists := make([]float32, nq*k)
	ids := make([]int64, nq*k)
	rc := C.faiss_Index_search(impl.ptr, C.idx_t(nq), (*C.float)(&xq[0]), C.idx_t(k), (*C.float)(&dists[0]), (*C.idx_t)(&ids[0]))
	if rc != 0 {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return nil, nil, fmt.Errorf("%s", C.GoString(perr))
		}
		return nil, nil, fmt.Errorf("faiss_Index_search rc=%d", int(rc))
	}
	return dists, ids, nil
}

func indexWriteToFile(idx *Index, path string) error {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return fmt.Errorf("nil index")
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	if rc := C.faiss_write_index_fname(impl.ptr, cpath); rc != 0 {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return fmt.Errorf("%s", C.GoString(perr))
		}
		return fmt.Errorf("faiss_write_index_fname rc=%d", int(rc))
	}
	return nil
}

func indexFree(idx *Index) {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return
	}
	C.faiss_Index_free(impl.ptr)
	impl.ptr = nil
}

func trainIndex(idx *Index, x []float32, n int) error {
	impl, ok := idx._impl.(*indexImpl)
	if !ok || impl.ptr == nil {
		return fmt.Errorf("nil index")
	}
	if n <= 0 || len(x) < n {
		return fmt.Errorf("invalid train args")
	}
	// Pass n as C.idx_t (int64); x as *C.float
	rc := C.faiss_Index_train(impl.ptr, C.idx_t(n), (*C.float)(&x[0]))
	if rc != 0 {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return fmt.Errorf("%s", C.GoString(perr))
		}
		return fmt.Errorf("faiss_Index_train rc=%d", int(rc))
	}
	return nil
}

// L2 square norm of one vector
func (c *cgoService) L2NormSqr(x []float32) float32 {
	if len(x) == 0 {
		return 0
	}
	return float32(C.faiss_fvec_norm_L2sqr((*C.float)(&x[0]), C.size_t(len(x))))
}

// Compute L2 norms for nx vectors of dimension d
func (c *cgoService) L2Norms(norms, x []float32, d, nx int) {
	if len(x) < d*nx || len(norms) < nx {
		return
	}
	C.faiss_fvec_norms_L2((*C.float)(&norms[0]), (*C.float)(&x[0]), C.size_t(d), C.size_t(nx))
}

// Compute L2^2 norms for nx vectors of dimension d
func (c *cgoService) L2NormsSqr(norms, x []float32, d, nx int) {
	if len(x) < d*nx || len(norms) < nx {
		return
	}
	C.faiss_fvec_norms_L2sqr((*C.float)(&norms[0]), (*C.float)(&x[0]), C.size_t(d), C.size_t(nx))
}

// Normalize one vector in-place. Returns its original norm
func (c *cgoService) Normalize(x []float32) float32 {
	d := len(x)
	if d == 0 {
		return 0
	}
	norm2 := float32(C.faiss_fvec_norm_L2sqr((*C.float)(&x[0]), C.size_t(d)))
	norm := float32(math.Sqrt(float64(norm2)))
	if norm > 0 {
		for i := 0; i < d; i++ {
			x[i] /= norm
		}
	}
	return norm
}

// Normalize all vectors in x (length must be a multiple of d), returns a new slice
func (c *cgoService) NormalizeBatch(x []float32, d int) []float32 {
	nx := len(x) / d
	if nx == 0 || d == 0 {
		return nil
	}
	result := make([]float32, len(x))
	copy(result, x)
	norms := make([]float32, nx)
	C.faiss_fvec_norms_L2((*C.float)(&norms[0]), (*C.float)(&result[0]), C.size_t(d), C.size_t(nx))
	for i := 0; i < nx; i++ {
		begin := i * d
		n := norms[i]
		if n > 0 {
			for j := 0; j < d; j++ {
				result[begin+j] /= n
			}
		}
	}
	return result
}

func (c *cgoService) Train(idx *Index, x []float32, n int) error {
	return trainIndex(idx, x, n)
}

// TODO: Cache hot indexes and overwrite cache during writes.
func (c *cgoService) ReadIndex(path string) (*Index, error) {
	fname := C.CString(path)
	defer C.free(unsafe.Pointer(fname))
	var idx *C.FaissIndex = nil
	rc := C.faiss_read_index_fname(fname, 0, &idx)
	if rc != 0 || idx == nil {
		perr := C.faiss_get_last_error()
		if perr != nil {
			return nil, fmt.Errorf("faiss_read_index_fname: %s", C.GoString(perr))
		}
		return nil, fmt.Errorf("faiss_read_index_fname failed for %s", path)
	}
	return &Index{_impl: &indexImpl{ptr: idx}}, nil
}
