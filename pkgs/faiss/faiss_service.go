package faiss

// Service provides a minimal API for interacting with FAISS.
// This abstracts the underlying cgo implementation to allow testing and !cgo builds.
type FAISSService interface {
	GetVersion() (string, error)

	// IndexFactory creates an index with the given dimension, description (e.g. "Flat"),
	// and metric type (use MetricL2 or MetricInnerProduct).
	IndexFactory(dimension int, description string, metric MetricType) (*Index, error)

	// Norm/math utilities
	L2NormSqr(x []float32) float32
	L2Norms(norms, x []float32, d, nx int)
	L2NormsSqr(norms, x []float32, d, nx int)
	Normalize(x []float32) float32               // Normalizes in-place. Returns norm before normalization.
	NormalizeBatch(x []float32, d int) []float32 // Returns a normalized copy, for nx vectors
	Train(idx *Index, x []float32, n int) error

	// New: Read index from disk, or return error
	ReadIndex(path string) (*Index, error)
}

func FAISS() FAISSService {
	return FAISSServiceImpl()
}

// MetricType defines metric used by FAISS indexes.
type MetricType int

const (
	MetricL2           MetricType = 1 // METRIC_L2
	MetricInnerProduct MetricType = 2 // METRIC_INNER_PRODUCT
)

// Index wraps a FAISS index instance.
type Index struct {
	// opaque; implemented per build tag
	_impl any
}

// IsTrained reports whether the index is trained.
func (idx *Index) IsTrained() (bool, error) { return indexIsTrained(idx) }

// Add inserts nb vectors (xb length must be nb*dimension).
func (idx *Index) Add(xb []float32, nb int) error { return indexAdd(idx, xb, nb) }

// Index must be wrapped with IndexIDMap.
func (idx *Index) AddWithIds(xb []float32, xids []int64, nb int) error {
	return indexAddWithIds(idx, xb, xids, nb)
}

// RemoveIds removes every vector whose id is in ids
func (idx *Index) RemoveIds(ids []int64) (int, error) { return indexRemoveIds(idx, ids) }

// WrapIDMap wraps the index in an IndexIDMap, to support custom ID's
func (idx *Index) WrapIDMap() error { return wrapIDMap(idx) }

// NTotal returns the number of vectors in the index.
func (idx *Index) NTotal() (int64, error) { return indexNTotal(idx) }

// Search queries nq vectors in xq, returning top-k distances and ids.
func (idx *Index) Search(xq []float32, nq int, k int) (distances []float32, ids []int64, err error) {
	return indexSearch(idx, xq, nq, k)
}

// WriteToFile serializes the index to the given file path.
func (idx *Index) WriteToFile(path string) error { return indexWriteToFile(idx, path) }

// Free releases native resources. Safe to call multiple times.
func (idx *Index) Free() { indexFree(idx) }

// Train trains the index on the given vectors.
func (idx *Index) Train(x []float32, n int) error {
	return trainIndex(idx, x, n)
}
