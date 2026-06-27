//go:build !cgo

package faiss

type nocgoService struct{}

func FAISSServiceImpl() FAISSService { return &nocgoService{} }

func (s *nocgoService) GetVersion() (string, error) {
	return "", errNoCgo()
}

func (s *nocgoService) IndexFactory(dimension int, description string, metric MetricType) (*Index, error) {
	return nil, errNoCgo()
}

func indexIsTrained(idx *Index) (bool, error)         { return false, errNoCgo() }
func indexAdd(idx *Index, xb []float32, nb int) error { return errNoCgo() }
func indexAddWithIds(idx *Index, xb []float32, xids []int64, nb int) error {
	return errNoCgo()
}
func indexRemoveIds(idx *Index, ids []int64) (int, error) { return 0, errNoCgo() }
func wrapIDMap(idx *Index) error                          { return errNoCgo() }
func indexNTotal(idx *Index) (int64, error)               { return 0, errNoCgo() }
func indexSearch(idx *Index, xq []float32, nq int, k int) ([]float32, []int64, error) {
	return nil, nil, errNoCgo()
}
func indexWriteToFile(idx *Index, path string) error { return errNoCgo() }
func indexFree(idx *Index)                           {}

func trainIndex(idx *Index, x []float32, n int) error {
	return errNoCgo()
}

func (n *nocgoService) L2NormSqr(x []float32) float32 {
	return genericL2NormSqr(x)
}

func (n *nocgoService) L2Norms(norms, x []float32, d, nx int) {
	for i := 0; i < nx; i++ {
		offset := i * d
		norm := float32(0)
		for j := 0; j < d; j++ {
			f := x[offset+j]
			norm += f * f
		}
		norms[i] = float32(Sqrt64(float64(norm)))
	}
}

func (n *nocgoService) L2NormsSqr(norms, x []float32, d, nx int) {
	for i := 0; i < nx; i++ {
		offset := i * d
		norm := float32(0)
		for j := 0; j < d; j++ {
			f := x[offset+j]
			norm += f * f
		}
		norms[i] = norm
	}
}

func (n *nocgoService) Normalize(x []float32) float32 {
	return genericNormalize(x)
}

func (n *nocgoService) NormalizeBatch(x []float32, d int) []float32 {
	return genericNormalizeBatch(x, d)
}

func (n *nocgoService) ReadIndex(path string) (*Index, error) {
	return nil, errNoCgo()
}
