//go:build !cgo

package wiredtiger

type nocgoService struct{}

func newService() Service { return &nocgoService{} }

func (s *nocgoService) Open(home string, config string) error {
	return errNoCgo()
}

func (s *nocgoService) Close() error { return nil }

func (s *nocgoService) CreateTable(name string, config string) error {
	return errNoCgo()
}

func (s *nocgoService) TableExists(name string) (bool, error) {
	return false, errNoCgo()
}

func (s *nocgoService) PutString(table string, key string, value string) error { return errNoCgo() }
func (s *nocgoService) GetString(table string, key string) (string, bool, error) {
	return "", false, errNoCgo()
}
func (s *nocgoService) DeleteString(table string, key string) error { return errNoCgo() }
