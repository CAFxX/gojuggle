package measurers

import "syscall"

type DiskSpaceMeasurer struct {
	path string
}

func NewDiskSpaceMeasurer(path string) *DiskSpaceMeasurer {
	return &DiskSpaceMeasurer{path: path}
}

func (m *DiskSpaceMeasurer) Measure() (float64, error) {
	var res syscall.Statfs_t
	if err := syscall.Statfs(m.path, &res); err != nil {
		return 0, err
	}
	return float64(res.Blocks-res.Bavail) * float64(res.Bsize), nil
}
