package logic

import (
	"github.com/panjf2000/ants/v2"
	"runtime"
)

func NewPoolWithCores(cores int) (_ *ants.Pool, err error) {
	numCores := cores
	if numCores < 0 {
		numCores = runtime.NumCPU()
	}
	return ants.NewPool(cores)
}
