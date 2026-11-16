// Package metrics collects low-overhead process metrics for benchmarks.
package metrics

import (
	"runtime"
	"time"
)

// Snapshot is a point-in-time view of process resource usage.
type Snapshot struct {
	At         time.Time     `json:"at"`
	Goroutines int           `json:"goroutines"`
	HeapAlloc  uint64        `json:"heap_alloc"`
	HeapInuse  uint64        `json:"heap_inuse"`
	HeapObjs   uint64        `json:"heap_objects"`
	StackInuse uint64        `json:"stack_inuse"`
	Sys        uint64        `json:"sys"`
	RSS        uint64        `json:"rss"`
	NumGC      uint32        `json:"num_gc"`
	PauseTotal time.Duration `json:"gc_pause_total"`
	CPU        time.Duration `json:"cpu"` // user+kernel process CPU time
}

// Take returns a snapshot. If gc is true, a full GC runs first so heap
// numbers reflect live (retained) memory rather than garbage.
func Take(gc bool) Snapshot {
	if gc {
		runtime.GC()
		runtime.GC()
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Snapshot{
		At:         time.Now(),
		Goroutines: runtime.NumGoroutine(),
		HeapAlloc:  ms.HeapAlloc,
		HeapInuse:  ms.HeapInuse,
		HeapObjs:   ms.HeapObjects,
		StackInuse: ms.StackInuse,
		Sys:        ms.Sys,
		RSS:        rss(),
		NumGC:      ms.NumGC,
		PauseTotal: time.Duration(ms.PauseTotalNs),
		CPU:        cpuTime(),
	}
}

// CPUCores returns average number of CPU cores used between a and b.
func CPUCores(a, b Snapshot) float64 {
	wall := b.At.Sub(a.At)
	if wall <= 0 {
		return 0
	}
	return float64(b.CPU-a.CPU) / float64(wall)
}
