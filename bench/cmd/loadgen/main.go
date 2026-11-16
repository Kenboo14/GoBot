// Command loadgen starts N MTProto sessions against a fakedc instance and
// measures per-session RAM, goroutines and CPU (idle and under update load).
//
//	go run ./bench/cmd/loadgen -profile gotd-ubot -n 1000
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/Kenboo14/GoBot/bench/internal/metrics"
	"github.com/Kenboo14/GoBot/bench/internal/profiles"
	"github.com/Kenboo14/GoBot/internal/fakedc"
)

type phase struct {
	Start, End metrics.Snapshot
}

type report struct {
	Profile     string             `json:"profile"`
	Sessions    int                `json:"sessions"`
	Ready       int64              `json:"ready"`
	Failed      int64              `json:"failed"`
	GoVersion   string             `json:"go_version"`
	GOMAXPROCS  int                `json:"gomaxprocs"`
	ConnectTime time.Duration      `json:"connect_time"`
	Baseline    metrics.Snapshot   `json:"baseline"`
	Settled     metrics.Snapshot   `json:"settled"`
	Idle        phase              `json:"idle"`
	Load        phase              `json:"load"`
	LoadRate    float64            `json:"load_rate_per_session"`
	LoadUpdates uint64             `json:"load_updates_received"`
	LoadPeak    metrics.Snapshot   `json:"load_peak"`
	AfterStop   metrics.Snapshot   `json:"after_stop"`
	Creators    []creator          `json:"goroutine_creators"`
	Derived     map[string]float64 `json:"derived"`
}

type creator struct {
	Func       string  `json:"func"`
	Count      int     `json:"count"`
	PerSession float64 `json:"per_session"`
}

func main() {
	var (
		infoPath = flag.String("info", filepath.Join("bench", "out", "dc.json"), "fakedc info file")
		profName = flag.String("profile", "gotd-ubot", "client profile: "+strings.Join(profiles.Names(), ", "))
		n        = flag.Int("n", 100, "number of sessions")
		ramp     = flag.Int("ramp", 32, "max concurrent connecting sessions")
		settle   = flag.Duration("settle", 15*time.Second, "wait after all sessions are ready")
		idle     = flag.Duration("idle", 30*time.Second, "idle measurement window")
		load     = flag.Duration("load", 30*time.Second, "load measurement window (0 = skip)")
		rate     = flag.Float64("rate", 1, "updates per session per second during load")
		outDir   = flag.String("out", filepath.Join("bench", "out"), "output directory")
		timeout  = flag.Duration("connect-timeout", 15*time.Minute, "max time to connect all sessions")
	)
	flag.Parse()

	prof, err := profiles.Get(*profName)
	if err != nil {
		log.Fatal(err)
	}
	info, err := fakedc.ReadInfo(*infoPath)
	if err != nil {
		log.Fatalf("read %s (is fakedc running?): %v", *infoPath, err)
	}
	pk, err := info.PublicKey()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	if err := control(info.Control, "/reset"); err != nil {
		log.Printf("warning: reset fakedc: %v", err)
	}

	env := &profiles.Env{
		APIID:     1,
		APIHash:   "bench",
		PublicKey: pk,
		DCList: dcs.List{Options: []tg.DCOption{{
			ID: info.DC, IPAddress: info.Addr, Port: info.Port, Static: true,
		}}},
		Resolver: dcs.Plain(dcs.PlainOptions{Protocol: transport.Intermediate}),
		Updates:  new(atomic.Uint64),
	}
	sess := prof.Setup(env)

	r := report{Profile: prof.Name, Sessions: *n, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0), LoadRate: *rate}
	r.Baseline = metrics.Take(true)
	logf("profile=%s n=%d baseline goroutines=%d heap=%s", prof.Name, *n, r.Baseline.Goroutines, mb(r.Baseline.HeapInuse))

	ctx, cancel := context.WithCancel(context.Background())
	var (
		ready, failed atomic.Int64
		wg            sync.WaitGroup
		sem           = make(chan struct{}, *ramp)
		firstErr      atomic.Value
	)
	start := time.Now()
	go func() {
		for i := 0; i < *n; i++ {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				var once sync.Once
				release := func() { once.Do(func() { <-sem }) }
				err := sess(ctx, env, i, func() {
					ready.Add(1)
					release()
				})
				release()
				if err != nil && ctx.Err() == nil {
					failed.Add(1)
					firstErr.CompareAndSwap(nil, err.Error())
				}
			}(i)
		}
	}()

	deadline := time.After(*timeout)
	tick := time.NewTicker(5 * time.Second)
wait:
	for {
		if ready.Load()+failed.Load() >= int64(*n) {
			break
		}
		select {
		case <-deadline:
			logf("connect timeout")
			break wait
		case <-tick.C:
			logf("connecting: ready=%d failed=%d goroutines=%d", ready.Load(), failed.Load(), runtime.NumGoroutine())
		case <-time.After(200 * time.Millisecond):
		}
	}
	tick.Stop()
	r.ConnectTime = time.Since(start)
	r.Ready, r.Failed = ready.Load(), failed.Load()
	logf("connected: ready=%d failed=%d in %s", r.Ready, r.Failed, r.ConnectTime.Round(time.Millisecond))
	if e := firstErr.Load(); e != nil {
		logf("first session error: %s", e)
	}
	if r.Ready == 0 {
		cancel()
		log.Fatal("no sessions became ready")
	}

	time.Sleep(*settle)
	r.Settled = metrics.Take(true)
	r.Creators = goroutineCreators(int(r.Ready))
	writeProfiles(*outDir, prof.Name, *n)

	logf("idle window %s", *idle)
	r.Idle.Start = metrics.Take(false)
	time.Sleep(*idle)
	r.Idle.End = metrics.Take(false)

	if *load > 0 {
		logf("load window %s at %.2f upd/session/s", *load, *rate)
		u0 := env.Updates.Load()
		if err := control(info.Control, fmt.Sprintf("/load?rate=%g", *rate)); err != nil {
			logf("control: %v", err)
		}
		r.Load.Start = metrics.Take(false)
		peak := r.Load.Start
		end := time.Now().Add(*load)
		for time.Now().Before(end) {
			time.Sleep(time.Second)
			s := metrics.Take(false)
			if s.HeapInuse > peak.HeapInuse {
				peak.HeapInuse = s.HeapInuse
			}
			if s.Goroutines > peak.Goroutines {
				peak.Goroutines = s.Goroutines
			}
			if s.RSS > peak.RSS {
				peak.RSS = s.RSS
			}
		}
		r.Load.End = metrics.Take(false)
		r.LoadPeak = peak
		r.LoadUpdates = env.Updates.Load() - u0
		_ = control(info.Control, "/load?rate=0")
	}

	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		logf("WARNING: sessions did not stop within 30s")
	}
	time.Sleep(2 * time.Second)
	r.AfterStop = metrics.Take(true)

	r.Derived = derive(r)
	printReport(r)
	path := filepath.Join(*outDir, fmt.Sprintf("%s-n%d.json", prof.Name, *n))
	b, _ := json.MarshalIndent(r, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Fatal(err)
	}
	logf("report written to %s", path)
}

func derive(r report) map[string]float64 {
	n := float64(r.Ready)
	d := map[string]float64{}
	d["goroutines_per_session"] = float64(r.Settled.Goroutines-r.Baseline.Goroutines) / n
	d["heap_inuse_kib_per_session"] = float64(int64(r.Settled.HeapInuse)-int64(r.Baseline.HeapInuse)) / n / 1024
	d["heap_alloc_kib_per_session"] = float64(int64(r.Settled.HeapAlloc)-int64(r.Baseline.HeapAlloc)) / n / 1024
	d["heap_objects_per_session"] = float64(int64(r.Settled.HeapObjs)-int64(r.Baseline.HeapObjs)) / n
	d["stack_kib_per_session"] = float64(int64(r.Settled.StackInuse)-int64(r.Baseline.StackInuse)) / n / 1024
	d["rss_kib_per_session"] = float64(int64(r.Settled.RSS)-int64(r.Baseline.RSS)) / n / 1024
	d["idle_cpu_cores"] = metrics.CPUCores(r.Idle.Start, r.Idle.End)
	d["idle_cpu_cores_per_1000_sessions"] = d["idle_cpu_cores"] / n * 1000
	d["idle_gc_per_min"] = float64(r.Idle.End.NumGC-r.Idle.Start.NumGC) / r.Idle.End.At.Sub(r.Idle.Start.At).Minutes()
	if r.LoadUpdates > 0 {
		wall := r.Load.End.At.Sub(r.Load.Start.At)
		cpu := r.Load.End.CPU - r.Load.Start.CPU
		d["load_cpu_cores"] = metrics.CPUCores(r.Load.Start, r.Load.End)
		d["load_updates_per_sec"] = float64(r.LoadUpdates) / wall.Seconds()
		d["load_cpu_us_per_update"] = float64(cpu.Microseconds()) / float64(r.LoadUpdates)
		d["load_peak_heap_mib"] = float64(r.LoadPeak.HeapInuse) / (1 << 20)
		d["load_peak_goroutines"] = float64(r.LoadPeak.Goroutines)
		d["load_gc_per_min"] = float64(r.Load.End.NumGC-r.Load.Start.NumGC) / wall.Minutes()
	}
	d["leaked_goroutines_after_stop"] = float64(r.AfterStop.Goroutines - r.Baseline.Goroutines)
	return d
}

func printReport(r report) {
	fmt.Println()
	fmt.Printf("=== %s | sessions ready=%d failed=%d | %s GOMAXPROCS=%d ===\n", r.Profile, r.Ready, r.Failed, r.GoVersion, r.GOMAXPROCS)
	fmt.Printf("connect time            %s\n", r.ConnectTime.Round(time.Millisecond))
	fmt.Printf("settled total           goroutines=%d heap_inuse=%s stack=%s rss=%s sys=%s\n",
		r.Settled.Goroutines, mb(r.Settled.HeapInuse), mb(r.Settled.StackInuse), mb(r.Settled.RSS), mb(r.Settled.Sys))
	keys := make([]string, 0, len(r.Derived))
	for k := range r.Derived {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%-36s %.2f\n", k, r.Derived[k])
	}
	fmt.Println("top goroutine creators (per session):")
	for i, c := range r.Creators {
		if i >= 15 {
			break
		}
		fmt.Printf("  %6.2f  %6d  %s\n", c.PerSession, c.Count, c.Func)
	}
}

// goroutineCreators groups live goroutines by the first non-stdlib frame on
// their stack (e.g. "mtproto.(*Conn).pingLoop"), falling back to the
// "created by" function. This attributes errgroup-spawned loops correctly.
func goroutineCreators(n int) []creator {
	buf := make([]byte, 64<<20)
	buf = buf[:runtime.Stack(buf, true)]
	counts := map[string]int{}
	for _, block := range strings.Split(string(buf), "\n\n") {
		lines := strings.Split(block, "\n")
		key := ""
		for _, l := range lines[1:] {
			if strings.HasPrefix(l, "\t") || l == "" {
				continue
			}
			if strings.HasPrefix(l, "created by ") {
				if key == "" {
					key = "created by " + strings.TrimPrefix(l, "created by ")
				}
				break
			}
			fn := l
			if i := strings.LastIndex(fn, "("); i > 0 && strings.HasSuffix(fn, ")") {
				fn = fn[:i]
			}
			if isUserFrame(fn) && !strings.Contains(fn, "golang.org/x/sync/errgroup") {
				key = fn
				break
			}
		}
		if i := strings.Index(key, " in goroutine"); i >= 0 {
			key = key[:i]
		}
		if key != "" {
			counts[key]++
		}
	}
	out := make([]creator, 0, len(counts))
	for fn, c := range counts {
		out = append(out, creator{Func: fn, Count: c, PerSession: float64(c) / float64(n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out
}

func isUserFrame(fn string) bool {
	if strings.HasPrefix(fn, "main.") {
		return true
	}
	first := fn
	if i := strings.Index(fn, "/"); i > 0 {
		first = fn[:i]
	} else {
		return false
	}
	return strings.Contains(first, ".")
}

func writeProfiles(dir, name string, n int) {
	for _, p := range []string{"goroutine", "heap"} {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s-n%d.%s.pprof", name, n, p)))
		if err != nil {
			logf("pprof %s: %v", p, err)
			continue
		}
		_ = pprof.Lookup(p).WriteTo(f, 0)
		_ = f.Close()
	}
}

func control(addr, path string) error {
	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := c.Get("http://" + addr + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

func mb(v uint64) string { return fmt.Sprintf("%.1fMiB", float64(v)/(1<<20)) }

func logf(format string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}
