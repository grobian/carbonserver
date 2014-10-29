package main

import (
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"code.google.com/p/gogoprotobuf/proto"
	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
	"github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	whisper "github.com/grobian/go-whisper"
	pickle "github.com/kisielk/og-rek"
	"github.com/lestrrat/go-file-rotatelogs"
	g2g "github.com/peterbourgon/g2g"
)

var config = struct {
	WhisperData  string
	GraphiteHost string
	MaxGlobs     int
	Buckets      int
}{
	WhisperData: "/var/lib/carbon/whisper",
	MaxGlobs:    10,
	Buckets:     10,
}

// grouped expvars for /debug/vars and graphite
var Metrics = struct {
	RenderRequests *expvar.Int
	RenderErrors   *expvar.Int
	NotFound       *expvar.Int
	FindRequests   *expvar.Int
	FindErrors     *expvar.Int
	FindZero       *expvar.Int
	InfoRequests   *expvar.Int
	InfoErrors     *expvar.Int
}{
	RenderRequests: expvar.NewInt("render_requests"),
	RenderErrors:   expvar.NewInt("render_errors"),
	NotFound:       expvar.NewInt("notfound"),
	FindRequests:   expvar.NewInt("find_requests"),
	FindErrors:     expvar.NewInt("find_errors"),
	FindZero:       expvar.NewInt("find_zero"),
	InfoRequests:   expvar.NewInt("info_requests"),
	InfoErrors:     expvar.NewInt("info_errors"),
}

var BuildVersion string = "(development build)"

var logger logLevel

type fileIndex struct {
	idx   trigram.Index
	files []string
}

var fileIdx unsafe.Pointer            // actual type is *fileIndex
func CurrentFileIndex() *fileIndex    { return (*fileIndex)(atomic.LoadPointer(&fileIdx)) }
func UpdateFileIndex(fidx *fileIndex) { atomic.StorePointer(&fileIdx, unsafe.Pointer(fidx)) }

func fileListUpdater(dir string, tick <-chan time.Time, force <-chan struct{}) {

	for {

		select {
		case <-tick:
		case <-force:
		}

		var files []string

		t0 := time.Now()

		err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Logf("error processing %q: %v\n", p, err)
				return nil
			}

			if info.IsDir() || strings.HasSuffix(info.Name(), ".wsp") {
				files = append(files, strings.TrimPrefix(p, config.WhisperData))
			}

			return nil
		})

		logger.Logln("file scan took", time.Since(t0), ",", len(files), "items")
		t0 = time.Now()

		idx := trigram.NewIndex(files)

		logger.Logln("indexing took took", time.Since(t0), len(idx), "trigrams")

		if err == nil {
			UpdateFileIndex(&fileIndex{idx, files})
		}
	}
}

func findHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob

	t0 := time.Now()

	Metrics.FindRequests.Add(1)

	req.ParseForm()
	format := req.FormValue("format")
	query := req.FormValue("query")

	if format != "json" && format != "pickle" && format != "protobuf" {
		Metrics.FindErrors.Add(1)
		logger.Logf("dropping invalid uri (format=%s): %s",
			format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if query == "" {
		Metrics.FindErrors.Add(1)
		logger.Logf("dropping invalid request (query=): %s", req.URL.RequestURI())
		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
		return
	}

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - carbon.[rz]    -> carbon.relays, carbon.zipper
	 * - carbon.{re,zi} -> carbon.relays, carbon.zipper
	 * - match is either dir or .wsp file
	 * unfortunately, filepath.Glob doesn't handle the curly brace
	 * expansion for us */

	query = strings.Replace(query, ".", "/", -1)

	var globs []string
	if !strings.HasSuffix(query, "*") {
		globs = append(globs, query+".wsp")
	}
	globs = append(globs, query)
	for {
		bracematch := false
		var newglobs []string
		for _, glob := range globs {
			lbrace := strings.Index(glob, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(glob[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				bracematch = true
				expansion := glob[lbrace+1 : rbrace]
				parts := strings.Split(expansion, ",")
				for _, sub := range parts {
					if len(newglobs) > config.MaxGlobs {
						break
					}
					newglobs = append(newglobs, glob[:lbrace]+sub+glob[rbrace+1:])
				}
			} else {
				if len(newglobs) > config.MaxGlobs {
					break
				}
				newglobs = append(newglobs, glob)
			}
		}
		globs = newglobs
		if !bracematch {
			break
		}
	}

	var files []string

	fidx := CurrentFileIndex()

	if fidx == nil {
		// no index -- hit the filesystem
		for _, g := range globs {
			nfiles, err := filepath.Glob(config.WhisperData + "/" + g)
			if err == nil {
				files = append(files, nfiles...)
			}
		}
	} else {
		// use the index
		docs := make(map[trigram.DocID]struct{})

		for _, g := range globs {

			gpath := "/" + g

			ts := extractTrigrams(g)

			ids := fidx.idx.QueryTrigrams(ts)

			for _, id := range ids {
				docid := trigram.DocID(id)
				if _, ok := docs[docid]; !ok {
					matched, err := filepath.Match(gpath, fidx.files[id])
					if err == nil && matched {
						docs[docid] = struct{}{}
					}
				}
			}
		}

		for id := range docs {
			files = append(files, config.WhisperData+fidx.files[id])
		}

		sort.Strings(files)
	}

	leafs := make([]bool, len(files))
	for i, p := range files {
		p = p[len(config.WhisperData+"/"):]
		if strings.HasSuffix(p, ".wsp") {
			p = p[:len(p)-4]
			leafs[i] = true
		} else {
			leafs[i] = false
		}
		files[i] = strings.Replace(p, "/", ".", -1)
	}

	if format == "json" || format == "protobuf" {
		name := req.FormValue("query")
		response := pb.GlobResponse{
			Name:    &name,
			Matches: make([]*pb.GlobMatch, 0),
		}

		for i, p := range files {
			response.Matches = append(response.Matches, &pb.GlobMatch{Path: proto.String(p), IsLeaf: proto.Bool(leafs[i])})
		}

		var b []byte
		var err error
		switch format {
		case "json":
			b, err = json.Marshal(response)
		case "protobuf":
			b, err = proto.Marshal(&response)
		}
		if err != nil {
			Metrics.FindErrors.Add(1)
			logger.Logf("failed to create %s data for glob %s: %s",
				format, *response.Name, err)
			return
		}
		wr.Write(b)
	} else if format == "pickle" {
		// [{'metric_path': 'metric', 'intervals': [(x,y)], 'isLeaf': True},]
		var metrics []map[string]interface{}
		var m map[string]interface{}

		for i, p := range files {
			m = make(map[string]interface{})
			m["metric_path"] = p
			// m["intervals"] = dunno how to do a tuple here
			m["isLeaf"] = leafs[i]
			metrics = append(metrics, m)
		}

		wr.Header().Set("Content-Type", "application/pickle")
		pEnc := pickle.NewEncoder(wr)
		pEnc.Encode(metrics)
	}

	if len(files) == 0 {
		// to get an idea how often we search for nothing
		Metrics.FindZero.Add(1)
	}

	logger.Debugf("find: %d hits for %s in %v", len(files), req.FormValue("query"), time.Since(t0))
	return
}

func fetchHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /render/?target=the.metric.name&format=pickle&from=1396008021&until=1396022421

	Metrics.RenderRequests.Add(1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	// Make sure we log which metric caused a panic()
	defer func() {
		if r := recover(); r != nil {
			var buf [1024]byte
			runtime.Stack(buf[:], false)
			logger.Logf("panic handling request: %s\n%s\n", req.RequestURI, string(buf[:]))
		}
	}()

	if format != "json" && format != "pickle" && format != "protobuf" {
		Metrics.RenderErrors.Add(1)
		logger.Logf("dropping invalid uri (format=%s): %s",
			format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		// the FE/carbonzipper often requests metrics we don't have
		Metrics.NotFound.Add(1)
		logger.Debugf("failed to %s", err)
		http.Error(wr, "Metric not found", http.StatusNotFound)
		return
	}

	i, err := strconv.Atoi(from)
	if err != nil {
		logger.Debugf("fromTime (%s) invalid: %s (in %s)",
			from, err, req.URL.RequestURI())
		if w != nil {
			w.Close()
		}
		w = nil
	}
	fromTime := int(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		logger.Debugf("untilTime (%s) invalid: %s (in %s)",
			from, err, req.URL.RequestURI())
		if w != nil {
			w.Close()
		}
		w = nil
	}
	untilTime := int(i)

	if w != nil {
		defer w.Close()
	} else {
		Metrics.RenderErrors.Add(1)
		http.Error(wr, "Bad request (invalid from/until time)",
			http.StatusBadRequest)
		return
	}

	points, err := w.Fetch(fromTime, untilTime)
	if err != nil {
		Metrics.RenderErrors.Add(1)
		logger.Logf("failed to fetch points from %s: %s", path, err)
		http.Error(wr, "Fetching data points failed",
			http.StatusInternalServerError)
		return
	}

	if points == nil {
		Metrics.NotFound.Add(1)
		logger.Debugf("Metric time range not found: metric=%s from=%d to=%d ", metric, fromTime, untilTime)
		http.Error(wr, "Metric time range not found", http.StatusNotFound)
		return
	}

	values := points.Values()

	if format == "json" || format == "protobuf" {
		fromTime := int32(points.FromTime())
		untilTime := int32(points.UntilTime())
		step := int32(points.Step())
		response := pb.FetchResponse{
			Name:      &metric,
			StartTime: &fromTime,
			StopTime:  &untilTime,
			StepTime:  &step,
			Values:    make([]float64, len(values)),
			IsAbsent:  make([]bool, len(values)),
		}

		for i, p := range values {
			if math.IsNaN(p) {
				response.Values[i] = 0
				response.IsAbsent[i] = true
			} else {
				response.Values[i] = p
				response.IsAbsent[i] = false
			}
		}

		var b []byte
		var err error
		switch format {
		case "json":
			b, err = json.Marshal(response)
		case "protobuf":
			b, err = proto.Marshal(&response)
		}
		if err != nil {
			Metrics.RenderErrors.Add(1)
			logger.Logf("failed to create %s data for %s: %s", format, path, err)
			return
		}
		wr.Write(b)
	} else if format == "pickle" {
		//[{'start': 1396271100, 'step': 60, 'name': 'metric',
		//'values': [9.0, 19.0, None], 'end': 1396273140}
		var metrics []map[string]interface{}
		var m map[string]interface{}

		m = make(map[string]interface{})
		m["start"] = points.FromTime()
		m["step"] = points.Step()
		m["end"] = points.UntilTime()
		m["name"] = metric

		mv := make([]interface{}, len(values))
		for i, p := range values {
			if math.IsNaN(p) {
				mv[i] = nil
			} else {
				mv[i] = p
			}
		}

		m["values"] = mv
		metrics = append(metrics, m)

		wr.Header().Set("Content-Type", "application/pickle")
		pEnc := pickle.NewEncoder(wr)
		pEnc.Encode(metrics)
	}

	logger.Debugf("served %d points for %s", len(values), metric)
	return
}

func infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.name&format=json

	Metrics.InfoRequests.Add(1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")

	if format != "json" {
		Metrics.InfoErrors.Add(1)
		logger.Logf("dropping invalid uri (format=%s): %s",
			format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		Metrics.NotFound.Add(1)
		logger.Debugf("failed to %s", err)
		http.Error(wr, "Metric not found", http.StatusNotFound)
		return
	}

	if format == "json" || format == "protobuf" {
		aggr := w.AggregationMethod()
		maxr := int32(w.MaxRetention())
		xfiles := float32(w.XFilesFactor())
		rets := make([]*pb.Retention, 0, 4)
		for _, retention := range w.Retentions() {
			spp := int32(retention.SecondsPerPoint())
			nop := int32(retention.NumberOfPoints())
			rets = append(rets, &pb.Retention{
				SecondsPerPoint: &spp,
				NumberOfPoints:  &nop,
			})
		}
		response := pb.InfoResponse{
			Name:              &metric,
			AggregationMethod: &aggr,
			MaxRetention:      &maxr,
			XFilesFactor:      &xfiles,
			Retentions:        rets,
		}

		var b []byte
		var err error
		switch format {
		case "json":
			b, err = json.Marshal(response)
		case "protobuf":
			b, err = proto.Marshal(&response)
		}
		if err != nil {
			Metrics.RenderErrors.Add(1)
			logger.Logf("failed to create %s data for %s: %s", format, path, err)
			return
		}
		wr.Write(b)
	}

	logger.Debugf("served info for %s", metric)
	return
}

func main() {
	port := flag.Int("p", 8080, "port to bind to")
	verbose := flag.Bool("v", false, "enable verbose logging")
	debug := flag.Bool("vv", false, "enable more verbose (debug) logging")
	whisperdata := flag.String("w", config.WhisperData, "location where whisper files are stored")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU()*80/100, "GOMAXPROCS")
	logdir := flag.String("logdir", "/var/log/carbonserver/", "logging directory")
	logtostdout := flag.Bool("stdout", false, "log also to stdout")
	scanFrequency := flag.Duration("scanfreq", 0, "file index scan frequency (0 to disable file index)")

	flag.Parse()

	rl := rotatelogs.NewRotateLogs(
		*logdir + "/carbonserver.%Y%m%d%H%M.log",
	)

	// Optional fields must be set afterwards
	rl.LinkName = *logdir + "/carbonserver.log"

	if *logtostdout {
		log.SetOutput(io.MultiWriter(os.Stdout, rl))
	} else {
		log.SetOutput(rl)
	}

	expvar.NewString("BuildVersion").Set(BuildVersion)
	log.Println("starting carbonserver", BuildVersion)

	loglevel := LOG_NORMAL
	if *verbose {
		loglevel = LOG_DEBUG
	}
	if *debug {
		loglevel = LOG_TRACE
	}

	logger = logLevel(loglevel)

	config.WhisperData = *whisperdata
	logger.Logf("reading whisper files from: %s", config.WhisperData)

	if *scanFrequency != 0 {
		logger.Logln("use file cache with scan frequency", *scanFrequency)
		force := make(chan struct{})
		go fileListUpdater(*whisperdata, time.Tick(*scanFrequency), force)
		force <- struct{}{}
	}

	runtime.GOMAXPROCS(*maxprocs)
	logger.Logf("set GOMAXPROCS=%d", *maxprocs)

	httputil.PublishTrackedConnections("httptrack")
	expvar.Publish("requestBuckets", expvar.Func(renderTimeBuckets))

	// +1 to track every over the number of buckets we track
	timeBuckets = make([]int64, config.Buckets+1)

	http.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(findHandler, bucketRequestTimes)))
	http.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(fetchHandler, bucketRequestTimes)))
	http.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(infoHandler, bucketRequestTimes)))

	// nothing in the config? check the environment
	if config.GraphiteHost == "" {
		if host := os.Getenv("GRAPHITEHOST") + ":" + os.Getenv("GRAPHITEPORT"); host != ":" {
			config.GraphiteHost = host
		}
	}

	// only register g2g if we have a graphite host
	if config.GraphiteHost != "" {

		logger.Logf("Using graphite host %v", config.GraphiteHost)

		// register our metrics with graphite
		graphite, err := g2g.NewGraphite(config.GraphiteHost, 60*time.Second, 10*time.Second)
		if err != nil {
			log.Fatalf("unable to connect to to graphite: %v: %v", config.GraphiteHost, err)
		}

		hostname, _ := os.Hostname()
		hostname = strings.Replace(hostname, ".", "_", -1)

		graphite.Register(fmt.Sprintf("carbon.server.%s.render_requests",
			hostname), Metrics.RenderRequests)
		graphite.Register(fmt.Sprintf("carbon.server.%s.render_errors",
			hostname), Metrics.RenderErrors)
		graphite.Register(fmt.Sprintf("carbon.server.%s.notfound",
			hostname), Metrics.NotFound)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_requests",
			hostname), Metrics.FindRequests)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_errors",
			hostname), Metrics.FindErrors)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_zero",
			hostname), Metrics.FindZero)

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("carbon.server.%s.requests_in_%dms_to_%dms", hostname, i*100, (i+1)*100), bucketEntry(i))
		}
	}

	listen := fmt.Sprintf(":%d", *port)
	logger.Logf("listening on %s", listen)
	err := http.ListenAndServe(listen, nil)
	if err != nil {
		log.Fatalf("%s", err)
	}
	logger.Logf("stopped")
}

type logLevel int

const (
	LOG_NORMAL logLevel = iota
	LOG_DEBUG
	LOG_TRACE
)

func (ll logLevel) Debugf(format string, a ...interface{}) {
	if ll >= LOG_DEBUG {
		log.Printf(format, a...)
	}
}

func (ll logLevel) Debugln(a ...interface{}) {
	if ll >= LOG_DEBUG {
		log.Println(a...)
	}
}

func (ll logLevel) Tracef(format string, a ...interface{}) {
	if ll >= LOG_TRACE {
		log.Printf(format, a...)
	}
}

func (ll logLevel) Traceln(a ...interface{}) {
	if ll >= LOG_TRACE {
		log.Println(a...)
	}
}
func (ll logLevel) Logln(a ...interface{}) {
	log.Println(a...)
}

func (ll logLevel) Logf(format string, a ...interface{}) {
	log.Printf(format, a...)
}

var timeBuckets []int64

type bucketEntry int

func (b bucketEntry) String() string {
	return strconv.Itoa(int(atomic.LoadInt64(&timeBuckets[b])))
}

func renderTimeBuckets() interface{} {
	return timeBuckets
}

func bucketRequestTimes(req *http.Request, t time.Duration) {

	ms := t.Nanoseconds() / int64(time.Millisecond)

	bucket := int(math.Log(float64(ms)) * math.Log10E)

	if bucket < 0 {
		bucket = 0
	}

	if bucket < config.Buckets {
		atomic.AddInt64(&timeBuckets[bucket], 1)
	} else {
		// Too big? Increment overflow bucket and log
		atomic.AddInt64(&timeBuckets[config.Buckets], 1)
		logger.Logf("Slow Request: %s: %s", t.String(), req.URL.String())
	}
}

func extractTrigrams(query string) []trigram.T {

	if len(query) < 3 {
		return nil
	}

	var start int
	var i int

	var trigrams []trigram.T

	for i < len(query) {
		if query[i] == '[' || query[i] == '*' || query[i] == '?' {
			trigrams = trigram.Extract(query[start:i], trigrams)

			if query[i] == '[' {
				for i < len(query) && query[i] != ']' {
					i++
				}
			}

			start = i + 1
		}
		i++
	}

	trigrams = trigram.Extract(query[start:i], trigrams)

	return trigrams
}
