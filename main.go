/*
 * Copyright 2013-2016 Fabian Groffen, Damian Gryski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
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

	"github.com/NYTimes/gziphandler"
	pb "github.com/dgryski/carbonzipper/carbonzipperpb"
	"github.com/dgryski/carbonzipper/mlog"
	"github.com/dgryski/carbonzipper/mstats"
	"github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	"github.com/gogo/protobuf/proto"
	whisper "github.com/grobian/go-whisper"
	pickle "github.com/kisielk/og-rek"
	g2g "github.com/peterbourgon/g2g"
	"github.com/facebookgo/pidfile"
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

var logger mlog.Level

type fileIndex struct {
	idx   trigram.Index
	files []string
}

var fileIdx atomic.Value

func CurrentFileIndex() *fileIndex {
	p := fileIdx.Load()
	if p == nil {
		return nil
	}
	return p.(*fileIndex)
}
func UpdateFileIndex(fidx *fileIndex) { fileIdx.Store(fidx) }

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

		logger.Debugln("file scan took", time.Since(t0), ",", len(files), "items")
		t0 = time.Now()

		idx := trigram.NewIndex(files)

		logger.Debugln("indexing took", time.Since(t0), len(idx), "trigrams")

		pruned := idx.Prune(0.95)

		logger.Debugln("pruned", pruned, "common trigrams")

		if err == nil {
			UpdateFileIndex(&fileIndex{idx, files})
		}
	}
}

func expandGlobs(query string) ([]string, []bool) {
	var useGlob bool

	if star := strings.IndexByte(query, '*'); strings.IndexByte(query, '[') == -1 && strings.IndexByte(query, '?') == -1 && (star == -1 || star == len(query)-1) {
		useGlob = true
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
	// TODO(dgryski): move this loop into its own function + add tests
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

	if fidx != nil && !useGlob {
		// use the index
		docs := make(map[trigram.DocID]struct{})

		for _, g := range globs {

			gpath := "/" + g

			ts := extractTrigrams(g)

			// TODO(dgryski): If we have 'not enough trigrams' we
			// should bail and use the file-system glob instead

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

	// Not an 'else' clause because the trigram-searching code might want
	// to fall back to the file-system glob

	if useGlob || fidx == nil {
		// no index or we were asked to hit the filesystem
		for _, g := range globs {
			nfiles, err := filepath.Glob(config.WhisperData + "/" + g)
			if err == nil {
				files = append(files, nfiles...)
			}
		}
	}

	leafs := make([]bool, len(files))
	for i, p := range files {
		s, err := os.Stat(p)
		if err != nil {
			continue
		}
		p = p[len(config.WhisperData+"/"):]
		if !s.IsDir() && strings.HasSuffix(p, ".wsp") {
			p = p[:len(p)-4]
			leafs[i] = true
		} else {
			leafs[i] = false
		}
		files[i] = strings.Replace(p, "/", ".", -1)
	}

	return files, leafs
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

	files, leafs := expandGlobs(query)

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

	t0 := time.Now()

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

	files, leafs := expandGlobs(metric)

	var badTime bool

	i, err := strconv.Atoi(from)
	if err != nil {
		logger.Debugf("fromTime (%s) invalid: %s (in %s)", from, err, req.URL.RequestURI())
		badTime = true
	}
	fromTime := int(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		logger.Debugf("untilTime (%s) invalid: %s (in %s)", from, err, req.URL.RequestURI())
		badTime = true
	}
	untilTime := int(i)

	if badTime {
		Metrics.RenderErrors.Add(1)
		http.Error(wr, "Bad request (invalid from/until time)", http.StatusBadRequest)
		return
	}

	var multi pb.MultiFetchResponse
	for i, metric := range files {

		if !leafs[i] {
			logger.Debugf("skipping directory = %q\n", metric)
			// can't fetch a directory
			continue
		}

		path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
		w, err := whisper.Open(path)
		if err != nil {
			// the FE/carbonzipper often requests metrics we don't have
			// We shouldn't really see this any more -- expandGlobs() should filter them out
			Metrics.NotFound.Add(1)
			logger.Logf("error opening %q: %v\n", path, err)
			continue
		}

		points, err := w.Fetch(fromTime, untilTime)
		w.Close()
		if err != nil {
			Metrics.RenderErrors.Add(1)
			logger.Logf("failed to fetch points from %s: %s", path, err)
			continue
		}

		if points == nil {
			Metrics.NotFound.Add(1)
			logger.Debugf("Metric time range not found: metric=%s from=%d to=%d ", metric, fromTime, untilTime)
			continue
		}

		values := points.Values()

		fromTime := int32(points.FromTime())
		untilTime := int32(points.UntilTime())
		step := int32(points.Step())
		response := pb.FetchResponse{
			Name:      proto.String(metric),
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

		multi.Metrics = append(multi.Metrics, &response)
	}

	var b []byte
	switch format {
	case "json":
		wr.Header().Set("Content-Type", "application/json")
		b, err = json.Marshal(multi)

	case "protobuf":
		wr.Header().Set("Content-Type", "application/protobuf")
		b, err = proto.Marshal(&multi)

	case "pickle":
		// transform protobuf data into what pickle expects
		//[{'start': 1396271100, 'step': 60, 'name': 'metric',
		//'values': [9.0, 19.0, None], 'end': 1396273140}

		var response []map[string]interface{}

		for _, metric := range multi.GetMetrics() {

			var m map[string]interface{}

			m = make(map[string]interface{})
			m["start"] = metric.StartTime
			m["step"] = metric.StepTime
			m["end"] = metric.StopTime
			m["name"] = metric.Name

			mv := make([]interface{}, len(metric.Values))
			for i, p := range metric.Values {
				if metric.IsAbsent[i] {
					mv[i] = nil
				} else {
					mv[i] = p
				}
			}

			m["values"] = mv
			response = append(response, m)
		}

		wr.Header().Set("Content-Type", "application/pickle")
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		err = pEnc.Encode(response)
		b = buf.Bytes()
	}

	if err != nil {
		Metrics.RenderErrors.Add(1)
		logger.Logf("failed to create %s data for %s: %s", format, "<metric>", err)
		return
	}
	wr.Write(b)

	logger.Debugf("fetch: served %q from %d to %d in %v", metric, fromTime, untilTime, time.Since(t0))
}

func infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.name&format=json

	Metrics.InfoRequests.Add(1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")

	if format == "" {
		format = "json"
	}

	if format != "json" && format != "protobuf" {
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

	defer w.Close()

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

	logger.Debugf("served info for %s", metric)
	return
}

func main() {
	port := flag.Int("p", 8080, "port to bind to")
	verbose := flag.Bool("v", false, "enable verbose logging")
	debug := flag.Bool("vv", false, "enable more verbose (debug) logging")
	whisperdata := flag.String("w", config.WhisperData, "location where whisper files are stored")
	maxglobs := flag.Int("maxexpand", config.MaxGlobs, "maximum expansion depth to perform on input via curly braces ({a,b,c})")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU()*80/100, "GOMAXPROCS")
	logdir := flag.String("logdir", "/var/log/carbonserver/", "logging directory")
	logtostdout := flag.Bool("stdout", false, "log also to stdout")
	scanFrequency := flag.Duration("scanfreq", 0, "file index scan frequency (0 to disable file index)")
	interval := flag.Duration("i", 60*time.Second, "interval to report internal statistics to graphite")
	pidFile := flag.String("pid", "", "pidfile (default: empty, don't create pidfile)")

	flag.Parse()

	if *logdir == "" {
		mlog.SetRawStream(os.Stdout)
	} else {
		mlog.SetOutput(*logdir, "carbonserver", *logtostdout)
	}

	expvar.NewString("BuildVersion").Set(BuildVersion)
	logger.Logln("starting carbonserver", BuildVersion)

	loglevel := mlog.Normal
	if *verbose {
		loglevel = mlog.Debug
	}
	if *debug {
		loglevel = mlog.Trace
	}

	logger = mlog.Level(loglevel)

	config.WhisperData = strings.TrimRight(*whisperdata, "/")
	logger.Logf("reading whisper files from: %s", config.WhisperData)

	config.MaxGlobs = *maxglobs
	logger.Logf("maximum brace expansion set to: %d", config.MaxGlobs)

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
		graphite := g2g.NewGraphite(config.GraphiteHost, *interval, 10*time.Second)

		hostname, _ := os.Hostname()
		hostname = strings.Replace(hostname, ".", "_", -1)

		graphite.Register(fmt.Sprintf("carbon.server.%s.render_requests", hostname), Metrics.RenderRequests)
		graphite.Register(fmt.Sprintf("carbon.server.%s.render_errors", hostname), Metrics.RenderErrors)
		graphite.Register(fmt.Sprintf("carbon.server.%s.notfound", hostname), Metrics.NotFound)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_requests", hostname), Metrics.FindRequests)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_errors", hostname), Metrics.FindErrors)
		graphite.Register(fmt.Sprintf("carbon.server.%s.find_zero", hostname), Metrics.FindZero)

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("carbon.server.%s.requests_in_%dms_to_%dms", hostname, i*100, (i+1)*100), bucketEntry(i))
		}

		go mstats.Start(*interval)

		graphite.Register(fmt.Sprintf("carbon.server.%s.alloc", hostname), &mstats.Alloc)
		graphite.Register(fmt.Sprintf("carbon.server.%s.total_alloc", hostname), &mstats.TotalAlloc)
		graphite.Register(fmt.Sprintf("carbon.server.%s.num_gc", hostname), &mstats.NumGC)
		graphite.Register(fmt.Sprintf("carbon.server.%s.pause_ns", hostname), &mstats.PauseNS)

	}

	if *pidFile != "" {
		pidfile.SetPidfilePath(*pidFile)
		err := pidfile.Write()
		if err != nil {
			logger.Fatalln("error during pidfile.Write():", err)
		}
	}

	listen := fmt.Sprintf(":%d", *port)
	logger.Logf("listening on %s", listen)
	err := http.ListenAndServe(listen, gziphandler.GzipHandler(http.DefaultServeMux))
	if err != nil {
		logger.Fatalf("%s", err)
	}
	logger.Logf("stopped")
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

	if start < i {
		trigrams = trigram.Extract(query[start:i], trigrams)
	}

	return trigrams
}
