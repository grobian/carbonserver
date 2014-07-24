package main

import (
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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/dgryski/httputil"
	pb "github.com/grobian/carbonserver/carbonserverpb"
	whisper "github.com/grobian/go-whisper"
	pickle "github.com/kisielk/og-rek"
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
	InfoRequests   *expvar.Int
	InfoErrors     *expvar.Int
}{
	RenderRequests: expvar.NewInt("render_requests"),
	RenderErrors:   expvar.NewInt("render_errors"),
	NotFound:       expvar.NewInt("notfound"),
	FindRequests:   expvar.NewInt("find_requests"),
	FindErrors:     expvar.NewInt("find_errors"),
	InfoRequests:   expvar.NewInt("info_requests"),
	InfoErrors:     expvar.NewInt("info_errors"),
}

var log Logger

func findHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob

	Metrics.FindRequests.Add(1)

	req.ParseForm()
	format := req.FormValue("format")
	query := req.FormValue("query")

	if format != "json" && format != "pickle" && format != "protobuf" {
		Metrics.FindErrors.Add(1)
		log.Warnf("dropping invalid uri (format=%s): %s",
			format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if query == "" {
		Metrics.FindErrors.Add(1)
		log.Warnf("dropping invalid request (query=): %s", req.URL.RequestURI())
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
	for _, glob := range globs {
		nfiles, err := filepath.Glob(config.WhisperData + "/" + glob)
		if err == nil {
			files = append(files, nfiles...)
		}
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
			log.Errorf("failed to create %s data for glob %s: %s",
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
	log.Infof("find: %d hits for %s", len(files), req.FormValue("query"))
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

	if format != "json" && format != "pickle" && format != "protobuf" {
		Metrics.RenderErrors.Add(1)
		log.Warnf("dropping invalid uri (format=%s): %s",
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
		log.Debugf("failed to %s", err)
		http.Error(wr, "Metric not found", http.StatusNotFound)
		return
	}

	i, err := strconv.Atoi(from)
	if err != nil {
		log.Debugf("fromTime (%s) invalid: %s (in %s)",
			from, err, req.URL.RequestURI())
		if w != nil {
			w.Close()
		}
		w = nil
	}
	fromTime := int(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		log.Debugf("untilTime (%s) invalid: %s (in %s)",
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
		log.Errorf("failed to fetch points from %s: %s", path, err)
		http.Error(wr, "Fetching data points failed",
			http.StatusInternalServerError)
		return
	}

	if points == nil {
		Metrics.NotFound.Add(1)
		log.Debugf("Metric time range not found: metric=%s from=%d to=%d ", metric, fromTime, untilTime)
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
			log.Errorf("failed to create %s data for %s: %s", format, path, err)
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

	log.Infof("served %d points for %s", len(values), metric)
	return
}

func infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.name&format=json

	//	Metrics.InfoRequests.Add(1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")

	if format != "json" {
		//		Metrics.InfoErrors.Add(1)
		log.Warnf("dropping invalid uri (format=%s): %s",
			format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		//		Metrics.NotFound.Add(1)
		log.Debugf("failed to %s", err)
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
			log.Errorf("failed to create %s data for %s: %s", format, path, err)
			return
		}
		wr.Write(b)
	}

	log.Infof("served info for %s", metric)
	return
}

func main() {
	port := flag.Int("p", 8080, "port to bind to")
	verbose := flag.Bool("v", false, "enable verbose logging")
	debug := flag.Bool("vv", false, "enable more verbose (debug) logging")
	whisperdata := flag.String("w", config.WhisperData, "location where whisper files are stored")
	maxprocs := flag.Int("maxprocs", runtime.NumCPU()*80/100, "GOMAXPROCS")

	flag.Parse()

	loglevel := WARN
	if *verbose {
		loglevel = INFO
	}
	if *debug {
		loglevel = DEBUG
	}
	log = NewOutputLogger(loglevel)

	config.WhisperData = *whisperdata
	log.Infof("reading whisper files from: %s", config.WhisperData)

	runtime.GOMAXPROCS(*maxprocs)
	log.Infof("set GOMAXPROCS=%d", *maxprocs)

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

		log.Infof("Using graphite host %v", config.GraphiteHost)

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

		for i := 0; i <= config.Buckets; i++ {
			graphite.Register(fmt.Sprintf("carbon.server.%s.requests_in_%dms_to_%dms", hostname, i*100, (i+1)*100), bucketEntry(i))
		}
	}

	listen := fmt.Sprintf(":%d", *port)
	log.Infof("listening on %s", listen)
	err := http.ListenAndServe(listen, nil)
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Infof("stopped")
}

// Logger is a wrapper to enable/disable lots of spam
type Logger interface {
	Infof(format string, a ...interface{})
	Warnf(format string, a ...interface{})
	Errorf(format string, a ...interface{})
	Fatalf(format string, a ...interface{})
	Debugf(format string, a ...interface{})
}

type LogLevel int

// Logging levels
const (
	FATAL LogLevel = iota
	ERROR
	WARN
	INFO
	DEBUG
)

type outputLogger struct {
	level LogLevel
	out   *os.File
	err   *os.File
}

func NewOutputLogger(level LogLevel) *outputLogger {
	r := new(outputLogger)
	r.level = level
	r.out = os.Stdout
	r.err = os.Stderr

	return r
}

func (l *outputLogger) Debugf(format string, a ...interface{}) {
	if l.level >= DEBUG {
		l.out.WriteString(fmt.Sprintf("DEBUG: "+format+"\n", a...))
	}
}

func (l *outputLogger) Infof(format string, a ...interface{}) {
	if l.level >= INFO {
		l.out.WriteString(fmt.Sprintf("INFO: "+format+"\n", a...))
	}
}

func (l *outputLogger) Warnf(format string, a ...interface{}) {
	if l.level >= WARN {
		l.out.WriteString(fmt.Sprintf("WARN: "+format+"\n", a...))
	}
}

func (l *outputLogger) Errorf(format string, a ...interface{}) {
	if l.level >= ERROR {
		l.err.WriteString(fmt.Sprintf("ERROR: "+format+"\n", a...))
	}
}

func (l *outputLogger) Fatalf(format string, a ...interface{}) {
	if l.level >= FATAL {
		l.err.WriteString(fmt.Sprintf("ERROR: "+format+"\n", a...))
	}
	os.Exit(1)
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
		log.Warnf("Slow Request: %s: %s", t.String(), req.URL.String())
	}
}
