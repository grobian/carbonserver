package main

import (
	//"github.com/kisielk/whisper-go/whisper"
	whisper "github.com/robyoung/go-whisper"
	pickle "github.com/kisielk/og-rek"

	"net/http"
	"encoding/json"
	"path/filepath"
	"strings"
	"strconv"
	"math"
	"fmt"
)

var config = struct {
	WhisperData	string
}{
	//WhisperData: "/var/lib/carbon/whisper",
	WhisperData: "..",
}

type WhisperFetchResponse struct {
	Name        string      `json:"name"`
	StartTime   int         `json:"startTime"`
	StopTime    int         `json:"stopTime"`
	StepTime    int         `json:"stepTime"`
	Values      []float64   `json:"values"`
	IsAbsent    []bool      `json:"isAbsent"`
}

type WhisperGlobResponse struct {
	Name        string      `json:"name"`
	Paths       []string    `json:"paths"`
}

func findHandler(wr http.ResponseWriter, req *http.Request) {
//	GET /metrics/find/?local=1&format=pickle&query=general.hadoop.lhr4.ha201jobtracker-01.jobtracker.NonHeapMemoryUsage.committed HTTP/1.1
//	http://localhost:8080/metrics/find/?query=test
	req.ParseForm()
	glob := req.FormValue("query")
	format := req.FormValue("format")

	if format != "json" && format != "pickle" {
		fmt.Printf("dropping invalid uri (format=%s): %s\n",
				format, req.URL.RequestURI())
		return
	}

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - carbon.[rz]    -> carbon.relays, carbon.zipper
	 * - carbon.{re,zi} -> carbon.relays, carbon.zipper
	 * - implicit * at the end of each query
	 * - match is either dir or .wsp file
	 * unfortunately, filepath.Glob doesn't handle the curly brace
	 * expansion for us */
	lbrace := strings.Index(glob, "{")
	rbrace := -1
	if lbrace > -1 {
		rbrace = strings.Index(glob[lbrace:], "}")
		if rbrace > -1 {
			rbrace += lbrace
		}
	}
	files := make([]string, 0)
	if lbrace > -1 && rbrace > -1 {
		expansion := glob[lbrace + 1:rbrace]
		parts := strings.Split(expansion, ",")
		for _, sub := range parts {
			sglob := glob[:lbrace] + sub + glob[rbrace + 1:]
			path := config.WhisperData + "/" + strings.Replace(sglob, ".", "/", -1) + "*"
			nfiles, err := filepath.Glob(path)
			if err == nil {
				files = append(files, nfiles...)
			}
		}
	} else {
		path := config.WhisperData + "/" + strings.Replace(glob, ".", "/", -1) + "*"
		nfiles, err := filepath.Glob(path)
		if err != nil {
			files = append(files, nfiles...)
		}
	}

	leafs := make([]bool, len(files))
	for i, p := range files {
		p = p[len(config.WhisperData + "/"):]
		if strings.HasSuffix(p, ".wsp") {
			p = p[:len(p) - 4]
			leafs[i] = true
		} else {
			leafs[i] = false
		}
		files[i] = strings.Replace(p, "/", ".", -1)
	}

	if format == "json" {
		response := WhisperGlobResponse {
			Name:		glob,
			Paths:		make([]string, 0),
		}
		for _, p := range files {
			response.Paths = append(response.Paths, p)
		}
		b, err := json.Marshal(response)
		if err != nil {
			fmt.Printf("failed to create JSON data for %s: %s\n", glob, err)
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
	fmt.Printf("find: %d hits for %s\n", len(files), glob)
	return
}

func fetchHandler(wr http.ResponseWriter, req *http.Request) {
//	GET /render/?target=general.me.1.percent_time_active.pfnredis&format=pickle&from=1396008021&until=1396022421 HTTP/1.1
//	http://localhost:8080/render/?target=testmetric&format=json&from=1395961200&until=1395961800
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	if format != "json" && format != "pickle" {
		fmt.Printf("dropping invalid uri (format=%s): %s\n",
				format, req.URL.RequestURI())
		http.Error(wr, "Bad request (unsupported format)", http.StatusBadRequest)
		return
	}

	path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		// the FE/carbonzipper often requests metrics we don't have
		//fmt.Printf("failed to open %s: %s\n", path, err)
		w = nil
	}

	i, err := strconv.Atoi(from)
	if err != nil {
		fmt.Printf("fromTime (%s) invalid: %s\n", from, err)
		if w != nil {
			w.Close()
		}
		w = nil
	}
	fromTime := int(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		fmt.Printf("untilTime (%s) invalid: %s\n", from, err)
		if w != nil {
			w.Close()
		}
		w = nil
	}
	untilTime := int(i)

	if (w != nil) {
		defer w.Close()
	} else {
		http.Error(wr, "Bad request (invalid from/until time)", http.StatusBadRequest)
		return
	}

	points, err := w.Fetch(fromTime, untilTime)
	if err != nil {
		fmt.Printf("failed to getch points from %s: %s\n", path, err)
		return
	}
	values := points.Values()

	if format == "json" {
		response := WhisperFetchResponse {
			Name:		metric,
			StartTime:	points.FromTime(),
			StopTime:	points.UntilTime(),
			StepTime:	points.Step(),
			Values:		make([]float64, len(values)),
			IsAbsent:	make([]bool, len(values)),
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

		b, err := json.Marshal(response)
		if err != nil {
			fmt.Printf("failed to create JSON data for %s: %s\n", path, err)
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

	fmt.Printf("served %d points\n", len(values))
	return
}

func main() {
	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", fetchHandler)

	http.ListenAndServe(":8080", nil)
}
