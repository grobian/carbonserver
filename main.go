package main

import (
	"github.com/kisielk/whisper-go/whisper"
	pickle "github.com/kisielk/og-rek"

	"net/http"
	"encoding/json"
	"path/filepath"
	"strings"
	"strconv"
	"fmt"
)

var config = struct {
	WhisperData	string
}{
	WhisperData: "/var/lib/carbon/whisper",
	//WhisperData: "..",
}

type WhisperFetchResponse struct {
	Name		string		`json:"name"`
	StartTime	uint32		`json:"startTime"`
	StopTime	uint32		`json:"stopTime"`
	StepTime	uint32		`json:"stepTime"`
	Values		[]float64	`json:"values"`
}

type WhisperGlobResponse struct {
	Name		string		`json:"name"`
	Paths		[]string	`json:"paths"`
}

func findHandler(wr http.ResponseWriter, req *http.Request) {
//	GET /metrics/find/?local=1&format=pickle&query=general.hadoop.lhr4.ha201jobtracker-01.jobtracker.NonHeapMemoryUsage.committed HTTP/1.1
//	http://localhost:8080/metrics/glob/?query=test
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
	 * - implicit * at the end of each query
	 * - match is either dir or .wsp file
	 * (this is less featureful than original carbon)
	 */
	path := config.WhisperData + "/" + strings.Replace(glob, ".", "/", -1) + "*"
	files, err := filepath.Glob(path)
	if err != nil {
		files = make([]string, 0)
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
	fmt.Printf("served %d points\n", len(files))
	return
}

func fetchHandler(wr http.ResponseWriter, req *http.Request) {
//	GET /render/?target=general.me.1.percent_time_active.pfnredis&format=pickle&from=1396008021&until=1396022421 HTTP/1.1
//	http://localhost:8080/metrics/fetch/?target=testmetric&format=json&from=1395961200&until=1395961800
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	if format != "json" && format != "pickle" {
		fmt.Printf("dropping invalid uri (format=%s): %s\n",
				format, req.URL.RequestURI())
		return
	}

	path := config.WhisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		fmt.Printf("failed to open %s: %s\n", path, err)
		return
	}
	defer w.Close()

	i, err := strconv.Atoi(from)
	if err != nil {
		fmt.Printf("fromTime (%s) invalid: %s\n", from, err)
	}
	fromTime := uint32(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		fmt.Printf("untilTime (%s) invalid: %s\n", from, err)
	}
	untilTime := uint32(i)

	interval, points, err := w.FetchUntil(fromTime, untilTime)
	if err != nil {
		fmt.Printf("failed to getch points from %s: %s\n", path, err)
		return
	}

	if format == "json" {
		response := WhisperFetchResponse {
			Name:		metric,
			StartTime:	interval.FromTimestamp,
			StopTime:	interval.UntilTimestamp,
			StepTime:	interval.Step,
		}
		for _, p := range points {
			response.Values = append(response.Values, p.Value)
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
		m["start"] = interval.FromTimestamp
		m["step"] = interval.Step
		m["end"] = interval.UntilTimestamp
		m["name"] = metric

		values := make([]interface{}, len(points))
		for i, p := range points {
			values[i] = p.Value
		}
		m["values"] = values

		metrics = append(metrics, m)

		wr.Header().Set("Content-Type", "application/pickle")
		pEnc := pickle.NewEncoder(wr)
		pEnc.Encode(metrics)
	}

	fmt.Printf("served %d points\n", len(points))
	return
}

func main() {
	http.HandleFunc("/metrics/find/", findHandler)
	http.HandleFunc("/render/", fetchHandler)

	http.ListenAndServe(":8080", nil)
}
