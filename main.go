package main

import (
	"github.com/kisielk/whisper-go/whisper"

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

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - implicit * at the end of each query
	 * - match is either dir or .wsp file
	 */
	path := config.WhisperData + "/" + strings.Replace(glob, ".", "/", -1) + "*"
	files, err := filepath.Glob(path)
	if err != nil {
		files = make([]string, 0)
	}

	response := WhisperGlobResponse {
		Name:		glob,
	}
	for _, p := range files {
		p = p[len(config.WhisperData + "/"):]
		if strings.HasSuffix(p, ".wsp") {
			p = p[:len(p) - 4]
		}
		response.Paths = append(response.Paths, strings.Replace(p, "/", ".", -1))
	}

	b, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("failed to create JSON data for %s: %s\n", glob, err)
		return
	}
	wr.Write(b)
	fmt.Printf("served %d points\n", len(response.Paths))
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

	if format != "json" {
		fmt.Printf("dropping invalid uri (format): %s\n", req.URL.RequestURI())
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
	fmt.Printf("served %d points\n", len(response.Values))
	return
}

func main() {
	http.HandleFunc("/metrics/glob/", findHandler)
	http.HandleFunc("/metrics/fetch/", fetchHandler)

	http.ListenAndServe("localhost:8080", nil)
}
