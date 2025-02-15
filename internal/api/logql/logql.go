package logql

import (
    "log"
    "fmt"
    "math"
    "net/http"
    "time"
    "strconv"
    //"errors"
    //"compress/gzip"
    //"io"
    //"bytes"
    //"regexp"
    //"io/ioutil"
    "strings"
    "encoding/json"
    "github.com/gorilla/mux"
	//"github.com/grafana/loki/pkg/logql"
    "github.com/ltkh/montools/internal/db"
    "github.com/ltkh/montools/internal/config/mtprom"
)

var (
    // MinTime is the default timestamp used for the start of optional time ranges.
    // Exposed to let downstream projects reference it.
    //
    // Historical note: This should just be time.Unix(math.MinInt64/1000, 0).UTC(),
    // but it was set to a higher value in the past due to a misunderstanding.
    // The value is still low enough for practical purposes, so we don't want
    // to change it now, avoiding confusion for importers of this variable.
    MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

    // MaxTime is the default timestamp used for the end of optional time ranges.
    // Exposed to let downstream projects to reference it.
    //
    // Historical note: This should just be time.Unix(math.MaxInt64/1000, 0).UTC(),
    // but it was set to a lower value in the past due to a misunderstanding.
    // The value is still high enough for practical purposes, so we don't want
    // to change it now, avoiding confusion for importers of this variable.
    MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

    minTimeFormatted = MinTime.Format(time.RFC3339Nano)
    maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)

/*
/loki/api/v1/labels
/loki/api/v1/query_range
/loki/api/v1/index/stats
*/

type LogQL struct {
    db           *db.Client
    debug        bool
}

type Resp struct {
    Status       string                    `json:"status"`
    Error        string                    `json:"error,omitempty"`
    Warnings     []string                  `json:"warnings,omitempty"`
    Data         interface{}               `json:"data"`
}

func encodeResp(resp *Resp) []byte {
    jsn, err := json.Marshal(resp)
    if err != nil {
        return encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)})
    }
    return jsn
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
    val := r.FormValue(paramName)
    if val == "" {
        return defaultValue, nil
    }
    result, err := parseTime(val)
    if err != nil {
        return time.Time{}, fmt.Errorf("invalid time value for '%s': %w", paramName, err)
    }
    return result, nil
}

func parseTime(s string) (time.Time, error) {
    if t, err := strconv.ParseFloat(s, 64); err == nil {
        s, ns := math.Modf(t)
        ns = math.Round(ns*1000) / 1000
        return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
    }
    if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
        return t, nil
    }

    // Stdlib's time parser can only handle 4 digit years. As a workaround until
    // that is fixed we want to at least support our own boundary times.
    // Context: https://github.com/prometheus/client_golang/issues/614
    // Upstream issue: https://github.com/golang/go/issues/20555
    switch s {
        case minTimeFormatted:
            return MinTime, nil
        case maxTimeFormatted:
            return MaxTime, nil
    }
    return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func New(conf *config.Upstream) (*LogQL, error) {
    conn, err := db.NewClient(conf.Source, conf.Debug)
    if err != nil {
        return &LogQL{}, err
    }
    return &LogQL{ db: &conn, debug: conf.Debug }, nil
}

func (api *LogQL) ApiLabels(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    start, err := parseTimeParam(r, "start", MinTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }
    end, err := parseTimeParam(r, "end", MaxTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }

    labels, err := db.Client.Labels(*api.db, start, end)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }
    labels = append(labels, "__table__")

    for _, v := range labels {
        log.Printf("%v", v)
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *LogQL) ApiLabelValues(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    start, err := parseTimeParam(r, "start", MinTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }
    end, err := parseTimeParam(r, "end", MaxTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }
    params := mux.Vars(r)

    labels, err := db.Client.LabelValues(*api.db, params["name"], start, end)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }

    if len(labels) == 0 {
        labels = make([]string, 0)
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *LogQL) ApiQuery(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *LogQL) ApiQueryRange(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string

	query := r.URL.Query().Get("query")
	if query == "" {
		w.WriteHeader(400)
		w.Write(encodeResp(&Resp{Status:"error", Error:"query string is empty", Data:make([]int, 0)}))
		return
	}

    /*
	test, err := logql.ParseLogSelector(query)
    if err != nil {
        w.WriteHeader(400)
		w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
		return
    }
    log.Printf("%v", test)
    */

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *LogQL) ApiSeries(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    start, err := parseTimeParam(r, "start", MinTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }
    end, err := parseTimeParam(r, "end", MaxTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }

    query := strings.Replace(r.FormValue("match[]"), ".", ":", -1)
    
    series, err := db.Client.Series(*api.db, query, start, end)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }

    if len(series) == 0 {
        series = make([]map[string]string, 0)
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:series}))
}

func (api *LogQL) ApiIndexStats(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:[]string{"zero"}}))
}