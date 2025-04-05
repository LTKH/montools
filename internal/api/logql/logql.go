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
    //"strings"
    "encoding/json"
    "github.com/gorilla/mux"
	//"github.com/grafana/loki/pkg/logql"
    //"github.com/prometheus/common/model"
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
)

type LogQL struct {
    db           *db.Client
    debug        bool
}

type Resp struct {
    Status       string                    `json:"status"`
    Error        string                    `json:"error,omitempty"`
    Warnings     []string                  `json:"warnings,omitempty"`
    Data         interface{}               `json:"data,omitempty"`
}

type Stats struct {
    Bytes        int                       `json:"bytes"`       
    Chunks       int                       `json:"chunks"`  
    Entries      int                       `json:"entries"`  
    Streams      int                       `json:"streams"`  
}

type ResultType struct {
    ResultType   string                    `json:"resultType,omitempty"`
    IsPartial    bool                      `json:"isPartial,omitempty"`
    Result       []config.Result           `json:"result"`
}

//start=1739613624388000000&end=1739617224388000000
//start=1739613820&end=1739617420
//prom - "matrix" | "vector" | "scalar" | "string"
//loki - "matrix" | "vector" | "streams"

func encodeResp(resp *Resp) []byte {
    jsn, err := json.Marshal(resp)
    if err != nil {
        return encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)})
    }
    return jsn
}

func parseTime(s string) (time.Time, error) {
    if t, err := strconv.ParseInt(s, 10, 0); err == nil {
        return time.Unix(0, t).UTC(), nil
    }

    return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
    if s == "" {
        return 0, nil
    } 
    if d, err := time.ParseDuration(s); err == nil {
        return d, nil
    }

    return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
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

    query := r.FormValue("query")
    limit := 0

    start, err := parseTimeParam(r, "start", MinTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }

    timeout, err := parseDuration("60s")
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }

	result, err := db.Client.LokiQuery(*api.db, query, start, timeout, limit)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        return
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
}

func (api *LogQL) ApiQueryRange(w http.ResponseWriter, r *http.Request) {
	if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    query := r.FormValue("query")
    limit := 0

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

    step, err := parseDuration(r.FormValue("step"))
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }  

    result, err := db.Client.LokiQueryRange(*api.db, query, start, end, step, limit)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        return
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
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

    query := r.FormValue("match[]")
    
    series, err := db.Client.Series(*api.db, query, start, end)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        return
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:series}))
}

func (api *LogQL) ApiMetadata(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make(map[string]interface{}, 0)}))
}

func (api *LogQL) ApiIndexStats(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:Stats{}}))
}