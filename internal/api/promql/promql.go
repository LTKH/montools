package promql

import (
    "io"
    "io/ioutil"
    "log"
    "fmt"
    "math"
    //"strconv"
    "strings"
    "net/http"
    "time"
    "errors"
    //"compress/gzip"
    //"io"
    //"bytes"
    //"regexp"
    //"io/ioutil"
    "strconv"
    "encoding/json"
    "compress/gzip"
    "github.com/gorilla/mux"
    "github.com/prometheus/common/model"
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

type Prom struct {
    db           *db.Client
    debug        bool
}

type Resp struct {
    Status       string                    `json:"status"`
    Error        string                    `json:"error,omitempty"`
    Warnings     []string                  `json:"warnings,omitempty"`
    Data         interface{}               `json:"data"`
}

type ResultType struct {
    ResultType   string                    `json:"resultType,omitempty"`
    IsPartial    bool                      `json:"isPartial,omitempty"`
    Result       []config.Result           `json:"result"`
}

type PromQuery struct {
    BinaryExpr   string
    AggrExpr     []string
    CallFunc     string
    MatrixSel    string
    VectorSel    string
    Field        string
    PromQuery    []PromQuery
}

func encodeResp(resp *Resp) []byte {
    jsn, err := json.Marshal(resp)
    if err != nil {
        return encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)})
    }
    return jsn
}

func readData(r *http.Request) (map[string][]string, []byte, error) {
    var reader io.ReadCloser
    var err error

    // Check that the server actual sent compressed data
    if r.Header.Get("Content-Encoding") == "gzip" {
        reader, err = gzip.NewReader(r.Body)
        if err != nil {
            return r.Header, nil, err
        }
        delete(r.Header, "Content-Encoding")
        delete(r.Header, "Content-Length")
        defer reader.Close()
    } else {
        reader = r.Body
    }
    defer r.Body.Close()

    data, err := ioutil.ReadAll(reader)
    if err != nil {
        return r.Header, nil, err
    }

    return r.Header, data, nil
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

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func New(conf *config.Upstream) (*Prom, error) {
    conn, err := db.NewClient(conf.Source, conf.Debug)
    if err != nil {
        return &Prom{}, err
    }
    return &Prom{ db: &conn, debug: conf.Debug }, nil
}

func (api *Prom) ApiLabels(w http.ResponseWriter, r *http.Request) {
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
    labels = append(labels, "__name__")

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *Prom) ApiLabelValues(w http.ResponseWriter, r *http.Request) {
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

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *Prom) ApiQuery(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    time, err := parseTimeParam(r, "time", MinTime)
    if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
    }

    query := strings.Replace(r.FormValue("query"), ".", ":", -1)
    
    results, err := db.Client.Query(*api.db, query, 1, time, 0)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        return
    }

    result := ResultType{
        ResultType: "vector",
        Result: results,
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
}

func (api *Prom) ApiQueryRange(w http.ResponseWriter, r *http.Request) {
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

    step, err := parseDuration(r.FormValue("step"))
	if err != nil {
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
	}

	if step <= 0 {
        err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
        log.Printf("[error] %v", err)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        w.WriteHeader(400)
        return
	}

    limit := 0
    query := strings.Replace(r.FormValue("query"), ".", ":", -1)

    results, err := db.Client.QueryRange(*api.db, query, limit, start, end, step)
    if err != nil {
        log.Printf("[error] %v", err)
        w.WriteHeader(500)
        w.Write(encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)}))
        return
    }

    result := ResultType{
        ResultType: "matrix",
        Result: results,
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
}

func (api *Prom) ApiQueryExemplars(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")

    var labels []string

    /*
    query := r.URL.Query().Get("query")
    if query == "" {
        w.WriteHeader(400)
        w.Write(encodeResp(&Resp{Status:"error", Error:"query string is empty", Data:make([]int, 0)}))
        return
    }

    
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

func (api *Prom) ApiSeries(w http.ResponseWriter, r *http.Request) {
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

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:series}))
}

func (api *Prom) ApiMetadata(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make(map[string]interface{}, 0)}))
}

func (api *Prom) ApiRules(w http.ResponseWriter, r *http.Request) {
    if api.debug { log.Printf(r.URL.Path) }
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make([]int, 0)}))
}