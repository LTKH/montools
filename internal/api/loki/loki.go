package loki

import (
    "log"
    //"fmt"
    //"strconv"
    "net/http"
    //"time"
    //"errors"
    //"compress/gzip"
    //"io"
    //"bytes"
    //"regexp"
    //"io/ioutil"
    "encoding/json"
	//"github.com/grafana/loki/pkg/logql"
)

/*
/loki/api/v1/labels
/loki/api/v1/query_range
/loki/api/v1/index/stats
*/

type Loki struct {

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

func NewLoki() (*Loki, error) {
    return &Loki{}, nil
}

func (loki *Loki) ApiLoki(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.WriteHeader(204)
    //w.Write()
}

func (loki *Loki) ApiLokiLabels(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string
	labels = append(labels, "table")
	labels = append(labels, "field")

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (loki *Loki) ApiLokiLabelValues(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string
	labels = append(labels, "test1")
	labels = append(labels, "test2")

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (loki *Loki) ApiLokiQuery(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (loki *Loki) ApiLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	log.Printf("%v", r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string

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

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}