package prom

import (
    "log"
    "fmt"
    //"strconv"
    //"strings"
    "net/http"
    "time"
    //"errors"
    //"compress/gzip"
    //"io"
    //"bytes"
    //"regexp"
    //"io/ioutil"
    "encoding/json"
	//"github.com/VictoriaMetrics/metricsql"
    //"github.com/prometheus/prometheus/promql"
    "github.com/prometheus/prometheus/promql/parser"
    "github.com/gorilla/mux"
    "github.com/ltkh/montools/internal/db"
    "github.com/ltkh/montools/internal/config"
)

type Prom struct {
    db           *db.Client
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
    Result       []Result                  `json:"result"`
}

type Result struct {
    Metric       map[string]string         `json:"metric"`
    Value        []interface{}             `json:"value,omitempty"`
    Values       []interface{}             `json:"values,omitempty"`
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

func Walk(query PromQuery, node parser.Node) (PromQuery) {
	switch n := node.(type) {
	case *parser.BinaryExpr:
        query.BinaryExpr = n.Op.String()
        fmt.Printf("Binary Expression: %s\n", n.Op)
        query = Walk(query, n.LHS)
        query = Walk(query, n.RHS)
	case *parser.AggregateExpr:
		fmt.Printf("Aggregate Expression: %s\n", n.Op)
        query.AggrExpr = append(query.AggrExpr, n.Op.String())
        //Walk(query.PromQuery, n.Expr)
    case *parser.Call:
        fmt.Printf("Call: %s\n", n.String())
        query.CallFunc = n.Func.Name
        for _, arg := range n.Args {
            query = Walk(query, arg)
        }
	case *parser.VectorSelector:
		fmt.Printf("Vector Selector: %s\n", n.String())
        query.VectorSel = n.String()
	case *parser.MatrixSelector:
		fmt.Printf("Matrix Selector: %s\n", n.String())
        query.MatrixSel = n.String()
        query = Walk(query, n.VectorSelector)
	case *parser.NumberLiteral:
		fmt.Printf("Number Literal: %f\n", n.Val)
	case *parser.StringLiteral:
		fmt.Printf("String Literal: %s\n", n.Val)
	case *parser.ParenExpr:
		fmt.Println("Parentheses")
		query = Walk(query, n.Expr)
	default:
		fmt.Printf("Unknown node type: %T\n", n)
	}

    return query
}

func encodeResp(resp *Resp) []byte {
    jsn, err := json.Marshal(resp)
    if err != nil {
        return encodeResp(&Resp{Status:"error", Error:err.Error(), Data:make([]int, 0)})
    }
    return jsn
}

func New(conf *config.Upstream) (*Prom, error) {
    conn, err := db.NewClient(conf.Source)
    if err != nil {
        return &Prom{}, err
    }
    return &Prom{ db: &conn }, nil
}

func (api *Prom) ApiProm(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.WriteHeader(204)
}

func (api *Prom) ApiLabels(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	var labels []string
	labels = append(labels, "__name__")

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
}

func (api *Prom) ApiLabelValues(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")

    params := mux.Vars(r)
    switch params["name"] {
        case "__name__":
            labels, err := db.Client.ShowTables(*api.db)
            if err != nil {
                log.Printf("[error] %v", err)
                w.WriteHeader(500)
                return
            }
            w.WriteHeader(200)
            w.Write(encodeResp(&Resp{Status:"success", Data:labels}))
            return
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make([]int, 0)}))
}

func (api *Prom) ApiQuery(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

	result := ResultType{
        ResultType: "vector",
        Result: []Result{},
    }

    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
}

func (api *Prom) ApiQueryRange(w http.ResponseWriter, r *http.Request) {
	//log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")

    result := ResultType{
        ResultType: "matrix",
        Result: []Result{},
    }

    /*
    query := "sum(rate(http_requests_total{job=\"webserver\"}[5m])) * sum(rate(http_requests_total{job=\"webserver\"}[5m]))"

	//parser := promql.NewParser(strings.NewReader(query))

	expr, err := parser.ParseExpr(query)
	if err != nil {
		fmt.Printf("Failed to parse expression: %s\n", err)
		return
	}

	//ast := parser.ExtractSelectors(expr)

    ast := expr.PromQLExpr()

	fmt.Println(ast)
    */

    query := "avg(sum(rate(http_requests_total:test[3m])) by (status)) * sum(rate(http_requests_total[4m]) + sum(rate(http_requests_total[4m]))) by (status) / sum(rate(http_requests_total[5m])) by (status)"
	//parser := promql.NewParser(promql.ParserOptions{})
	tree, err := parser.ParseExpr(query)
	if err != nil {
		fmt.Printf("Error parsing query: %s\n", err.Error())
		return
	}

    //parser.Walk(tree, func(node parser.Node, _ *parser.Scanner) error {
	//	fmt.Printf("Node Type: %T\n", node)
	//	return nil
	//})

    promQuery := Walk(PromQuery{}, tree)

    var jsonData []byte
    jsonData, err = json.Marshal(promQuery)
    if err != nil {
        log.Println(err)
    }
    fmt.Println(string(jsonData))

	//vectorNode, ok := node.(parser.VectorNode)
	//if !ok {
	//	fmt.Println("Query does not return a vector")
	//	return
	//}

	//fmt.Printf("Tree: %v\n", parser.Children(node))
	//fmt.Printf("Result type: %s\n", vectorNode.ReturnType())

    //values := []interface{}{
    //    float64(time.Now().Unix()-5), 
    //    "1",
    //}
    //values = append(values1, float64(time.Now().Unix()-5)+0.666)
    //values = append(values1, "1")

    //values2 := []interface{}{}
    //values2 = append(values2, float64(time.Now().Unix())+0.666)
    //values2 = append(values2, "2")

    /*
    metric := Result{
        Metric: map[string]string{"__name__": "kqi_base:count"},
        Value: []interface{}{float64(time.Now().Unix()), "1"},
    }
    */

    metric := Result{
        Metric: map[string]string{"__name__": "kqi_base:count","test":"test"},
        Values: []interface{}{
            []interface{}{float64(time.Now().Unix()-60), "1"},
            []interface{}{float64(time.Now().Unix()-45), "1"},
            []interface{}{float64(time.Now().Unix()-30), "1"},
            []interface{}{float64(time.Now().Unix()-15), "2"},
        },
    }

    result.Result = append(result.Result, metric)

    //1687859205
    //1435781430.781

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
    w.Write(encodeResp(&Resp{Status:"success", Data:result}))
}

func (api *Prom) ApiQueryExemplars(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
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
	log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make([]int, 0)}))
}

func (api *Prom) ApiMetadata(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make([]int, 0)}))
}

func (api *Prom) ApiRules(w http.ResponseWriter, r *http.Request) {
	log.Printf(r.URL.Path)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(200)
    w.Write(encodeResp(&Resp{Status:"success", Data:make([]int, 0)}))
}