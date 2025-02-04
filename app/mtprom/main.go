package main

import (
    "log"
    //"fmt"
    "flag"
	//"time"
    "net/http"
    "os"
    "os/signal"
    "syscall"
	"github.com/gorilla/mux"
    "github.com/ltkh/montools/internal/api/promql"
	"github.com/ltkh/montools/internal/config/mtprom"
)

func main() {
    // Command-line flag parsing
    cfFile := flag.String("config.file", "config/mtprom.yml", "config file")
    flag.Parse()

	// Loading configuration file
    cfg, err := config.New(*cfFile)
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    for _, upstream := range cfg.Upstreams {
        api, err := promql.New(upstream)
        if err != nil {
            log.Fatalf("[error] %v", err)
        }
        rtr := mux.NewRouter()
        rtr.HandleFunc("/api/v1/labels", api.ApiLabels)
        rtr.HandleFunc("/api/v1/label/{name:[^/]+}/values", api.ApiLabelValues)
        rtr.HandleFunc("/api/v1/query", api.ApiQuery)
        rtr.HandleFunc("/api/v1/query_range", api.ApiQueryRange)
        rtr.HandleFunc("/api/v1/query_exemplars", api.ApiQueryExemplars)
        rtr.HandleFunc("/api/v1/series", api.ApiSeries)
        rtr.HandleFunc("/api/v1/metadata", api.ApiMetadata)
        rtr.HandleFunc("/api/v1/rules", api.ApiRules)
        http.Handle("/", rtr)
        go func(){
            err = http.ListenAndServe(upstream.ListenAddr, nil)
            if err != nil {
                log.Fatalf("[error] %v", err)
            }
        }()
        //mux := http.NewServeMux()
        //mux.HandleFunc("/loki/api/v1/labels", apiLoki.ApiLabels)
        //mux.HandleFunc("/loki/api/v1/label/*/values", apiLoki.ApiLabelValues)
        //mux.HandleFunc("/loki/api/v1/query_range", apiLoki.ApiQueryRange)
        //mux.HandleFunc("/", apiLoki.ApiLoki)
        
        //go func(){
        //	err = http.ListenAndServe("127.0.0.1:3100", mux)
        //	if err != nil {
        //		log.Fatalf("[error] %v", err)
        //	}
        //}()
	}

    //rtr := mux.NewRouter()
	//rtr.HandleFunc("/api/v1/labels", apiProm.ApiPromLabels)
    //rtr.HandleFunc("/api/v1/label/{name:[^/]+}/values", apiProm.ApiPromLabelValues)
    //rtr.HandleFunc("/api/v1/query", apiProm.ApiPromQuery)
    //rtr.HandleFunc("/api/v1/query_range", apiProm.ApiPromQueryRange)
	

    /*
    test, err := parser.ParseMetricSelector(`dark_kqi.kqi_base_metrics:cnt{service=~"(Service|Self|ServiceTech|MegaLabs)",status!="error"}`)
    if err != nil {
        // parse error
        log.Printf("%v", err)
    }
    log.Printf("%v", test)
    */

	/*
    expr, err := metricsql.Parse(`{table="dark_kqi.kqi_base_metrics",service=~"(Service|Self|ServiceTech|MegaLabs)",status!="error"}`)
    if err != nil {
        // parse error
        log.Printf("%v", err)
    }
    ae, ok := expr.(*metricsql.AggrFuncExpr)
    if ok {
        // parse error
        fmt.Printf("aggr func: name=%s, arg=%s, modifier=%s\n", ae.Name, ae.Args[0].AppendString(nil), ae.Modifier.AppendString(nil))
        for _, arg := range ae.Args {
            log.Printf("%v", arg)
        }
    }

    me, ok := expr.(*metricsql.MetricExpr)
    if ok {
        log.Printf("me: %v", me.LabelFilters)
    }
	*/
    

    // Now expr contains parsed MetricsQL as `*Expr` structs.
    // See Parse examples for more details.
    //log.Printf("%v", expr)

    //http://localhost:3000/api/datasources/proxy/1/loki/api/v1/query_range?direction=BACKWARD&limit=1144&query=rate(%7Bapp%3D%22alertmanager%22%7D%5B1m%5D)&start=1687657950225000000&end=1687679550225000000&step=20

	log.Print("[info] mtprod started")

    // Program signal processing
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
    for {
        <-c
        log.Print("[info] mtprod stopped")
        os.Exit(0)
    }

}