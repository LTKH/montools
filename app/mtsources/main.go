package main

import (
    "log"
    //"fmt"
    "flag"
	"time"
    "net/http"
	"github.com/gorilla/mux"
	"github.com/ltkh/montools/internal/config"
    "github.com/ltkh/montools/internal/api/prom"
	//"github.com/ltkh/montools/internal/api/loki"
)

func getFuncExpr() {

}

func main() {
    // Command-line flag parsing
    cfFile := flag.String("config.file", "config/mtsources.yml", "config file")
    flag.Parse()

	// Loading configuration file
    cfg, err := config.NewMTSources(*cfFile)
    if err != nil {
        log.Fatalf("[error] %v", err)
    }

    for _, upstream := range cfg.Upstreams {
		switch upstream.Type {
			case "prometheus":
				// Creating Prom API
				apiProm, err := prom.New(upstream)
				if err != nil {
					log.Fatalf("[error] %v", err)
				}
				rtr := mux.NewRouter()
				rtr.HandleFunc("/api/v1/labels", apiProm.ApiLabels)
				rtr.HandleFunc("/api/v1/label/{name:[^/]+}/values", apiProm.ApiLabelValues)
				rtr.HandleFunc("/api/v1/query", apiProm.ApiQuery)
				rtr.HandleFunc("/api/v1/query_range", apiProm.ApiQueryRange)
				rtr.HandleFunc("/api/v1/query_exemplars", apiProm.ApiQueryExemplars)
				rtr.HandleFunc("/api/v1/series", apiProm.ApiSeries)
				rtr.HandleFunc("/api/v1/metadata", apiProm.ApiMetadata)
				rtr.HandleFunc("/api/v1/rules", apiProm.ApiRules)
				http.Handle("/", rtr)
				go func(){
					err = http.ListenAndServe(upstream.ListenAddr, nil)
					if err != nil {
						log.Fatalf("[error] %v", err)
					}
				}()
			case "loki":
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

	// Daemon mode
    for {
        time.Sleep(600 * time.Second)
    }

}