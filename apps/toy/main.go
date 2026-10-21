// Toy is a tiny HTTP dependency graph for real Kubernetes validation.
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ready") })
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintln(w, os.Getenv("SERVICE_NAME"))
	})
	http.HandleFunc("/journey", func(w http.ResponseWriter, r *http.Request) {
		if dependency := os.Getenv("DEPENDENCY"); dependency != "" {
			client := &http.Client{Timeout: time.Second}
			response, err := client.Get(dependency)
			if err != nil {
				http.Error(w, "dependency unavailable", 503)
				return
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				http.Error(w, "dependency failed", 503)
				return
			}
		}
		fmt.Fprintln(w, "journey ok")
	})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
