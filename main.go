package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/riak-redis-cache/proxy"

	redis "gopkg.in/redis.v5"
)

const keyPrefix = "riak_cache:"

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

var riakURL, err = url.Parse("http://localhost:8098")
var riakProxy = riakproxy.GetProxy(riakURL, cacheToRedis)

func main() {
	http.HandleFunc("/", httpHandler)
	log.Fatal(http.ListenAndServe(":8198", nil))
}

func httpHandler(w http.ResponseWriter, request *http.Request) {
	fmt.Printf("%s %s\n", request.Method, request.URL.Path)

	if request.Method == "GET" {
		//Check cache
		cached := checkCache(request)

		//If no cache forward request to Riak
		if len(cached) == 0 {
			riakProxy.ServeHTTP(w, request)
		} else {
			fmt.Printf("Cached response %d\n", len(cached))
			writeDump(cached, w)
		}
	} else if request.Method == "POST" || request.Method == "PUT" {
		//riakProxy.ServeHTTP(w, request)
	} else {
		w.WriteHeader(405)
		w.Write([]byte("Method not allowed"))
	}
}

func checkCache(request *http.Request) string {
	bucket, key := parsePath(request.URL.Path)

	fmt.Printf("Check cahce for /%s/%s\n", bucket, key)

	value, err := client.HGet(keyPrefix+bucket, key).Result()

	if err == redis.Nil {
		fmt.Printf("%s does not exists\n", key)
		return ""
	} else if err != nil {
		panic(err)
	} else {
		fmt.Printf("Found /%s/%s: %s\n", bucket, key, value)
		return value
	}
}

func cacheToRedis(request *http.Request, response *http.Response) {
	bucket, key := parsePath(request.URL.Path)

	dump, err := httputil.DumpResponse(response, true)
	if err != nil {
		return
	}

	value := string(dump[:])

	fmt.Printf("Save cache for /%s/%s: %v\n", bucket, key, value)

	err = client.HSet(keyPrefix+bucket, key, value).Err()

	if err != nil {
		panic(err)
	} else {
		fmt.Printf("Cached /%s/%s\n", bucket, key)
	}
}

func parsePath(path string) (string, string) {
	segments := strings.Split(path, "/")

	if len(segments) < 4 || segments[1] != "riak" {
		fmt.Printf("Non-cacheable request path: %v\n", path)
		return "", ""
	}

	return segments[2], segments[3]
}

func writeDump(dump string, w http.ResponseWriter) {
	parts := strings.Split(dump, "\r\n")

	var responseCode int
	var body []byte

	var bodyStarted bool

	for i, p := range parts {
		if i == 0 {
			//TODO: response code parsing
			responseCode = 200
			continue
		}

		if p == "" {
			bodyStarted = true
			continue
		}

		if !bodyStarted {
			keyValues := strings.Split(p, ": ")
			w.Header().Add(keyValues[0], keyValues[1])
		} else {
			body = []byte(p)
		}
	}

	w.WriteHeader(responseCode)
	w.Write(body)
}
