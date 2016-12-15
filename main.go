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

const redisAddr = "localhost:6379"
const rianEndpoint = "http://localhost:8098"

const keyPrefix = "riak_cache:"

var client = redis.NewClient(&redis.Options{
	Addr: redisAddr,
})

var riakURL, _ = url.Parse(rianEndpoint)
var riakCacheableProxy = riakproxy.GetProxy(riakURL, cacheToRedis)
var riakDummyProxy = riakproxy.GetProxy(riakURL, passForward)
var cacheInvalidatorProxy = riakproxy.GetProxy(riakURL, invalidateCache)

func main() {
	http.HandleFunc("/", httpHandler)
	log.Fatal(http.ListenAndServe(":8400", nil))
}

func httpHandler(w http.ResponseWriter, request *http.Request) {
	fmt.Printf("%s %s\n", request.Method, request.URL.Path)

	//TODO: abstract logic for proxy selection

	if request.Method == http.MethodHead {
		//TODO: use cache
		riakDummyProxy.ServeHTTP(w, request)
	} else if request.Method == http.MethodGet {
		//TODO: wrap cache checking to proxy

		//Check cache
		cached := checkCache(request)

		if len(cached) == 0 {
			//If no cache forward request to Riak
			riakCacheableProxy.ServeHTTP(w, request)
		} else {
			//Response with cached result
			writeDump(cached, true, w)
		}
	} else if request.Method == http.MethodPost || request.Method == http.MethodPut || request.Method == http.MethodDelete {
		//Forward to Riak and invalidate cache
		cacheInvalidatorProxy.ServeHTTP(w, request)
	} else {
		w.WriteHeader(405)
		w.Write([]byte("Method not allowed"))
	}
}

func checkCache(request *http.Request) string {
	bucket, key := parsePath(request.URL.Path)

	//fmt.Printf("Check cahce for /%s/%s\n", bucket, key)

	value, err := client.HGet(keyPrefix+bucket, key).Result()

	if err == redis.Nil {
		//fmt.Printf("%s does not exists\n", key)
		return ""
	} else if err != nil {
		panic(err)
	} else {
		fmt.Printf("From cache /%s/%s\n", bucket, key)
		return value
	}
}

func cacheToRedis(request *http.Request, response *http.Response) {
	if response.StatusCode != 200 {
		return
	}

	bucket, key := parsePath(request.URL.Path)

	dump, err := httputil.DumpResponse(response, true)
	if err != nil {
		return
	}

	value := string(dump[:])

	//fmt.Printf("Save cache for /%s/%s: %v\n", bucket, key, value)

	err = client.HSet(keyPrefix+bucket, key, value).Err()

	if err != nil {
		panic(err)
	} else {
		fmt.Printf("Cached /%s/%s\n", bucket, key)
	}
}

func invalidateCache(request *http.Request, response *http.Response) {
	bucket, key := parsePath(request.URL.Path)

	err := client.HDel(keyPrefix+bucket, key).Err()

	if err != nil {
		panic(err)
	} else {
		fmt.Printf("Invalidated cache /%s/%s\n", bucket, key)
	}
}

func passForward(request *http.Request, response *http.Response) {
	//Do nothing
}

func parsePath(path string) (string, string) {
	segments := strings.Split(path, "/")

	if len(segments) < 4 || segments[1] != "riak" {
		fmt.Printf("Non-cacheable request path: %v\n", path)
		return "", ""
	}

	return segments[2], segments[3]
}

func writeDump(dump string, writeBody bool, w http.ResponseWriter) {
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
			if writeBody {
				bodyStarted = true
				continue
			} else {
				break
			}
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
