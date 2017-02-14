package riakproxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

//Interceptor http response interceptor
type Interceptor func(request *http.Request, response *http.Response)

type transport struct {
	http.RoundTripper
	intercept Interceptor
}

func (t *transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	resp, err = t.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	t.intercept(req, resp)

	return resp, nil
}

//GetProxy creates instance of interceptable proxy
func GetProxy(target *url.URL, interceptor Interceptor) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Header.Set("Accept", "*/*")
	}
	proxy := &httputil.ReverseProxy{Director: director}
	proxy.Transport = &transport{http.DefaultTransport, interceptor}

	return proxy
}
