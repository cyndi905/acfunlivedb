// 爬虫相关
package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/valyala/fasthttp"
)

type httpClient struct {
	client      *fasthttp.Client
	url         string
	body        []byte
	method      string
	cookies     []*fasthttp.Cookie
	userAgent   string
	contentType string
	referer     string
}

var defaultClient = &fasthttp.Client{
	MaxIdleConnDuration: 90 * time.Second,
	ReadTimeout:         10 * time.Second,
	WriteTimeout:        10 * time.Second,
}

// http 请求，调用后需要 defer fasthttp.ReleaseResponse(resp)
func (c *httpClient) doRequest() (resp *fasthttp.Response, e error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovering from panic in doRequest(), the error is: %v", err)
			e = fmt.Errorf("请求 %s 时出错，错误为 %v", c.url, err)
			fasthttp.ReleaseResponse(resp)
		}
	}()

	if c.client == nil {
		c.client = defaultClient
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp = fasthttp.AcquireResponse()

	if c.url != "" {
		req.SetRequestURI(c.url)
	} else {
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("请求的 url 不能为空")
	}

	if len(c.body) != 0 {
		req.SetBody(c.body)
	}

	if c.method != "" {
		req.Header.SetMethod(c.method)
	} else {
		// 默认为 GET
		req.Header.SetMethod(fasthttp.MethodGet)
	}

	if len(c.cookies) != 0 {
		for _, cookie := range c.cookies {
			req.Header.SetCookieBytesKV(cookie.Key(), cookie.Value())
		}
	}

	const userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

	if c.userAgent != "" {
		req.Header.SetUserAgent(c.userAgent)
	} else {
		req.Header.SetUserAgent(userAgent)
	}

	if c.contentType != "" {
		req.Header.SetContentType(c.contentType)
	}

	if c.referer != "" {
		req.Header.SetReferer(c.referer)
	}

	req.Header.SetCookie("_did", deviceID)

	req.Header.Set("Accept-Encoding", "gzip")

	err := c.client.Do(req, resp)
	checkErr(err)

	return resp, nil
}

// 获取响应 body
func getBody(resp *fasthttp.Response) []byte {
	if string(resp.Header.Peek("content-encoding")) == "gzip" || string(resp.Header.Peek("Content-Encoding")) == "gzip" {
		body, err := resp.BodyGunzip()
		if err == nil {
			return body
		}
	}

	return resp.Body()
}

// IsLiveOnByPage 通过 wap 版网页查看主播是否在直播
func IsLiveOnByPage(uid int) (isLive bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Recovering from panic in isLiveOnByPage(), the error is:%v", err)
			log.Printf("获取 %d 的直播页面时出错", uid)
		}
	}()

	const acLivePage = "https://m.acfun.cn/live/detail/%d"
	const mobileUserAgent = "Mozilla/5.0 (iPad; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"

	client := &httpClient{
		url:       fmt.Sprintf(acLivePage, uid),
		method:    fasthttp.MethodGet,
		userAgent: mobileUserAgent,
	}
	resp, err := client.doRequest()
	checkErr(err)
	defer fasthttp.ReleaseResponse(resp)
	body := getBody(resp)

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	checkErr(err)
	return doc.Find("p.closed-tip").Text() != "直播已结束"
}
