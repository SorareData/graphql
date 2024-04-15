package graphql

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Inspired by https://medium.com/@kdthedeveloper/golang-http-retries-fbf7abacbe27

const RetryCount = 5

func NewRetryableClient(logger func(s string), defaultWaitAfterTooManyRequests time.Duration) *http.Client {
	transport := &retryableTransport{
		transport:                       &http.Transport{},
		defaultWaitAfterTooManyRequests: defaultWaitAfterTooManyRequests,
		logger:                          logger,
	}

	return &http.Client{
		Transport: transport,
	}
}

type retryableTransport struct {
	transport                       http.RoundTripper
	defaultWaitAfterTooManyRequests time.Duration
	logger                          func(s string)
}

func (t *retryableTransport) shouldRetry(err error, resp *http.Response) (time.Duration, bool) {
	if err != nil {
		return 0, false // Don't retry on pure technical error
	}

	if resp.StatusCode == http.StatusBadGateway ||
		resp.StatusCode == http.StatusServiceUnavailable ||
		resp.StatusCode == http.StatusGatewayTimeout {
		return 250 * time.Millisecond, true
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		waitTimeInSecs, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
		waitTimeDuration := time.Duration(waitTimeInSecs) * time.Second
		if waitTimeInSecs == 0 {
			// Some Sorare APIs don't return Retry-After header (US sports)
			// So wait for the time specified in the config
			waitTimeDuration = t.defaultWaitAfterTooManyRequests
		}
		return waitTimeDuration, true
	}
	return 0, false
}

func (t *retryableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request body
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}
	// Send the request
	resp, err := t.transport.RoundTrip(req)
	// Retry logic
	retries := 0
	for retries < RetryCount {
		timeToWait, toRetry := t.shouldRetry(err, resp)
		if !toRetry {
			break
		}
		if timeToWait > 0 {
			t.logger(fmt.Sprintf("server returned %d, retrying after %s", resp.StatusCode, timeToWait))
			time.Sleep(timeToWait)
		} else {
			t.logger(fmt.Sprintf("server returned %d, retrying right now", resp.StatusCode))
		}
		// We're going to retry, consume any response to reuse the connection.
		drainBody(resp)
		// Clone the request body again
		if req.Body != nil {
			req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}
		// Retry the request
		resp, err = t.transport.RoundTrip(req)
		retries++
	}
	if retries >= RetryCount {
		if err != nil {
			return resp, fmt.Errorf("retry limit reached (err=%s)", err.Error())
		}
		return resp, fmt.Errorf("retry limit reached")
	}
	// Return the response
	return resp, err
}

func drainBody(resp *http.Response) {
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
