package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("Server error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	// Get the number of nodes from the environment variable
	noOfNodes, ok := readIntEnvVar("NO_OF_NODES")
	if !ok {
		return fmt.Errorf("required environment variable is not set or is invalid: NO_OF_NODES")
	}

	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 10 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConns:          20,
		MaxConnsPerHost:       20 / noOfNodes,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   30 * time.Second,
	}

	httpSrv := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get the routing key from the HTTP header
			routingKey := r.Header.Get("X-RoutingKey")
			r.Header.Set("X-RoutingKey", "")

			// Hash the routing key to determine the node
			hash := sha256.Sum256([]byte(routingKey))
			gwInstance := int(hash[0]) % noOfNodes

			// Create the reverse proxy
			r.URL = &url.URL{
				Scheme: "http",
				Host: "gwserver-statefulset-" + strconv.Itoa(gwInstance) +
					".gwserver-svc.fcasula.svc.cluster.local:8080",
			}
			r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
			r.RequestURI = ""

			var (
				err  error
				resp *http.Response
			)
			operation := func() error {
				resp, err = client.Do(r)
				defer closeResponse(resp)
				return err
			}
			backoffWithMaxRetry := backoff.WithContext(
				backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), ctx,
			)
			err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
				log.Printf("Failed to POST with error: %v, retrying after %v", err, t)
			})

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if resp.StatusCode != http.StatusOK {
				http.Error(w, "unexpected status code", http.StatusInternalServerError)
			}
		}),
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("Failed to shutdown HTTP server gracefully: %v", err)
		}
	}()

	err := httpSrv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func closeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		const maxBodySlurpSize = 2 << 10 // 2KB
		_, _ = io.CopyN(io.Discard, resp.Body, maxBodySlurpSize)
		_ = resp.Body.Close()
	}
}

func readIntEnvVar(name string) (int, bool) {
	rawValue, exists := os.LookupEnv(name)
	if !exists {
		return 0, false
	}

	value, err := strconv.Atoi(rawValue)
	if err != nil {
		return 0, false
	}

	return value, true
}
