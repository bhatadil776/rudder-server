package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"
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

	httpSrv := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get the routing key from the HTTP header
			routingKey := r.Header.Get("X-RoutingKey")

			// Hash the routing key to determine the node
			hash := sha256.Sum256([]byte(routingKey))
			nodeNumber := int(hash[0]) % noOfNodes

			// Create the proxy URL
			proxyURL := &url.URL{
				Scheme: "http",
				Host: fmt.Sprintf(
					"gwpulsar-statefulset-%d.gwpulsar-service.my-namespace.svc.cluster.local",
					nodeNumber,
				),
			}

			// Create the reverse proxy
			proxy := httputil.NewSingleHostReverseProxy(proxyURL)

			// Update the headers to allow for SSL redirection
			r.URL.Host = proxyURL.Host
			r.URL.Scheme = proxyURL.Scheme
			r.Header.Set("X-Forwarded-Host", r.Header.Get("Host"))
			r.Host = proxyURL.Host

			// Note that ServeHttp is non-blocking and uses a go routine under the hood
			proxy.ServeHTTP(w, r)
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
