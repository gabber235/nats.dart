package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const operationTimeout = 5 * time.Second

var httpClient = &http.Client{Timeout: 250 * time.Millisecond}

type fixture struct {
	binary       string
	originConfig string
	processes    map[string]*exec.Cmd
	mu           sync.Mutex
}

func main() {
	binary := flag.String("server", "", "NATS server binary")
	configDirectory := flag.String("configs", "", "NATS configuration directory")
	flag.Parse()
	if *binary == "" || *configDirectory == "" {
		fail(errors.New("server and configs are required"))
	}

	f := &fixture{
		binary:       *binary,
		originConfig: filepath.Join(*configDirectory, "origin.conf"),
		processes:    map[string]*exec.Cmd{},
	}
	defer f.stopAll()
	for _, name := range []string{"a", "b", "origin", "certificate"} {
		if err := f.start(name, filepath.Join(*configDirectory, name+".conf")); err != nil {
			fail(err)
		}
	}

	first := connect("nats://127.0.0.1:4222")
	defer first.Close()
	second := connect("nats://127.0.0.1:4223")
	defer second.Close()
	originProbe := connect("nats://127.0.0.1:4224")
	originProbe.Close()
	awaitRoute("http://127.0.0.1:8222/routez")
	installInterop(second)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)
	mux.HandleFunc("/stop-a", f.stopFirst)
	mux.HandleFunc("/origin", f.setOrigin)
	server := &http.Server{
		Addr:              ":8090",
		Handler:           cors(mux),
		ReadHeaderTimeout: operationTimeout,
		ReadTimeout:       operationTimeout,
		WriteTimeout:      operationTimeout,
		IdleTimeout:       operationTimeout,
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-shutdown:
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			fail(err)
		}
	}
	_ = server.Close()
}

func (f *fixture) start(name, config string) error {
	command := exec.Command(f.binary, "-c", config)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return fmt.Errorf("start NATS server %s: %w", name, err)
	}
	f.processes[name] = command
	return nil
}

func (f *fixture) stopFirst(response http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	command := f.processes["a"]
	if command == nil {
		http.Error(response, "first server already stopped", http.StatusConflict)
		return
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := awaitProcess(command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	delete(f.processes, "a")
	response.WriteHeader(http.StatusNoContent)
}

func (f *fixture) setOrigin(response http.ResponseWriter, request *http.Request) {
	origin := request.URL.Query().Get("value")
	parsed, err := url.ParseRequestURI(origin)
	if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" {
		http.Error(response, "invalid origin", http.StatusBadRequest)
		return
	}
	if strings.ContainsAny(origin, "\r\n\"") {
		http.Error(response, "invalid origin", http.StatusBadRequest)
		return
	}

	config := fmt.Sprintf(`
port: 4224
http_port: 8224
websocket {
  port: 8082
  no_tls: true
  allowed_origins: ["%s"]
}
`, origin)
	f.mu.Lock()
	defer f.mu.Unlock()
	command := f.processes["origin"]
	if command == nil {
		http.Error(response, "origin server is not running", http.StatusConflict)
		return
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := awaitProcess(command); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	delete(f.processes, "origin")
	if err := os.WriteFile(f.originConfig, []byte(config), 0o600); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := f.start("origin", f.originConfig); err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
	probe, err := awaitConnection("nats://127.0.0.1:4224")
	if err != nil {
		http.Error(response, err.Error(), http.StatusGatewayTimeout)
		return
	}
	probe.Close()
	response.WriteHeader(http.StatusNoContent)
}

func installInterop(connection *nats.Conn) {
	subscribe(connection, "browser.go.request", func(message *nats.Msg) error {
		values := headerValues(message.Header, "X-From-Dart")
		if len(values) != 2 || !contains(values, "dart-one") || !contains(values, "dart-two") {
			return fmt.Errorf("unexpected repeated Dart headers: %v", values)
		}
		response := nats.NewMsg(message.Reply)
		response.Data = append([]byte("go:"), message.Data...)
		response.Header.Add("Traceparent", "first")
		response.Header.Add("Traceparent", "second")
		return message.RespondMsg(response)
	})
	subscribe(connection, "browser.go.publish", func(message *nats.Msg) error {
		publication := nats.NewMsg("browser.dart.receive")
		publication.Data = append([]byte(nil), message.Data...)
		publication.Header.Add("X-Interop", "go-one")
		publication.Header.Add("X-Interop", "go-two")
		if err := connection.PublishMsg(publication); err != nil {
			return err
		}
		if err := connection.FlushTimeout(operationTimeout); err != nil {
			return err
		}
		return message.Respond([]byte("OK"))
	})
	subscribe(connection, "browser.go.ready", func(message *nats.Msg) error {
		return message.Respond([]byte("READY"))
	})
	if err := connection.FlushTimeout(operationTimeout); err != nil {
		fail(err)
	}
}

func subscribe(connection *nats.Conn, subject string, handler func(*nats.Msg) error) {
	if _, err := connection.Subscribe(subject, func(message *nats.Msg) {
		if err := handler(message); err != nil {
			_ = message.Respond([]byte("ERROR: " + err.Error()))
		}
	}); err != nil {
		fail(err)
	}
}

func connect(server string) *nats.Conn {
	connection, err := awaitConnection(server)
	if err != nil {
		fail(err)
	}
	return connection
}

func awaitConnection(server string) (*nats.Conn, error) {
	deadline := time.Now().Add(operationTimeout)
	var lastError error
	for time.Now().Before(deadline) {
		connection, err := nats.Connect(
			server,
			nats.Name("nats.go browser fixture"),
			nats.Timeout(250*time.Millisecond),
			nats.NoReconnect(),
		)
		if err == nil {
			return connection, nil
		}
		lastError = err
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("connect %s: %w", server, lastError)
}

func awaitRoute(endpoint string) {
	deadline := time.Now().Add(operationTimeout)
	for time.Now().Before(deadline) {
		response, err := httpClient.Get(endpoint)
		if err == nil {
			var body struct {
				Routes int `json:"num_routes"`
			}
			decodeError := json.NewDecoder(response.Body).Decode(&body)
			_ = response.Body.Close()
			if decodeError == nil && body.Routes > 0 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	fail(errors.New("NATS cluster route was not ready"))
}

func health(response http.ResponseWriter, _ *http.Request) {
	response.WriteHeader(http.StatusNoContent)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(response, request)
	})
}

func awaitProcess(command *exec.Cmd) error {
	done := make(chan error, 1)
	go func() {
		done <- command.Wait()
	}()
	select {
	case err := <-done:
		if err == nil || expectedTermination(err) {
			return nil
		}
		return err
	case <-time.After(operationTimeout):
		return errors.New("NATS server shutdown timed out")
	}
}

func expectedTermination(err error) bool {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return false
	}
	status, ok := exitError.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGTERM
}

func headerValues(header nats.Header, name string) []string {
	values := []string{}
	for key, entries := range header {
		if strings.EqualFold(key, name) {
			values = append(values, entries...)
		}
	}
	return values
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func (f *fixture) stopAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, command := range f.processes {
		_ = command.Process.Signal(syscall.SIGTERM)
		_ = awaitProcess(command)
		delete(f.processes, name)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
