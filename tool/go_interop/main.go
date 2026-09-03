package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const operationTimeout = 3 * time.Second

func main() {
	url := flag.String("url", nats.DefaultURL, "NATS server URL")
	prefix := flag.String("prefix", "", "Unique interoperability subject prefix")
	streamName := flag.String("stream", "", "JetStream stream name")
	keyValueBucket := flag.String("kv", "", "Key Value bucket name")
	objectStoreBucket := flag.String("object", "", "Object Store bucket name")
	flag.Parse()

	if *prefix == "" || *streamName == "" || *keyValueBucket == "" || *objectStoreBucket == "" {
		fmt.Fprintln(os.Stderr, "prefix, stream, kv, and object are required")
		os.Exit(2)
	}

	connection, err := nats.Connect(
		*url,
		nats.Name("nats.go interoperability fixture"),
		nats.Timeout(operationTimeout),
	)
	if err != nil {
		fail(err)
	}
	defer connection.Close()

	js, err := jetstream.New(connection)
	if err != nil {
		fail(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	keyValue, err := js.KeyValue(ctx, *keyValueBucket)
	cancel()
	if err != nil {
		fail(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), operationTimeout)
	objectStore, err := js.ObjectStore(ctx, *objectStoreBucket)
	cancel()
	if err != nil {
		fail(err)
	}

	subscribe(connection, *prefix+".core.request", func(message *nats.Msg) error {
		response := append([]byte("go:"), message.Data...)
		return message.Respond(response)
	})
	subscribe(connection, *prefix+".core.to_go", func(message *nats.Msg) error {
		return connection.Publish(*prefix+".core.observed_by_go", message.Data)
	})
	subscribe(connection, *prefix+".command.publish", func(message *nats.Msg) error {
		if err := connection.Publish(*prefix+".core.from_go", message.Data); err != nil {
			return err
		}
		if err := connection.FlushTimeout(operationTimeout); err != nil {
			return err
		}
		return message.Respond([]byte("OK"))
	})
	subscribe(connection, *prefix+".command.request_dart", func(message *nats.Msg) error {
		response, err := connection.Request(
			*prefix+".dart.request",
			message.Data,
			operationTimeout,
		)
		if err != nil {
			return err
		}
		return message.Respond(response.Data)
	})
	subscribe(connection, *prefix+".command.js_get", func(message *nats.Msg) error {
		sequence, err := strconv.ParseUint(string(message.Data), 10, 64)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		stream, err := js.Stream(ctx, *streamName)
		if err != nil {
			return err
		}
		stored, err := stream.GetMsg(ctx, sequence)
		if err != nil {
			return err
		}
		return message.Respond(stored.Data)
	})
	subscribe(connection, *prefix+".command.js_publish", func(message *nats.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		acknowledgement, err := js.Publish(
			ctx,
			*prefix+".js.from_go",
			message.Data,
		)
		if err != nil {
			return err
		}
		return message.Respond([]byte(strconv.FormatUint(acknowledgement.Sequence, 10)))
	})
	subscribe(connection, *prefix+".command.kv_get", func(message *nats.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		entry, err := keyValue.Get(ctx, "dart")
		if err != nil {
			return err
		}
		return message.Respond(entry.Value())
	})
	subscribe(connection, *prefix+".command.kv_put", func(message *nats.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		revision, err := keyValue.Put(ctx, "go", message.Data)
		if err != nil {
			return err
		}
		return message.Respond([]byte(strconv.FormatUint(revision, 10)))
	})
	subscribe(connection, *prefix+".command.object_get", func(message *nats.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		value, err := objectStore.GetBytes(ctx, "dart")
		if err != nil {
			return err
		}
		return message.Respond(value)
	})
	subscribe(connection, *prefix+".command.object_put", func(message *nats.Msg) error {
		ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
		defer cancel()
		info, err := objectStore.PutBytes(ctx, "go", message.Data)
		if err != nil {
			return err
		}
		return message.Respond([]byte(info.Digest))
	})

	if err := connection.FlushTimeout(operationTimeout); err != nil {
		fail(err)
	}
	if err := connection.LastError(); err != nil {
		fail(err)
	}
	fmt.Println("READY")

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
}

func subscribe(
	connection *nats.Conn,
	subject string,
	handler func(*nats.Msg) error,
) {
	_, err := connection.Subscribe(subject, func(message *nats.Msg) {
		if err := handler(message); err != nil {
			_ = message.Respond([]byte("ERROR: " + err.Error()))
		}
	})
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
