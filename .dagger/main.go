package main

import (
	"context"
	"dagger/nats-dart/internal/dagger"
	"errors"
	"fmt"
)

const dartImage = "dart:3.13.2@sha256:1f86408456fbcdc5f9c33fa267d3680d86e79c255ec338b9a215459224769770"

const minimumDartImage = "dart:3.11.0@sha256:193a4d037dcef48b56f2a3544f053f4ef3b5e9865953f8f5ff2a284554da567a"

const browserImage = "selenium/standalone-chromium:4.46.0-20260707@sha256:3400b92f1cddb2dfaaf358654e8f7d83d7be45192fb73c5f28c25faa28d36504"

const flutterImage = "ghcr.io/gmeligio/flutter-linux:3.47.2@sha256:b0534f1e7a44f28a5c6e9aefe45a4310d37b1223c9db286946578dd92c7a9cdc"

const dartCacheKey = "dart-3.13.2-pub-v1"

const minimumDartCacheKey = "dart-3.11.0-pub-v1"

const browserDartCacheKey = "dart-browser-3.13.2-pub-v2"

const flutterPubCacheKey = "flutter-3.47.2-pub-v2"

const goModuleCacheKey = "go-1.26.0-nats-go-1.53.1-mod-v1"

const goBuildCacheKey = "go-1.26.0-nats-go-1.53.1-build-v1"

const externalDartConsumerPubspec = `name: nats_external_consumer
publish_to: none

environment:
  sdk: ">=3.11.0 <4.0.0"

dependencies:
  nats_core:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_core
  nats_jetstream:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_jetstream

dependency_overrides:
  nats_core:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_core
`

const externalFlutterConsumerPubspec = `name: nats_external_flutter_consumer
publish_to: none

environment:
  sdk: ">=3.11.0 <4.0.0"

dependencies:
  flutter:
    sdk: flutter
  nats_core:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_core
  nats_jetstream:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_jetstream

dependency_overrides:
  nats_core:
    git:
      url: file:///repo
      ref: main
      path: packages/nats_core

dev_dependencies:
  flutter_test:
    sdk: flutter
`

const latestNatsImage = "nats:2.14.6-alpine@sha256:ad7a43eb7e3337c3c38ce5d784d1461791f95f730f252d2b25eee699752a0ca3"

const compatibility212Image = "nats:2.12.15-alpine@sha256:b270f5e2428354c0335612694d7dd2fb588148e567a5757fdff325ef9c9332e6"

const compatibility210Image = "nats:2.10.29-alpine@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927"

const goBuilderImage = "golang:1.26.0-alpine@sha256:d4c4845f5d60c6a974c6000ce58ae079328d03ab7f721a0734277e69905473e5"

type NatsDart struct{}

func (m *NatsDart) sourceDirectory(source *dagger.Workspace) *dagger.Directory {
	return source.Directory("/", dagger.WorkspaceDirectoryOpts{
		Exclude:   []string{".git", ".dart_tool", "build", "coverage"},
		Gitignore: true,
	})
}

func (m *NatsDart) dartWithImage(
	source *dagger.Workspace,
	image string,
	cacheKey string,
) *dagger.Container {
	return dag.Container().
		From(image).
		WithDirectory("/workspace", m.sourceDirectory(source)).
		WithMountedCache(
			"/root/.pub-cache",
			dag.CacheVolume(cacheKey),
		).
		WithWorkdir("/workspace").
		WithExec([]string{"dart", "pub", "get", "--enforce-lockfile"})
}

func (m *NatsDart) dart(source *dagger.Workspace) *dagger.Container {
	return m.dartWithImage(source, dartImage, dartCacheKey)
}

func (m *NatsDart) gitFixture(
	container *dagger.Container,
) *dagger.Container {
	return container.
		WithWorkdir("/repo").
		WithExec([]string{"git", "init", "--quiet", "--initial-branch=main"}).
		WithExec([]string{"git", "config", "user.name", "NATS fixture"}).
		WithExec([]string{"git", "config", "user.email", "fixture@nats.invalid"}).
		WithExec([]string{"git", "add", "."}).
		WithExec([]string{"git", "commit", "--quiet", "--message", "fixture"})
}

// +check
func (m *NatsDart) ExternalGitConsumer(
	source *dagger.Workspace,
) *dagger.Container {
	return m.gitFixture(
		dag.Container().
			From(dartImage).
			WithDirectory("/repo", m.sourceDirectory(source)).
			WithMountedCache(
				"/root/.pub-cache",
				dag.CacheVolume(dartCacheKey),
			),
	).
		WithNewFile("/consumer/pubspec.yaml", externalDartConsumerPubspec).
		WithNewFile(
			"/consumer/bin/main.dart",
			"import \"package:nats_core/nats_core.dart\";\n"+
				"import \"package:nats_jetstream/nats_jetstream.dart\";\n\n"+
				"void main() {\n"+
				"  final server = NatsServer.nats(\"127.0.0.1\");\n"+
				"  final options = JetStreamPublishOptions(messageId: \"consumer\");\n"+
				"  print(\"${server.uri} ${options.messageId}\");\n"+
				"}\n",
		).
		WithWorkdir("/consumer").
		WithExec([]string{"dart", "pub", "get"}).
		WithExec([]string{"dart", "pub", "get", "--enforce-lockfile"}).
		WithExec([]string{"dart", "analyze", "--fatal-infos", "--fatal-warnings"}).
		WithExec([]string{"dart", "run", "bin/main.dart"})
}

// +check
func (m *NatsDart) ExternalFlutterGitConsumer(
	source *dagger.Workspace,
) *dagger.Container {
	const owner = "flutter:flutter"
	return m.gitFixture(
		dag.Container(dagger.ContainerOpts{Platform: "linux/amd64"}).
			From(flutterImage).
			WithDirectory(
				"/repo",
				m.sourceDirectory(source),
				dagger.ContainerWithDirectoryOpts{Owner: owner},
			).
			WithMountedCache(
				"/home/flutter/.pub-cache",
				dag.CacheVolume(flutterPubCacheKey),
				dagger.ContainerWithMountedCacheOpts{Owner: owner},
			),
	).
		WithDirectory(
			"/consumer",
			dag.Directory(),
			dagger.ContainerWithDirectoryOpts{Owner: owner},
		).
		WithNewFile(
			"/consumer/pubspec.yaml",
			externalFlutterConsumerPubspec,
			dagger.ContainerWithNewFileOpts{Owner: owner},
		).
		WithNewFile(
			"/consumer/test/packages_test.dart",
			"import \"package:flutter_test/flutter_test.dart\";\n"+
				"import \"package:nats_core/nats_core.dart\";\n"+
				"import \"package:nats_jetstream/nats_jetstream.dart\";\n\n"+
				"void main() {\n"+
				"  test(\"both packages resolve from one revision\", () {\n"+
				"    expect(NatsServer.nats(\"127.0.0.1\").uri.host, \"127.0.0.1\");\n"+
				"    expect(JetStreamPublishOptions().messageId, isNull);\n"+
				"  });\n"+
				"}\n",
			dagger.ContainerWithNewFileOpts{Owner: owner},
		).
		WithWorkdir("/consumer").
		WithExec([]string{"flutter", "pub", "get"}).
		WithExec([]string{"flutter", "pub", "get", "--enforce-lockfile"}).
		WithExec([]string{"flutter", "test"})
}

func (m *NatsDart) browserDart(source *dagger.Workspace) *dagger.Container {
	dartSdk := dag.Container().
		From(dartImage).
		Directory("/usr/lib/dart")

	return dag.Container().
		From(browserImage).
		WithUser("root").
		WithDirectory("/usr/lib/dart", dartSdk).
		WithEnvVariable(
			"PATH",
			"/usr/lib/dart/bin:$PATH",
			dagger.ContainerWithEnvVariableOpts{Expand: true},
		).
		WithNewFile(
			"/usr/local/bin/chrome-for-test",
			"#!/bin/sh\n"+
				"exec /usr/bin/chromium --no-sandbox "+
				"--ignore-certificate-errors-spki-list="+
				"NFHRuqZ4rfTYLEf+whQf6rGu9tMDZtjZqT3F8wLMT64= \"$@\"\n",
			dagger.ContainerWithNewFileOpts{Permissions: 0o755},
		).
		WithDirectory("/workspace", m.sourceDirectory(source)).
		WithMountedCache(
			"/root/.pub-cache",
			dag.CacheVolume(browserDartCacheKey),
		).
		WithWorkdir("/workspace").
		WithExec([]string{"dart", "pub", "get", "--enforce-lockfile"})
}

func (m *NatsDart) runIntegrationLane(
	container *dagger.Container,
	lane string,
) *dagger.Container {
	return container.WithExec([]string{
		"dart", "run", "tool/run_integration_lane.dart", lane,
	})
}

type natsReadinessOptions struct {
	url             string
	monitorURL      string
	jetStream       bool
	tlsFirst        bool
	ca              string
	clientCert      string
	clientKey       string
	credentialsFile string
	jwtFile         string
	username        string
	password        string
	token           string
}

func (m *NatsDart) withNatsService(
	container *dagger.Container,
	alias string,
	service *dagger.Service,
	options natsReadinessOptions,
) *dagger.Container {
	ready := container.
		WithServiceBinding(alias, service).
		WithEnvVariable("NATS_READINESS_URL", options.url).
		WithEnvVariable("NATS_READINESS_MONITOR_URL", options.monitorURL).
		WithEnvVariable(
			"NATS_READINESS_JETSTREAM",
			fmt.Sprintf("%t", options.jetStream),
		).
		WithEnvVariable(
			"NATS_READINESS_TLS_FIRST",
			fmt.Sprintf("%t", options.tlsFirst),
		)
	optionalEnvironment := []struct {
		name  string
		value string
	}{
		{"NATS_READINESS_CA", options.ca},
		{"NATS_READINESS_CLIENT_CERTIFICATE", options.clientCert},
		{"NATS_READINESS_CLIENT_KEY", options.clientKey},
		{"NATS_READINESS_CREDENTIALS_FILE", options.credentialsFile},
		{"NATS_READINESS_JWT_FILE", options.jwtFile},
		{"NATS_READINESS_USERNAME", options.username},
	}
	for _, environment := range optionalEnvironment {
		if environment.value != "" {
			ready = ready.WithEnvVariable(environment.name, environment.value)
		}
	}
	if options.password != "" {
		ready = ready.WithSecretVariable(
			"NATS_READINESS_PASSWORD",
			dag.SetSecret("nats-readiness-password", options.password),
		)
	}
	if options.token != "" {
		ready = ready.WithSecretVariable(
			"NATS_READINESS_TOKEN",
			dag.SetSecret("nats-readiness-token", options.token),
		)
	}
	return ready.WithExec([]string{
		"dart", "run", "tool/nats_service_readiness.dart",
	})
}

// +check
func (m *NatsDart) Format(source *dagger.Workspace) *dagger.Container {
	return m.dart(source).WithExec([]string{
		"dart",
		"format",
		"--output=none",
		"--set-exit-if-changed",
		"packages",
		"test",
		"tool",
	})
}

// +check
func (m *NatsDart) Analyze(source *dagger.Workspace) *dagger.Container {
	return m.dart(source).WithExec([]string{
		"dart",
		"analyze",
		"--fatal-infos",
		"--fatal-warnings",
	})
}

// +check
func (m *NatsDart) ApiDocs(source *dagger.Workspace) *dagger.Container {
	return m.dart(source).
		WithExec([]string{
			"dart", "doc", "--output", "/tmp/nats-core-doc", "packages/nats_core",
		}).
		WithExec([]string{
			"dart", "doc", "--output", "/tmp/nats-jetstream-doc", "packages/nats_jetstream",
		})
}

// +check
func (m *NatsDart) MinimumSdk(source *dagger.Workspace) *dagger.Container {
	return m.dartWithImage(
		source,
		minimumDartImage,
		minimumDartCacheKey,
	).WithExec([]string{
		"dart",
		"run",
		"tool/run_unit_tests.dart",
	})
}

func (m *NatsDart) flutterConsumer(source *dagger.Workspace) *dagger.Container {
	consumer := source.Directory("tool/flutter_consumer").
		WithFile(
			"compatibility.json",
			source.Directory("tool").File("compatibility.json"),
		).
		WithDirectory(
			"packages/nats_core",
			source.Directory("packages/nats_core"),
		).
		WithDirectory(
			"packages/nats_jetstream",
			source.Directory("packages/nats_jetstream"),
		)

	return dag.Container(dagger.ContainerOpts{Platform: "linux/amd64"}).
		From(flutterImage).
		WithDirectory(
			"/consumer",
			consumer,
			dagger.ContainerWithDirectoryOpts{Owner: "flutter:flutter"},
		).
		WithMountedCache(
			"/home/flutter/.pub-cache",
			dag.CacheVolume(flutterPubCacheKey),
			dagger.ContainerWithMountedCacheOpts{Owner: "flutter:flutter"},
		).
		WithWorkdir("/consumer")
}

// +check
func (m *NatsDart) FlutterConsumer(source *dagger.Workspace) *dagger.Container {
	return m.flutterConsumer(source).
		WithExec([]string{"dart", "verify_consumer.dart"})
}

func (m *NatsDart) FlutterConsumerLock(source *dagger.Workspace) *dagger.File {
	return m.flutterConsumer(source).
		WithExec([]string{"dart", "verify_consumer.dart", "--lock-only"}).
		File("/consumer/pubspec.lock")
}

// +check
func (m *NatsDart) CompatibilityManifest(source *dagger.Workspace) *dagger.Container {
	return m.dart(source).WithExec([]string{
		"dart",
		"run",
		"tool/verify_compatibility.dart",
	})
}

// +check
func (m *NatsDart) Unit(source *dagger.Workspace) *dagger.Container {
	return m.dart(source).WithExec([]string{
		"dart",
		"run",
		"tool/run_unit_tests.dart",
	})
}

func (m *NatsDart) natsServer(image string) *dagger.Service {
	return dag.Container().
		From(image).
		WithNewFile(
			"/etc/nats/nats.conf",
			"jetstream { store_dir: \"/data\" }\n"+
				"http_port: 8222\n"+
				"max_payload: 16777216\n",
		).
		WithMountedTemp("/data").
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) domainNatsServer() *dagger.Service {
	return dag.Container().
		From(latestNatsImage).
		WithNewFile(
			"/etc/nats/nats.conf",
			"jetstream { domain: \"DART\", store_dir: \"/data\" }\n"+
				"http_port: 8222\n"+
				"max_payload: 16777216\n",
		).
		WithMountedTemp("/data").
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) configuredNatsServer(
	image string,
	config *dagger.File,
) *dagger.Service {
	return dag.Container().
		From(image).
		WithFile("/etc/nats/nats.conf", config).
		WithMountedTemp("/data").
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) ackSubjectV2Server() *dagger.Service {
	config := dag.Container().
		From(latestNatsImage).
		WithNewFile(
			"/fixture/nats.conf",
			"jetstream { domain: \"ACKV2\", store_dir: \"/data\" }\n"+
				"http_port: 8222\n"+
				"feature_flags { js_ack_fc_v2: true }\n",
		).
		File("/fixture/nats.conf")
	return m.configuredNatsServer(latestNatsImage, config)
}

func (m *NatsDart) tierAccountFixture(
	source *dagger.Workspace,
) *dagger.Directory {
	return dag.Container().
		From(goBuilderImage).
		WithMountedCache(
			"/go/pkg/mod",
			dag.CacheVolume(goModuleCacheKey),
		).
		WithMountedCache(
			"/root/.cache/go-build",
			dag.CacheVolume(goBuildCacheKey),
		).
		WithDirectory("/src", source.Directory("tool/go_tier_fixture")).
		WithWorkdir("/src").
		WithExec([]string{"go", "run", ".", "/out"}).
		Directory("/out")
}

func (m *NatsDart) securityFixture(
	source *dagger.Workspace,
) *dagger.Directory {
	return dag.Container().
		From(goBuilderImage).
		WithMountedCache(
			"/go/pkg/mod",
			dag.CacheVolume(goModuleCacheKey),
		).
		WithMountedCache(
			"/root/.cache/go-build",
			dag.CacheVolume(goBuildCacheKey),
		).
		WithDirectory("/src", source.Directory("tool/go_security_fixture")).
		WithWorkdir("/src").
		WithExec([]string{"go", "run", ".", "/out"}).
		Directory("/out")
}

func (m *NatsDart) mutualTLSServer(
	fixture *dagger.Directory,
) *dagger.Service {
	const config = `
port: 4222
http_port: 8222
tls {
  cert_file: "/fixture/mtls-server.pem"
  key_file: "/fixture/mtls-server.key"
  ca_file: "/fixture/mtls-ca.pem"
  verify: true
  timeout: 5
}
`
	return dag.Container().
		From(latestNatsImage).
		WithDirectory("/fixture", fixture).
		WithNewFile("/etc/nats/nats.conf", config).
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) compatibilityVersion(
	source *dagger.Workspace,
	image string,
	alias string,
	apiLevel string,
	lane string,
) *dagger.Container {
	url := "nats://" + alias + ":4222"
	container := m.withNatsService(
		m.dart(source),
		alias,
		m.natsServer(image),
		natsReadinessOptions{
			url:        url,
			monitorURL: "http://" + alias + ":8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", url).
		WithEnvVariable("NATS_MONITOR_URL", "http://"+alias+":8222").
		WithEnvVariable("NATS_EXPECTED_API_LEVEL", apiLevel)
	return m.runIntegrationLane(container, lane)
}

// +check
func (m *NatsDart) Compatibility212(source *dagger.Workspace) *dagger.Container {
	return m.compatibilityVersion(
		source,
		compatibility212Image,
		"nats-212",
		"3",
		"compatibility212",
	)
}

// +check
func (m *NatsDart) Compatibility210(source *dagger.Workspace) *dagger.Container {
	return m.compatibilityVersion(
		source,
		compatibility210Image,
		"nats-210",
		"0",
		"compatibility210",
	)
}

// +check
func (m *NatsDart) CoreLatest(source *dagger.Workspace) *dagger.Container {
	container := m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithEnvVariable("NATS_MONITOR_URL", "http://nats:8222")
	return m.runIntegrationLane(container, "coreLatest")
}

// +check
func (m *NatsDart) JetStreamLatest(source *dagger.Workspace) *dagger.Container {
	container := m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	)
	container = m.withNatsService(
		container,
		"nats-domain",
		m.domainNatsServer(),
		natsReadinessOptions{
			url:        "nats://nats-domain:4222",
			monitorURL: "http://nats-domain:8222",
			jetStream:  true,
		},
	)
	container = container.
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithEnvVariable("NATS_MONITOR_URL", "http://nats:8222").
		WithEnvVariable("NATS_EXPECTED_API_LEVEL", "4").
		WithEnvVariable("NATS_DOMAIN_URL", "nats://nats-domain:4222").
		WithEnvVariable("NATS_DOMAIN_API_PREFIX", "$JS.DART.API")
	return m.runIntegrationLane(container, "jetStreamLatest")
}

// +check
func (m *NatsDart) AckSubjectV2Latest(
	source *dagger.Workspace,
) *dagger.Container {
	container := m.withNatsService(
		m.dart(source),
		"nats-ack-v2",
		m.ackSubjectV2Server(),
		natsReadinessOptions{
			url:        "nats://nats-ack-v2:4222",
			monitorURL: "http://nats-ack-v2:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats-ack-v2:4222")
	return m.runIntegrationLane(container, "ackSubjectV2Latest")
}

// +check
func (m *NatsDart) AccountTierLatest(
	source *dagger.Workspace,
) *dagger.Container {
	fixture := m.tierAccountFixture(source)
	server := m.configuredNatsServer(
		latestNatsImage,
		fixture.File("nats.conf"),
	)
	base := m.dart(source).
		WithFile("/run/nats-tier/user.jwt", fixture.File("user.jwt"))
	container := m.withNatsService(
		base,
		"nats-tier",
		server,
		natsReadinessOptions{
			url:        "nats://nats-tier:4222",
			monitorURL: "http://nats-tier:8222",
			jetStream:  true,
			jwtFile:    "/run/nats-tier/user.jwt",
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats-tier:4222").
		WithEnvVariable(
			"NATS_TIER_USER_JWT_FILE",
			"/run/nats-tier/user.jwt",
		)
	return m.runIntegrationLane(container, "accountTierLatest")
}

func (m *NatsDart) soak(
	source *dagger.Workspace,
	durationSeconds int,
	seed int,
) *dagger.Container {
	container := m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222")
	return container.WithExec([]string{
		"dart",
		"run",
		"tool/run_soak.dart",
		"--duration-seconds",
		fmt.Sprintf("%d", durationSeconds),
		"--seed",
		fmt.Sprintf("%d", seed),
	})
}

// +check
func (m *NatsDart) SoakSmoke(source *dagger.Workspace) *dagger.Container {
	return m.soak(source, 15, 20260902)
}

func (m *NatsDart) Soak(
	source *dagger.Workspace,
	durationSeconds int,
	seed int,
) *dagger.Container {
	return m.soak(source, durationSeconds, seed)
}

// +check
func (m *NatsDart) OrderedConsumerLatest(source *dagger.Workspace) *dagger.Container {
	serverBinary := dag.Container().
		From(latestNatsImage).
		File("/usr/local/bin/nats-server")

	container := m.dart(source).
		WithFile("/usr/local/bin/nats-server", serverBinary)
	return m.withNatsService(
		container,
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithEnvVariable("NATS_SERVER_BINARY", "/usr/local/bin/nats-server").
		WithExec([]string{
			"dart",
			"test",
			"--chain-stack-traces",
			"--concurrency=1",
			"packages/nats_jetstream/test/integration/ordered_consumer_latest_test.dart",
			"packages/nats_jetstream/test/integration/ordered_consumer_reconnect_latest_test.dart",
		})
}

// +check
func (m *NatsDart) GoInteropLatest(source *dagger.Workspace) *dagger.Container {
	fixtureSource := source.Directory("tool/go_interop")
	fixture := dag.Container().
		From(goBuilderImage).
		WithMountedCache(
			"/go/pkg/mod",
			dag.CacheVolume(goModuleCacheKey),
		).
		WithMountedCache(
			"/root/.cache/go-build",
			dag.CacheVolume(goBuildCacheKey),
		).
		WithDirectory("/src", fixtureSource).
		WithWorkdir("/src").
		WithExec([]string{
			"go", "build", "-trimpath", "-o", "/out/nats-go-interop", ".",
		}).
		File("/out/nats-go-interop")
	base := m.dart(source).
		WithFile("/usr/local/bin/nats-go-interop", fixture)
	container := m.withNatsService(
		base,
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithEnvVariable(
			"NATS_GO_INTEROP_BINARY",
			"/usr/local/bin/nats-go-interop",
		)
	return m.runIntegrationLane(container, "goInteropLatest")
}

// +check
func (m *NatsDart) JetStreamProtocolLatest(source *dagger.Workspace) *dagger.Container {
	return m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithExec([]string{
			"dart",
			"test",
			"--chain-stack-traces",
			"--concurrency=1",
			"packages/nats_jetstream/test/integration/jetstream_account_stream_latest_test.dart",
			"packages/nats_jetstream/test/integration/kv_codecs_transforms_latest_test.dart",
			"packages/nats_jetstream/test/integration/kv_core_latest_test.dart",
			"packages/nats_jetstream/test/integration/kv_management_latest_test.dart",
			"packages/nats_jetstream/test/integration/kv_mirror_sources_latest_test.dart",
			"packages/nats_jetstream/test/integration/consumer_management_latest_test.dart",
			"packages/nats_jetstream/test/integration/consumer_delivery_latest_test.dart",
			"packages/nats_jetstream/test/integration/publish_counter_latest_test.dart",
			"packages/nats_jetstream/test/integration/atomic_fast_batch_latest_test.dart",
			"packages/nats_jetstream/test/integration/message_schedule_latest_test.dart",
			"packages/nats_jetstream/test/integration/message_ttl_constraints_latest_test.dart",
		})
}

// +check
func (m *NatsDart) StreamTransferLatest(source *dagger.Workspace) *dagger.Container {
	return m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithExec([]string{
			"dart",
			"test",
			"--chain-stack-traces",
			"--concurrency=1",
			"packages/nats_jetstream/test/integration/stream_transfer_latest_test.dart",
		})
}

// +check
func (m *NatsDart) ObjectStoreLatest(source *dagger.Workspace) *dagger.Container {
	return m.withNatsService(
		m.dart(source),
		"nats",
		m.natsServer(latestNatsImage),
		natsReadinessOptions{
			url:        "nats://nats:4222",
			monitorURL: "http://nats:8222",
			jetStream:  true,
		},
	).
		WithEnvVariable("NATS_URL", "nats://nats:4222").
		WithEnvVariable("NATS_MONITOR_URL", "http://nats:8222").
		WithExec([]string{
			"dart",
			"test",
			"--chain-stack-traces",
			"--concurrency=1",
			"packages/nats_jetstream/test/integration/object_store_crud_upload_latest_test.dart",
			"packages/nats_jetstream/test/integration/object_store_links_metadata_latest_test.dart",
			"packages/nats_jetstream/test/integration/object_store_integrity_limits_latest_test.dart",
			"packages/nats_jetstream/test/integration/object_store_management_latest_test.dart",
			"packages/nats_jetstream/test/integration/object_store_watch_latest_test.dart",
			"packages/nats_jetstream/test/integration/object_store_watch_adversarial_latest_test.dart",
		})
}

// +check
func (m *NatsDart) ReconnectLatest(source *dagger.Workspace) *dagger.Container {
	serverBinary := dag.Container().
		From(latestNatsImage).
		File("/usr/local/bin/nats-server")

	container := m.dart(source).
		WithFile("/usr/local/bin/nats-server", serverBinary).
		WithEnvVariable("NATS_SERVER_BINARY", "/usr/local/bin/nats-server")
	return m.runIntegrationLane(container, "reconnectLatest")
}

// ReconnectSuspendLatest proves heartbeat failure against a suspended server.
//
// +check
func (m *NatsDart) ReconnectSuspendLatest(source *dagger.Workspace) *dagger.Container {
	serverBinary := dag.Container().
		From(latestNatsImage).
		File("/usr/local/bin/nats-server")

	container := m.dart(source).
		WithFile("/usr/local/bin/nats-server", serverBinary).
		WithEnvVariable("NATS_SERVER_BINARY", "/usr/local/bin/nats-server")
	return m.runIntegrationLane(container, "reconnectSuspendLatest")
}

// +check
func (m *NatsDart) JetStreamClusterLatest(source *dagger.Workspace) *dagger.Container {
	serverBinary := dag.Container().
		From(latestNatsImage).
		File("/usr/local/bin/nats-server")

	container := m.dart(source).
		WithFile("/usr/local/bin/nats-server", serverBinary).
		WithEnvVariable("NATS_SERVER_BINARY", "/usr/local/bin/nats-server")
	return m.runIntegrationLane(container, "jetStreamClusterLatest")
}

func (m *NatsDart) tlsCertificates(source *dagger.Workspace) *dagger.Directory {
	return source.Directory("tool/test_certs")
}

func (m *NatsDart) tlsServer(
	certificates *dagger.Directory,
	handshakeFirst bool,
) *dagger.Service {
	config := fmt.Sprintf(`
port: 4222
http_port: 8222
tls {
  cert_file: "/certs/server.pem"
  key_file: "/certs/server.key"
  ca_file: "/certs/ca.pem"
  handshake_first: %t
  timeout: 5
}
`, handshakeFirst)

	return dag.Container().
		From(latestNatsImage).
		WithDirectory("/certs", certificates).
		WithFile(
			"/etc/nats/nats.conf",
			dag.Directory().WithNewFile("nats.conf", config).File("nats.conf"),
		).
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

// +check
func (m *NatsDart) TlsLatest(
	ctx context.Context,
	source *dagger.Workspace,
) (result string, resultErr error) {
	certificates := m.tlsCertificates(source)
	security := m.securityFixture(source)
	services := []*dagger.Service{
		m.tlsServer(certificates, false),
		m.tlsServer(certificates, true),
		m.mutualTLSServer(security),
	}
	startedServices := make([]*dagger.Service, 0, len(services))
	defer func() {
		for index := len(startedServices) - 1; index >= 0; index-- {
			_, stopErr := startedServices[index].Stop(
				ctx,
				dagger.ServiceStopOpts{Kill: true},
			)
			if stopErr != nil {
				resultErr = errors.Join(resultErr, stopErr)
			}
		}
	}()
	for _, service := range services {
		startedService, err := service.Start(ctx)
		if err != nil {
			return "", err
		}
		startedServices = append(startedServices, startedService)
	}
	base := m.dart(source).
		WithDirectory("/test-certs", certificates).
		WithDirectory("/security", security)
	container := m.withNatsService(
		base,
		"nats-tls",
		startedServices[0],
		natsReadinessOptions{
			url:        "tls://nats-tls:4222",
			monitorURL: "http://nats-tls:8222",
			ca:         "/test-certs/ca.pem",
		},
	)
	container = m.withNatsService(
		container,
		"nats-tls-first",
		startedServices[1],
		natsReadinessOptions{
			url:        "tls://nats-tls-first:4222",
			monitorURL: "http://nats-tls-first:8222",
			ca:         "/test-certs/ca.pem",
			tlsFirst:   true,
		},
	)
	container = m.withNatsService(
		container,
		"nats-mtls",
		startedServices[2],
		natsReadinessOptions{
			url:        "tls://nats-mtls:4222",
			monitorURL: "http://nats-mtls:8222",
			ca:         "/security/mtls-ca.pem",
			clientCert: "/security/mtls-client.pem",
			clientKey:  "/security/mtls-client.key",
		},
	)
	container = container.
		WithEnvVariable("NATS_TLS_URL", "tls://nats-tls:4222").
		WithEnvVariable(
			"NATS_TLS_FIRST_URL",
			"tls://nats-tls-first:4222",
		).
		WithEnvVariable("NATS_CA", "/test-certs/ca.pem").
		WithEnvVariable("NATS_MTLS_URL", "tls://nats-mtls:4222").
		WithEnvVariable("NATS_MTLS_CA", "/security/mtls-ca.pem").
		WithEnvVariable("NATS_MTLS_CLIENT_CERT", "/security/mtls-client.pem").
		WithEnvVariable("NATS_MTLS_CLIENT_KEY", "/security/mtls-client.key").
		WithEnvVariable("NATS_MTLS_OUTSIDER_CERT", "/security/mtls-outsider.pem").
		WithEnvVariable("NATS_MTLS_OUTSIDER_KEY", "/security/mtls-outsider.key")
	if _, err := m.runIntegrationLane(container, "tlsLatest").Sync(ctx); err != nil {
		return "", err
	}
	return "TLS verification passed", nil
}

func (m *NatsDart) webSocketServer(
	certificates *dagger.Directory,
	secure bool,
) *dagger.Service {
	webSocketPort := 8080
	webSocketTls := "no_tls: true"
	if secure {
		webSocketPort = 8443
		webSocketTls = `tls {
    cert_file: "/certs/server.pem"
    key_file: "/certs/server.key"
  }`
	}
	config := fmt.Sprintf(`
port: 4222
http_port: 8222
websocket {
  port: %d
  %s
}
`, webSocketPort, webSocketTls)

	return dag.Container().
		From(latestNatsImage).
		WithDirectory("/certs", certificates).
		WithFile(
			"/etc/nats/nats.conf",
			dag.Directory().WithNewFile("nats.conf", config).File("nats.conf"),
		).
		WithExposedPort(4222).
		WithExposedPort(webSocketPort).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) browserFixtureBinary(
	source *dagger.Workspace,
) *dagger.File {
	return dag.Container().
		From(goBuilderImage).
		WithMountedCache(
			"/go/pkg/mod",
			dag.CacheVolume(goModuleCacheKey),
		).
		WithMountedCache(
			"/root/.cache/go-build",
			dag.CacheVolume(goBuildCacheKey),
		).
		WithDirectory(
			"/src",
			source.Directory("tool/go_browser_fixture"),
		).
		WithWorkdir("/src").
		WithExec([]string{
			"go", "build", "-trimpath", "-o", "/out/browser-fixture", ".",
		}).
		File("/out/browser-fixture")
}

func (m *NatsDart) browserFixture(
	source *dagger.Workspace,
	certificates *dagger.Directory,
) *dagger.Service {
	configs := dag.Directory().
		WithNewFile("a.conf", `
server_name: "browser-a"
port: 4222
http_port: 8222
websocket {
  port: 8080
  no_tls: true
  advertise: "nats-browser:8080"
}
cluster {
  name: "browser"
  port: 6222
}
`).
		WithNewFile("b.conf", `
server_name: "browser-b"
port: 4223
http_port: 8223
websocket {
  port: 8081
  no_tls: true
  advertise: "nats-browser:8081"
}
cluster {
  name: "browser"
  port: 6223
  routes: ["nats-route://127.0.0.1:6222"]
}
`).
		WithNewFile("origin.conf", `
port: 4224
http_port: 8224
websocket {
  port: 8082
  no_tls: true
}
`).
		WithNewFile("certificate.conf", `
port: 4225
websocket {
  port: 8444
  tls {
    cert_file: "/certs/browser-untrusted.pem"
    key_file: "/certs/browser-untrusted.key"
  }
}
`)

	return dag.Container().
		From(latestNatsImage).
		WithFile(
			"/usr/local/bin/browser-fixture",
			m.browserFixtureBinary(source),
		).
		WithDirectory("/etc/nats-browser", configs).
		WithDirectory("/certs", certificates).
		WithExposedPort(8080).
		WithExposedPort(8081).
		WithExposedPort(8082).
		WithExposedPort(8090).
		WithExposedPort(8444).
		AsService(dagger.ContainerAsServiceOpts{
			Args: []string{
				"/usr/local/bin/browser-fixture",
				"--server", "/usr/local/bin/nats-server",
				"--configs", "/etc/nats-browser",
			},
		})
}

// +check
func (m *NatsDart) WebSocketLatest(source *dagger.Workspace) *dagger.Container {
	certificates := m.tlsCertificates(source)
	browserFixture := m.browserFixture(source, certificates)
	base := m.browserDart(source).
		WithDirectory("/test-certs", certificates)
	container := m.withNatsService(
		base,
		"nats-ws",
		m.webSocketServer(certificates, false),
		natsReadinessOptions{
			url:        "ws://nats-ws:8080/nats",
			monitorURL: "http://nats-ws:8222",
		},
	)
	container = m.withNatsService(
		container,
		"nats-wss",
		m.webSocketServer(certificates, true),
		natsReadinessOptions{
			url:        "wss://nats-wss:8443/nats",
			monitorURL: "http://nats-wss:8222",
			ca:         "/test-certs/ca.pem",
		},
	)
	container = container.
		WithServiceBinding("nats-browser", browserFixture).
		WithEnvVariable("NATS_WS_URL", "ws://nats-ws:8080/nats").
		WithEnvVariable("NATS_WSS_URL", "wss://nats-wss:8443/nats").
		WithEnvVariable("NATS_WS_CA", "/test-certs/ca.pem")
	container = m.runIntegrationLane(container, "webSocketLatest").
		WithExec([]string{
			"dart",
			"compile",
			"js",
			"packages/nats_core/tool/websocket_browser_smoke.dart",
			"-o",
			"/tmp/websocket_browser_smoke.js",
		}).
		WithEnvVariable("CHROME_EXECUTABLE", "/usr/local/bin/chrome-for-test")
	return m.runIntegrationLane(container, "webSocketBrowserLatest")
}

func (m *NatsDart) authServer() *dagger.Service {
	const config = `
port: 4222
http_port: 8222
max_subscriptions: 2
authorization {
  users: [
    {
      user: "alice"
      password: "wonderland"
      permissions: {
        publish: {allow: ["allowed.>"]}
        subscribe: {allow: ["allowed.>"]}
      }
    },
    {nkey: "UD466L6EBCM3YY5HEGHJANNTN4LSKTSUXTH7RILHCKEQMQHTBNLHJJXT"}
  ]
}
`

	return dag.Container().
		From(latestNatsImage).
		WithFile(
			"/etc/nats/nats.conf",
			dag.Directory().WithNewFile("nats.conf", config).File("nats.conf"),
		).
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

func (m *NatsDart) tokenAuthServer() *dagger.Service {
	const config = `
port: 4222
http_port: 8222
authorization {
  token: "latest-token"
}
`

	return dag.Container().
		From(latestNatsImage).
		WithFile(
			"/etc/nats/nats.conf",
			dag.Directory().WithNewFile("nats.conf", config).File("nats.conf"),
		).
		WithExposedPort(4222).
		WithExposedPort(8222).
		AsService(dagger.ContainerAsServiceOpts{
			Args:          []string{"-c", "/etc/nats/nats.conf"},
			UseEntrypoint: true,
		})
}

// +check
func (m *NatsDart) AuthLatest(source *dagger.Workspace) *dagger.Container {
	security := m.securityFixture(source)
	base := m.dart(source).
		WithFile("/security/user.creds", security.File("user.creds"))
	container := m.withNatsService(
		base,
		"nats-auth",
		m.authServer(),
		natsReadinessOptions{
			url:        "nats://nats-auth:4222",
			monitorURL: "http://nats-auth:8222",
			username:   "alice",
			password:   "wonderland",
		},
	)
	container = m.withNatsService(
		container,
		"nats-token-auth",
		m.tokenAuthServer(),
		natsReadinessOptions{
			url:        "nats://nats-token-auth:4222",
			monitorURL: "http://nats-token-auth:8222",
			token:      "latest-token",
		},
	)
	container = m.withNatsService(
		container,
		"nats-credentials",
		m.configuredNatsServer(latestNatsImage, security.File("auth.conf")),
		natsReadinessOptions{
			url:             "nats://nats-credentials:4222",
			monitorURL:      "http://nats-credentials:8222",
			credentialsFile: "/security/user.creds",
		},
	)
	container = container.
		WithEnvVariable("NATS_AUTH_URL", "nats://nats-auth:4222").
		WithEnvVariable("NATS_TOKEN_AUTH_URL", "nats://nats-token-auth:4222").
		WithEnvVariable("NATS_CREDENTIALS_URL", "nats://nats-credentials:4222").
		WithEnvVariable("NATS_CREDENTIALS_FILE", "/security/user.creds")
	return m.runIntegrationLane(container, "authLatest")
}
