package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: go_tier_fixture OUTPUT_DIRECTORY")
	}
	outputDirectory := os.Args[1]
	must(os.MkdirAll(outputDirectory, 0o700))

	operatorKey := mustValue(nkeys.CreateOperator())
	operatorPublic := mustValue(operatorKey.PublicKey())
	systemKey := mustValue(nkeys.CreateAccount())
	systemPublic := mustValue(systemKey.PublicKey())
	operatorClaim := jwt.NewOperatorClaims(operatorPublic)
	operatorClaim.Name = "NatsDartTier"
	operatorClaim.SystemAccount = systemPublic
	operatorToken := mustValue(operatorClaim.Encode(operatorKey))
	systemClaim := jwt.NewAccountClaims(systemPublic)
	systemClaim.Name = "SYS"
	systemToken := mustValue(systemClaim.Encode(operatorKey))

	accountKey := mustValue(nkeys.CreateAccount())
	accountPublic := mustValue(accountKey.PublicKey())
	accountClaim := jwt.NewAccountClaims(accountPublic)
	accountClaim.Name = "TIER"
	accountClaim.Limits.JetStreamTieredLimits["R1"] = jwt.JetStreamLimits{
		MemoryStorage:        8_000_000,
		DiskStorage:          16_000_000,
		Streams:              3,
		Consumer:             4,
		MaxAckPending:        32,
		MemoryMaxStreamBytes: 4_000_000,
		DiskMaxStreamBytes:   6_000_000,
	}
	accountToken := mustValue(accountClaim.Encode(operatorKey))

	userKey := mustValue(nkeys.CreateUser())
	userPublic := mustValue(userKey.PublicKey())
	userClaim := jwt.NewUserClaims(userPublic)
	userClaim.Name = "tester"
	userClaim.BearerToken = true
	userToken := mustValue(userClaim.Encode(accountKey))

	config := fmt.Sprintf(`
operator: %s
system_account: %s
resolver: MEMORY
resolver_preload: {
  %s: %s
  %s: %s
}
http_port: 8222
jetstream {
  store_dir: "/data"
  max_memory_store: 33554432
  max_file_store: 33554432
}
`,
		operatorToken,
		systemPublic,
		systemPublic,
		systemToken,
		accountPublic,
		accountToken,
	)
	must(os.WriteFile(filepath.Join(outputDirectory, "nats.conf"), []byte(config), 0o600))
	must(os.WriteFile(filepath.Join(outputDirectory, "user.jwt"), []byte(userToken), 0o600))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustValue[T any](value T, err error) T {
	must(err)
	return value
}
