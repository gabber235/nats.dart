package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: go_security_fixture OUTPUT_DIRECTORY")
	}
	outputDirectory := os.Args[1]
	must(os.MkdirAll(outputDirectory, 0o700))
	writeCredentialFixture(outputDirectory)
	writeMutualTLSFixture(outputDirectory)
}

func writeCredentialFixture(outputDirectory string) {
	operatorKey := mustValue(nkeys.CreateOperator())
	operatorPublic := mustValue(operatorKey.PublicKey())
	systemKey := mustValue(nkeys.CreateAccount())
	systemPublic := mustValue(systemKey.PublicKey())
	operatorClaim := jwt.NewOperatorClaims(operatorPublic)
	operatorClaim.Name = "NatsDartCredentials"
	operatorClaim.SystemAccount = systemPublic
	operatorToken := mustValue(operatorClaim.Encode(operatorKey))
	systemClaim := jwt.NewAccountClaims(systemPublic)
	systemClaim.Name = "SYS"
	systemToken := mustValue(systemClaim.Encode(operatorKey))

	accountKey := mustValue(nkeys.CreateAccount())
	accountPublic := mustValue(accountKey.PublicKey())
	accountClaim := jwt.NewAccountClaims(accountPublic)
	accountClaim.Name = "CREDENTIALS"
	accountToken := mustValue(accountClaim.Encode(operatorKey))

	userKey := mustValue(nkeys.CreateUser())
	userPublic := mustValue(userKey.PublicKey())
	userClaim := jwt.NewUserClaims(userPublic)
	userClaim.Name = "credentials-test"
	userToken := mustValue(userClaim.Encode(accountKey))
	userSeed := mustValue(userKey.Seed())

	config := fmt.Sprintf(`
port: 4222
http_port: 8222
operator: %s
system_account: %s
resolver: MEMORY
resolver_preload: {
  %s: %s
  %s: %s
}
`, operatorToken, systemPublic, systemPublic, systemToken, accountPublic, accountToken)
	credentials := fmt.Sprintf(`-----BEGIN NATS USER JWT-----
%s
------END NATS USER JWT------

************************* IMPORTANT *************************

-----BEGIN USER NKEY SEED-----
%s
------END USER NKEY SEED------
`, userToken, userSeed)

	write(outputDirectory, "auth.conf", []byte(config))
	write(outputDirectory, "user.creds", []byte(credentials))
}

func writeMutualTLSFixture(outputDirectory string) {
	trustedCA := newCertificateAuthority("NATS Dart trusted test CA", 1)
	outsiderCA := newCertificateAuthority("NATS Dart outsider test CA", 2)
	server := newCertificate(
		trustedCA,
		"nats-mtls",
		3,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		[]string{"nats-mtls"},
	)
	client := newCertificate(
		trustedCA,
		"trusted-client",
		4,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		nil,
	)
	outsider := newCertificate(
		outsiderCA,
		"outsider-client",
		5,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		nil,
	)

	write(outputDirectory, "mtls-ca.pem", trustedCA.certificatePEM)
	write(outputDirectory, "mtls-server.pem", server.certificatePEM)
	write(outputDirectory, "mtls-server.key", server.privateKeyPEM)
	write(outputDirectory, "mtls-client.pem", client.certificatePEM)
	write(outputDirectory, "mtls-client.key", client.privateKeyPEM)
	write(outputDirectory, "mtls-outsider.pem", outsider.certificatePEM)
	write(outputDirectory, "mtls-outsider.key", outsider.privateKeyPEM)
}

type certificateAuthority struct {
	certificate    *x509.Certificate
	privateKey     *rsa.PrivateKey
	certificatePEM []byte
}

type certificateIdentity struct {
	certificatePEM []byte
	privateKeyPEM  []byte
}

func newCertificateAuthority(name string, serial int64) certificateAuthority {
	key := mustValue(rsa.GenerateKey(rand.Reader, 2048))
	template := certificateTemplate(name, serial)
	template.IsCA = true
	template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	template.BasicConstraintsValid = true
	der := mustValue(x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key))
	return certificateAuthority{
		certificate:    template,
		privateKey:     key,
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func newCertificate(
	issuer certificateAuthority,
	name string,
	serial int64,
	usages []x509.ExtKeyUsage,
	dnsNames []string,
) certificateIdentity {
	key := mustValue(rsa.GenerateKey(rand.Reader, 2048))
	template := certificateTemplate(name, serial)
	template.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	template.ExtKeyUsage = usages
	template.DNSNames = dnsNames
	der := mustValue(
		x509.CreateCertificate(
			rand.Reader,
			template,
			issuer.certificate,
			&key.PublicKey,
			issuer.privateKey,
		),
	)
	return certificateIdentity{
		certificatePEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		privateKeyPEM: pem.EncodeToMemory(
			&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)},
		),
	}
}

func certificateTemplate(name string, serial int64) *x509.Certificate {
	now := time.Now()
	return &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
	}
}

func write(directory string, name string, contents []byte) {
	must(os.WriteFile(filepath.Join(directory, name), contents, 0o600))
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
