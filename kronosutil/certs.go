package kronosutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/rubrikinc/kronos/kronosutil/log"

	"github.com/scaledata/etcd/pkg/transport"
	"google.golang.org/grpc/credentials"
)

const (
	defaultMinTLSVersionKey     = "KRONOS_MIN_TLS_VERSION"
	defaultMaxTLSVersionKey     = "KRONOS_MAX_TLS_VERSION"
	defaultTLS12CipherSuitesKey = "KRONOS_TLS_1_2_CIPHER_SUITES"

	defaultMinTLSVersionValue = tls.VersionTLS12
	defaultMaxTLSVersionValue = tls.VersionTLS13
)

var tls12CipherSuitesDefaultValue = []uint16{
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
}

// SSLCreds returns credentials by reading keys and certificates from
// certsDir
func SSLCreds(certsDir string) (credentials.TransportCredentials, error) {
	// Load the certificates from disk
	certificate, err := tls.LoadX509KeyPair(
		fmt.Sprint(filepath.Join(certsDir, NodeCert)),
		fmt.Sprint(filepath.Join(certsDir, NodeKey)),
	)
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %s", err)
	}

	// Create a certificate pool from the certificate authority
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(fmt.Sprint(filepath.Join(certsDir, CACert)))
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	// Append the client certificates from the CA
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append client certs")
	}

	// Create the TLS credentials
	minVersion, maxVersion := getTLSVersions()
	tls12Ciphers := getTls12CipherSuites()
	creds := credentials.NewTLS(&tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    certPool,
		RootCAs:      certPool,
		MinVersion:   minVersion,
		MaxVersion:   maxVersion,
		CipherSuites: tls12Ciphers,
	})
	return creds, nil
}

// TLSInfo returns the tlsInfo with certificates in the certsDir. if certsDir
// is empty, it returns empty tlsInfo.
func TLSInfo(certsDir string) transport.TLSInfo {
	if certsDir == "" {
		return transport.TLSInfo{}
	}
	return transport.TLSInfo{
		CertFile: filepath.Join(certsDir, NodeCert),
		KeyFile:  filepath.Join(certsDir, NodeKey),
		CAFile:   filepath.Join(certsDir, CACert),
	}
}

func parseTLS12CipherSuites(
	inputCiphersInIana string,
) ([]uint16, error) {
	if inputCiphersInIana == "" {
		return nil, fmt.Errorf("no cipher suites provided")
	}
	ianaCiphersList := strings.Split(inputCiphersInIana, ":")
	supportedSuites := tls.CipherSuites()
	var acceptedSuites []uint16
	var discardedSuites []string
	for _, cipher := range ianaCiphersList {
		found := false
		for _, suite := range supportedSuites {
			if suite.Name == cipher {
				acceptedSuites = append(acceptedSuites, suite.ID)
				found = true
				break
			}
		}
		if !found {
			discardedSuites = append(discardedSuites, cipher)
		}
	}
	if len(acceptedSuites) == 0 {
		return nil, fmt.Errorf("provided cipher suite(s) %+v is/are not "+
			"supported by current TLS package", inputCiphersInIana)
	}
	if len(discardedSuites) > 0 {
		log.Warningf(
			context.Background(),
			"Some of the provided cipher suite(s) %v is/are not supported by"+
				" TLS package",
			discardedSuites)
	}
	return acceptedSuites, nil
}

func convertTLSVersionStrToInt(tlsVersionStr string) (uint16, error) {
	switch tlsVersionStr {
	case "TLSv1.2":
		return tls.VersionTLS12, nil
	case "TLSv1.3":
		return tls.VersionTLS13, nil
	}
	return 0, fmt.Errorf(
		"invalid TLS version: %s, supported values: 'TLSv1.2', 'TLSv1.3'",
		tlsVersionStr)
}

func getTLSVersions() (uint16, uint16) {
	minVersion := os.Getenv(defaultMinTLSVersionKey)
	minVersionInt, err := convertTLSVersionStrToInt(minVersion)
	if err != nil {
		log.Errorf(
			context.Background(),
			"Failed to convert min TLS version: %v. Using default values instead.",
			err)
		return defaultMinTLSVersionValue, defaultMaxTLSVersionValue
	}
	maxVersion := os.Getenv(defaultMaxTLSVersionKey)
	maxVersionInt, err := convertTLSVersionStrToInt(maxVersion)
	if err != nil {
		log.Errorf(
			context.Background(),
			"Failed to convert max TLS version: %v. Using default values instead.",
			err)
		return defaultMinTLSVersionValue, defaultMaxTLSVersionValue
	}
	if minVersion > maxVersion {
		log.Errorf(
			context.Background(),
			"minimum TLS version is higher than maximum TLS version. "+
				"Using default values instead.")
		return defaultMinTLSVersionValue, defaultMaxTLSVersionValue
	}
	log.Infof(
		context.Background(),
		"Fetched TLS versions: min=%s, max=%s",
		minVersion,
		maxVersion)
	return minVersionInt, maxVersionInt
}

func getTls12CipherSuites() []uint16 {
	ianaTls12Ciphers := os.Getenv(defaultTLS12CipherSuitesKey)
	convertedCiphers, err := parseTLS12CipherSuites(ianaTls12Ciphers)
	if err != nil {
		log.Errorf(
			context.Background(),
			"Failed to convert TLS 1.2 cipher suites: %v. Defaulting to %v.",
			err,
			tls12CipherSuitesDefaultValue)
		return tls12CipherSuitesDefaultValue
	}
	log.Infof(
		context.Background(),
		"Fetched TLS 1.2 cipher suites: %v",
		ianaTls12Ciphers)
	return convertedCiphers
}
