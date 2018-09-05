package kronosutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/scaledata/etcd/pkg/transport"
	"google.golang.org/grpc/credentials"
)

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
	creds := credentials.NewTLS(&tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    certPool,
		RootCAs:      certPool,
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
