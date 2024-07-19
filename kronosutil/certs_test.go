package kronosutil

import (
	"crypto/tls"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTls12CipherSuites(t *testing.T) {
	testCases := []struct {
		inputCipherSuites    string
		expectedCipherSuites []uint16
	}{
		{
			// Test 1: Positive test - 1 valid cipher suite
			inputCipherSuites:    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
			expectedCipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
		}, {
			// Test 2: Positive test - multiple valid cipher suites
			inputCipherSuites: "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:" +
				"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
			expectedCipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
		}, {
			// Test 3: Negative test - one invalid cipher suite
			inputCipherSuites:    "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:invalid",
			expectedCipherSuites: []uint16{tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384},
		}, {
			// Test 4: Negative test - Empty string
			inputCipherSuites: "",
			expectedCipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			},
		},
	}

	for _, tc := range testCases {
		assert.Nil(
			t,
			os.Setenv("KRONOS_TLS_1_2_CIPHER_SUITES", tc.inputCipherSuites))
		result := GetTls12CipherSuites()
		assert.ElementsMatch(t, result, tc.expectedCipherSuites)
		assert.Nil(t, os.Unsetenv("KRONOS_TLS_1_2_CIPHER_SUITES"))
	}
}

func TestGetTLSVersions(t *testing.T) {
	testCases := []struct {
		inputMinVersionStr string
		inputMaxVersionStr string
		expectedMinVersion uint16
		expectedMaxVersion uint16
	}{
		{
			// Test 1: Valid versions
			inputMinVersionStr: "TLSv1.2",
			inputMaxVersionStr: "TLSv1.3",
			expectedMinVersion: tls.VersionTLS12,
			expectedMaxVersion: tls.VersionTLS13,
		},
		{
			// Test 2: Valid versions
			inputMinVersionStr: "TLSv1.2",
			inputMaxVersionStr: "TLSv1.2",
			expectedMinVersion: tls.VersionTLS12,
			expectedMaxVersion: tls.VersionTLS12,
		},
		{
			// Test 3: Valid versions
			inputMinVersionStr: "TLSv1.3",
			inputMaxVersionStr: "TLSv1.3",
			expectedMinVersion: tls.VersionTLS13,
			expectedMaxVersion: tls.VersionTLS13,
		},
		{
			// Test 4: Empty strings
			inputMinVersionStr: "",
			inputMaxVersionStr: "",
			expectedMinVersion: tls.VersionTLS12,
			expectedMaxVersion: tls.VersionTLS13,
		},
		{
			// Test 5: Invalid config
			inputMinVersionStr: "TLSv1.3",
			inputMaxVersionStr: "TLSv1.2",
			expectedMinVersion: tls.VersionTLS12,
			expectedMaxVersion: tls.VersionTLS13,
		},
		{
			// Test 6: Invalid config
			inputMinVersionStr: "TLSv1.1",
			inputMaxVersionStr: "TLSv1.4",
			expectedMinVersion: tls.VersionTLS12,
			expectedMaxVersion: tls.VersionTLS13,
		},
	}

	for _, tc := range testCases {
		assert.Nil(
			t,
			os.Setenv("KRONOS_MIN_TLS_VERSION", tc.inputMinVersionStr))
		assert.Nil(
			t,
			os.Setenv("KRONOS_MAX_TLS_VERSION", tc.inputMaxVersionStr))
		minVersion, maxVersion := GetTLSVersions()
		assert.Equal(t, minVersion, tc.expectedMinVersion)
		assert.Equal(t, maxVersion, tc.expectedMaxVersion)
		assert.Nil(t, os.Unsetenv("KRONOS_MIN_TLS_VERSION"))
		assert.Nil(t, os.Unsetenv("KRONOS_MAX_TLS_VERSION"))
	}
}
