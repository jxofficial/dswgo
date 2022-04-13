package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	IsServer      bool
	Certificates  []tls.Certificate
}

func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}

	// only server will have these two files
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		b, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		// add CA's cert to CA
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM(b)
		if !ok {
			// %q doesnt parse the \ escape char
			return nil, fmt.Errorf(
				"failed to parse CA root certificate: %q",
				cfg.CAFile,
			)
		}
		if cfg.IsServer {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}

	return tlsConfig, nil
}
