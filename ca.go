package resonance

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"time"

	"github.com/pkg/errors"
)

const certCommonName = "resonance.invalid"

var _ CASource = &CA{}

// CA represents certificate authority issuing TLS configs.
type CA struct {
	caCert    *x509.Certificate
	caPrivKey ed25519.PrivateKey
}

// NewCA creates new CA.
func NewCA(r io.Reader) (*CA, error) {
	caPub, caPriv, err := ed25519.GenerateKey(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	caCert := &x509.Certificate{
		SerialNumber: big.NewInt(1),

		NotBefore: time.Now().AddDate(-2, 0, 0),
		NotAfter:  time.Now().AddDate(100, 0, 0),

		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
	}
	caDER, err := x509.CreateCertificate(r, caCert, caCert, caPub, caPriv)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	caCert, err = x509.ParseCertificate(caDER)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &CA{
		caCert:    caCert,
		caPrivKey: caPriv,
	}, nil
}

// Generate generates new TLS config with resh client certificate.
func (ca *CA) Generate() (*tls.Config, error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	connCert := &x509.Certificate{
		SerialNumber: big.NewInt(2),

		DNSNames: []string{certCommonName},

		NotBefore: time.Now().AddDate(-1, 0, 0),
		NotAfter:  time.Now().AddDate(50, 0, 0),

		BasicConstraintsValid: true,
	}
	connDER, err := x509.CreateCertificate(nil, connCert, ca.caCert, pub, ca.caPrivKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	tlsCert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: connDER,
		}),
		pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privBytes,
		}),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(ca.caCert)
	return &tls.Config{
		RootCAs:      caPool,
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{tlsCert},
		ServerName:   certCommonName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
