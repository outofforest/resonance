package proton

// SignatureAlgorithm is the signature algorithm.
type SignatureAlgorithm uint8

// Signature algorithms.
const (
	SignatureAlgorithmED25519 SignatureAlgorithm = iota
)

// TransactionHeader message definition.
type TransactionHeader struct {
	Properties map[string]string
	EdgeNode   string
	Signature  Signature
}

// Signature message definition.
type Signature struct {
	Algorithm SignatureAlgorithm
	Signature [64]byte
}

// Transaction message definition.
type Transaction struct {
	Hash    [16]byte
	Payload []byte
	GasUsed int64
	Header  TransactionHeader
}

// TransactionResponse message definition.
type TransactionResponse struct {
	Hash    [16]byte
	Success bool
	Message string
}
