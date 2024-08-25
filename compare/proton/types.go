package proton

// TransactionHeader message definition.
type TransactionHeader struct {
	Properties map[string]string
	EdgeNode   string
	Signature  Signature
}

// Signature message definition.
type Signature struct {
	Algorithm string
	Signature string
}

// Transaction message definition.
type Transaction struct {
	Hash    string
	Payload []byte
	GasUsed int64
	Header  TransactionHeader
}

// TransactionResponse message definition.
type TransactionResponse struct {
	Hash    string
	Success bool
	Message string
}
