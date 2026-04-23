package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) error {
	var err error
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func sha256Generator() sarama.SCRAMClient {
	return &xdgSCRAMClient{HashGeneratorFcn: func() hash.Hash { return sha256.New() }}
}

func sha512Generator() sarama.SCRAMClient {
	return &xdgSCRAMClient{HashGeneratorFcn: func() hash.Hash { return sha512.New() }}
}
