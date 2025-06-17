package server_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestServer(t *testing.T) {
	// t.Skip()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Server Suite")
}
