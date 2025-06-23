package diskcache_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDiskcache(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Diskcache Suite")
}
