package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAgentNodes(t *testing.T) {
	expectedNodes := []string{
		"cc-trogdor-service-agent-0",
		"cc-trogdor-service-agent-1",
		"cc-trogdor-service-agent-2",
	}
	clientNodes := TrogdorAgentPodNames(3)
	assert.Equal(t, clientNodes, expectedNodes)
}

func TestDurationToMs(t *testing.T) {
	durA := time.Duration(1000*1000) * time.Microsecond
	durB := time.Duration(1000) * time.Millisecond
	durC := time.Duration(1) * time.Second

	assert.Equal(t, uint64(1000), uint64(durA/time.Millisecond))
	assert.Equal(t, uint64(1000), uint64(durB/time.Millisecond))
	assert.Equal(t, uint64(1000), uint64(durC/time.Millisecond))
}
