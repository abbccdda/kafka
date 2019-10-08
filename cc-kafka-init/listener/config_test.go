package listener

import (
	"os"
	"testing"
)

func Test_readAdvertisedListenerProperty(t *testing.T) {
	fd, err := os.Open("golden/server-pod.properties.golden")
	if err != nil {
		t.Error(err)
	}
	listenersProp, err := readAdvertisedListener("EXTERNAL", fd)
	if err != nil {
		t.Error(err)
	}
	if listenersProp != "b0-pkc-1mqdxgn.us-central1.gcp.priv.cpdev.cloud:9092" {
		t.Error("unexpected listeners prop")
	}
}
