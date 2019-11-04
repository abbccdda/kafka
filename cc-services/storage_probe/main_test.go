package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

// A simple test processor that always succeeds
func testSimpleProcessor(filename string) error {
	return nil
}

func TestProcessFile(t *testing.T) {
	var testfile = "/tmp/foo"
	if err := processFile(testfile); err != nil {
		t.Error(err)
	}
	f, err := os.Open(testfile)
	if err != nil {
		t.Error(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Error(err)
	}
	if fi.Name() != path.Base(testfile) {
		t.Errorf("Name mismatch: expected %s got %s", testfile, fi.Name())
	}
	if fi.Size() == 0 {
		t.Errorf("Expected some data, got 0")
	}
}

func multifileTester(t *testing.T, processor func(string) error,
	timeoutSecs int, filenames ...string) {
	startTime := time.Now()
	ctx := context.Background()
	err := processFilenames(ctx, processor, filenames...)
	if err != nil {
		t.Errorf("Expected trivial success, got %v", err)
	}
	elapsedTime := time.Since(startTime)
	if elapsedTime > (time.Duration(timeoutSecs) * time.Second) {
		t.Error("Timeout without an error!")
	}
}

func multifileSimpleTester(t *testing.T, timeoutSecs int, filenames ...string) {
	multifileTester(t, testSimpleProcessor, timeoutSecs, filenames...)
}

func TestProcessNoFiles(t *testing.T) {
	multifileSimpleTester(t, 30)
}

func TestProcessOneFile(t *testing.T) {
	multifileSimpleTester(t, 30, "foo")
}

func TestProcessTwoFiles(t *testing.T) {
	multifileSimpleTester(t, 30, "foo", "bar")
}

func TestProcessMultipleFiles(t *testing.T) {
	multifileSimpleTester(t, 30, "foo", "bar", "baz", "qux", "10")
}

func testErrorProcessor(file string) error {
	return fmt.Errorf("injected error for file %v", file)
}

func multifileFailTester(t *testing.T, timeoutSecs int, filenames ...string) {
	startTime := time.Now()
	ctx := context.Background()
	err := processFilenames(ctx, testErrorProcessor, filenames...)
	if err == nil {
		t.Error("Expected failure, got nil")
	}
	elapsedTime := time.Since(startTime)
	if elapsedTime > (time.Duration(timeoutSecs) * time.Second) {
		t.Error("Timeout without an error!")
	}
}

func TestFailProcessNoFiles(t *testing.T) {
	// This should actually succeed.
	multifileTester(t, testErrorProcessor, 30)
}

func TestFailProcessOneFile(t *testing.T) {
	multifileFailTester(t, 30, "foo")
}

func TestFailProcessTwoFiles(t *testing.T) {
	multifileFailTester(t, 30, "foo", "bar")
}

func TestFailProcessOneFiles(t *testing.T) {
	multifileFailTester(t, 30, "foo", "bar", "baz", "qux", "10")
}

func timeoutGenerator(waitSecs int) func(string) error {
	return func(filename string) error {
		time.Sleep(time.Duration(waitSecs+5) * time.Second)
		return nil
	}
}

func multifileTimeoutTester(t *testing.T, timeoutSecs int, filenames ...string) {
	startTime := time.Now()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	defer cancel()

	err := processFilenames(ctx, timeoutGenerator(timeoutSecs), filenames...)
	if err == nil {
		t.Error("Expected timeout, got nil")
	}
	elapsedTime := time.Since(startTime)
	if elapsedTime < (time.Duration(timeoutSecs) * time.Second) {
		t.Error("Expected timeout but didn't get one!")
	}
}

func TestTimeoutProcessNoFiles(t *testing.T) {
	// This should actually succeed
	multifileTester(t, timeoutGenerator(1), 1)
}

func TestTimeoutProcessOneFile(t *testing.T) {
	multifileTimeoutTester(t, 1, "foo")
}

func TestTimeoutProcessTwoFiles(t *testing.T) {
	multifileTimeoutTester(t, 1, "foo", "bar")
}

func TestTimeoutProcessOneFiles(t *testing.T) {
	multifileTimeoutTester(t, 1, "foo", "bar", "baz", "qux", "10")
}
