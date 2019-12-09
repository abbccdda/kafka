package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	datadog "github.com/DataDog/opencensus-go-exporter-datadog"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// The (trivial) data written out to our probe files.
const PROBE_DATA = "livenessProbe"

var probe_data = []byte(PROBE_DATA)

// Metrics
var (
	mLatencyMs = stats.Float64("writeLatencyMs", "Time to run the probes in milliseconds",
		stats.UnitMilliseconds)
	mErrors   = stats.Int64("errors", "Error count", stats.UnitDimensionless)
	mTimeouts = stats.Int64("timeouts", "Timeouts", stats.UnitDimensionless)

	// Tag the stats with the relevant cluster and pod so we can drill down to a problematic instance.
	podName, _   = tag.NewKey("pod-name")
	clusterId, _ = tag.NewKey("clusterid")

	statsLatencyView = view.View{
		Name:        "storage_probe/latency",
		Measure:     mLatencyMs,
		Description: "Storage probe runtime in milliseconds",
		TagKeys:     []tag.Key{podName, clusterId},
		Aggregation: view.Distribution(0, 100, 1000, 10000, 30000),
	}
	statsErrorView = view.View{
		Name:        "storage_probe/errors",
		Measure:     mErrors,
		Description: "Number of errors (timeout or FS error) generated by storage probe writes",
		TagKeys:     []tag.Key{podName, clusterId},
		Aggregation: view.Count(),
	}
	statsTimeoutView = view.View{
		Name:        "storage_probe/timeouts",
		Measure:     mTimeouts,
		Description: "Storage probe timeouts",
		TagKeys:     []tag.Key{podName, clusterId},
		Aggregation: view.Count(),
	}
)

// Handle an interrupt signal in a context-friendly fashion.
func contextHandleSignal(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(c)
		cancel()
	}
}

// Attempt to write some arbitrary data into a given file.
// Success is if we can sync it.
func processFile(filename string) error {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(probe_data)
	if err != nil {
		return err
	}
	return f.Sync()
}

// Process a collection of filenames, with the result returned through
// the 'errs' channel.
// fileProcessor is what actually does stuff to files, it exists for test
// purposes.
func processFilenames(ctx context.Context, fileProcessor func(string) error,
	filenames ...string) error {
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	// Process each file in parallel
	for _, filename := range filenames {
		wg.Add(1)
		go func(fn string) {
			defer wg.Done()
			if err := fileProcessor(fn); err != nil {
				errs <- err
			}
		}(filename)
	}

	// Wait for all filename processing to finish.
	go func() {
		wg.Wait()
		// At this point guaranteed that no processor goroutines are still running,
		// safe to close this channel.
		close(errs)
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

/*
 * Setup stats and do the real work. This is done in a sub-function rather
 * than main in order to allow stats to always exit cleanly via defer.
 */
func setupMetricsAndRun(ctx context.Context, statsAddr string, filenames ...string) error {
	var startTime time.Time
	const EXPORT_PERIOD = 100 * time.Millisecond      // Frequency of stats collection
	const MAX_CLOSE_DURATION = 500 * time.Millisecond // How long to allow close to take
	if statsAddr != "" {
		ddExporter, err := datadog.NewExporter(datadog.Options{
			Service:   "storage-probe",
			StatsAddr: statsAddr,
		})
		if err != nil {
			log.Fatal(fmt.Sprintf("Unable to set up DD stats: %v", err))
		}
		log.Printf("Started DD exporter to %s", statsAddr)
		// Close up the exporter on exit. There's some trickiness and concerns.
		defer func() {
			// Datadog's stats exporter doesn't guarantee flush, so hold off for at
			// least one  sampling period. When that is fixed, this can be removed.
			// https://github.com/DataDog/opencensus-go-exporter-datadog/issues/53
			time.Sleep(2 * EXPORT_PERIOD)

			// Wait for a maximum of MAX_CLOSE_DURATION for the shutdown to complete.
			finishChan := make(chan interface{})
			go func(c chan interface{}) {
				ddExporter.Stop()
				close(c)
			}(finishChan)

			select {
			case <-finishChan:
				break
			case <-time.NewTimer(MAX_CLOSE_DURATION).C:
				log.Printf("Stats shutdown time (%v) exceeded, exiting.",
					MAX_CLOSE_DURATION)
				break
			}
		}()

		view.RegisterExporter(ddExporter)
		view.SetReportingPeriod(EXPORT_PERIOD)
	}
	startTime = time.Now()

	// Update context for stats
	// Set up stats tags
	ctx, err := tag.New(ctx, tag.Insert(podName, os.Getenv("CAAS_POD_ID")))
	if err != nil {
		return err
	}

	// The cluster ID is derived from the Kubernetes namespace (one physical Kafka cluster
	// exists in a specific namespace), so get that information. Export the namespace as
	// "clusterid" for consistency with all of our stats & dashboards.
	ctx, err = tag.New(ctx, tag.Insert(clusterId, os.Getenv("POD_NAMESPACE")))
	if err != nil {
		return err
	}

	err = view.Register(&statsLatencyView, &statsErrorView, &statsTimeoutView)
	if err != nil {
		return err
	}

	err = processFilenames(ctx, processFile, flag.Args()...)
	stats.Record(ctx, mLatencyMs.M(float64(time.Since(startTime).Nanoseconds()/1e6)))

	if err != nil {
		stats.Record(ctx, mErrors.M(1))
		if ctx.Err() == context.DeadlineExceeded {
			stats.Record(ctx, mTimeouts.M(1))
		}
	}
	return err
}

func main() {
	timeout := flag.Duration("timeout", time.Second*30, "max time to run")
	statsAddr := flag.String("statsAddr", "", "host:port for local stats")
	help := flag.Bool("h", false, "help")

	flag.Parse()

	if *help {
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Signal handling
	ctx, cancel := contextHandleSignal(context.Background())
	// Timeouts if needed
	if *timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *timeout)
		defer cancel()
	}

	if err := setupMetricsAndRun(ctx, *statsAddr, flag.Args()...); err != nil {
		log.Fatal(err)
	}
}
