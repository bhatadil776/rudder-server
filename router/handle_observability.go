package router

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
)

func (rt *Handle) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.telemetry.requestsMetricLock.Lock()
		rt.telemetry.requestsMetric = append(rt.telemetry.requestsMetric, reqMetric)
		rt.telemetry.requestsMetricLock.Unlock()
	}
}

func (rt *Handle) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableRouterMetric {
		return
	}

	for {
		select {
		case <-ctx.Done():
			rt.logger.Debugf("[%v Router] :: collectMetrics exiting", rt.destType)
			return
		case <-rt.telemetry.diagnosisTicker.C:
		}
		rt.telemetry.requestsMetricLock.RLock()
		var diagnosisProperties map[string]interface{}
		retries := 0
		aborted := 0
		success := 0
		var compTime time.Duration
		for _, reqMetric := range rt.telemetry.requestsMetric {
			retries += reqMetric.RequestRetries
			aborted += reqMetric.RequestAborted
			success += reqMetric.RequestSuccess
			compTime += reqMetric.RequestCompletedTime
		}
		if len(rt.telemetry.requestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				rt.destType: map[string]interface{}{
					diagnostics.RouterAborted:       aborted,
					diagnostics.RouterRetries:       retries,
					diagnostics.RouterSuccess:       success,
					diagnostics.RouterCompletedTime: (compTime / time.Duration(len(rt.telemetry.requestsMetric))) / time.Millisecond,
				},
			}
			if diagnostics.Diagnostics != nil {
				diagnostics.Diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
			}
		}

		rt.telemetry.requestsMetric = nil
		rt.telemetry.requestsMetricLock.RUnlock()

		// This lock will ensure we don't send out Track Request while filling up the
		// failureMetric struct
		rt.telemetry.failureMetricLock.Lock()
		for key, value := range rt.telemetry.failuresMetric {
			var err error
			stringValueBytes, err := jsonfast.Marshal(value)
			if err != nil {
				stringValueBytes = []byte{}
			}
			if diagnostics.Diagnostics != nil {
				diagnostics.Diagnostics.Track(key, map[string]interface{}{
					diagnostics.RouterDestination: rt.destType,
					diagnostics.Count:             len(value),
					diagnostics.ErrorCountMap:     string(stringValueBytes),
				})
			}
		}
		rt.telemetry.failuresMetric = make(map[string]map[string]int)
		rt.telemetry.failureMetricLock.Unlock()
	}
}

func (rt *Handle) updateRudderSourcesStats(ctx context.Context, tx jobsdb.UpdateSafeTx, jobs []*jobsdb.JobT, jobStatuses []*jobsdb.JobStatusT) error {
	rsourcesStats := rsources.NewStatsCollector(rt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.JobStatusesUpdated(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	if err != nil {
		rt.logger.Errorf("publishing rsources stats: %w", err)
	}
	return err
}

func (rt *Handle) updateProcessedEventsMetrics(statusList []*jobsdb.JobStatusT) {
	eventsPerStateAndCode := map[string]map[string]int{}
	for i := range statusList {
		state := statusList[i].JobState
		code := statusList[i].ErrorCode
		if _, ok := eventsPerStateAndCode[state]; !ok {
			eventsPerStateAndCode[state] = map[string]int{}
		}
		eventsPerStateAndCode[state][code]++
	}
	for state, codes := range eventsPerStateAndCode {
		for code, count := range codes {
			stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
				"module":   "router",
				"destType": rt.destType,
				"state":    state,
				"code":     code,
			}).Count(count)
		}
	}
}

func (rt *Handle) sendRetryStoreStats(attempt int) {
	rt.logger.Warnf("Timeout during store jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func (rt *Handle) sendRetryUpdateStats(attempt int) {
	rt.logger.Warnf("Timeout during update job status in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func (rt *Handle) sendQueryRetryStats(attempt int) {
	rt.logger.Warnf("Timeout during query jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

// pipelineDelayStats reports the delay of the pipeline as a range:
//
// - max - time elapsed since the first job was created
//
// - min - time elapsed since the last job was created
func (rt *Handle) pipelineDelayStats(partition string, first, last *jobsdb.JobT) {
	var firstJobDelay float64
	var lastJobDelay float64
	if first != nil {
		firstJobDelay = time.Since(first.CreatedAt).Seconds()
	}
	if last != nil {
		lastJobDelay = time.Since(last.CreatedAt).Seconds()
	}
	stats.Default.NewTaggedStat("pipeline_delay_min_seconds", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition, "module": "router"}).Gauge(lastJobDelay)
	stats.Default.NewTaggedStat("pipeline_delay_max_seconds", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition, "module": "router"}).Gauge(firstJobDelay)
}