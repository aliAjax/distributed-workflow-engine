package adapter

import "github.com/acme/distributed-workflow-engine/internal/platform"

type MetricsRecorder struct {
	metrics *platform.Metrics
}

func NewMetricsRecorder(metrics *platform.Metrics) *MetricsRecorder {
	return &MetricsRecorder{metrics: metrics}
}

func (r *MetricsRecorder) IncNode(tenantID, workflowID, nodeID, status string) {
	r.metrics.NodesProcessed.WithLabelValues(tenantID, workflowID, nodeID, status).Inc()
}

func (r *MetricsRecorder) SetQueueDepth(tenantID, queue string, depth float64) {
	r.metrics.QueueDepth.WithLabelValues(tenantID, queue).Set(depth)
}

func (r *MetricsRecorder) SetActiveLeases(count float64) {
	r.metrics.ActiveLeases.Add(count)
}

func (r *MetricsRecorder) IncEvent(tenantID, eventType string) {
	r.metrics.EventPublished.WithLabelValues(tenantID, eventType).Inc()
}
