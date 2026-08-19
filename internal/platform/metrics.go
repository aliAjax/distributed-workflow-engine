package platform

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	registry          *prometheus.Registry
	HTTPRequests      *prometheus.CounterVec
	HTTPDuration      *prometheus.HistogramVec
	ExecutionsCreated *prometheus.CounterVec
	NodesProcessed    *prometheus.CounterVec
	QueueDepth        *prometheus.GaugeVec
	ActiveLeases      prometheus.Gauge
	EventPublished    *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: registry,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_http_requests_total",
			Help: "Total HTTP requests handled by the API.",
		}, []string{"method", "path", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "workflow_http_request_duration_seconds",
			Help:    "HTTP request latency distribution.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		ExecutionsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_executions_created_total",
			Help: "Workflow executions created by trigger source.",
		}, []string{"tenant_id", "workflow_id", "trigger_source"}),
		NodesProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_nodes_processed_total",
			Help: "Workflow nodes processed by final status.",
		}, []string{"tenant_id", "workflow_id", "node_id", "status"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "workflow_queue_depth",
			Help: "Current workflow execution queue depth.",
		}, []string{"tenant_id", "queue"}),
		ActiveLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "workflow_active_leases",
			Help: "Current active node leases held by this process.",
		}),
		EventPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_events_published_total",
			Help: "Events published to the internal event bus.",
		}, []string{"tenant_id", "type"}),
	}
	registry.MustRegister(m.HTTPRequests, m.HTTPDuration, m.ExecutionsCreated, m.NodesProcessed, m.QueueDepth, m.ActiveLeases, m.EventPublished)
	return m
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}
