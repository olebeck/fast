package qq

import "github.com/prometheus/client_golang/prometheus"

var (
	requestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "server_request_count",
			Help: "Total number of requests received",
		},
		[]string{"type"},
	)
	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "server_error_count",
			Help: "Total number of errors encountered",
		},
		[]string{"type"},
	)
	subscriptionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "server_subscription_count",
			Help: "Current number of subscriptions",
		},
		[]string{"queue"},
	)
	connectionCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "server_connection_count",
			Help: "Total number of client connections",
		},
	)
	activeConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "server_active_connections",
			Help: "Current number of active client connections",
		},
	)
	noReceiverCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "server_events_no_receiver_count",
			Help: "Total number of events with no subscribers",
		},
		[]string{"queue"},
	)
	successfulPublishes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "server_successful_publishes",
			Help: "Total number of successfully published events",
		},
		[]string{"queue"},
	)
)

func init() {
	prometheus.MustRegister(
		requestCount, errorCount, subscriptionCount,
		connectionCount, activeConnections, noReceiverCount,
		successfulPublishes,
	)
}
