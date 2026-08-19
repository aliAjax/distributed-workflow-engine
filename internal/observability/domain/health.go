package domain

import "time"

type HealthStatus string

const (
	HealthOK       HealthStatus = "ok"
	HealthDegraded HealthStatus = "degraded"
	HealthDown     HealthStatus = "down"
)

type Check struct {
	Name      string       `json:"name"`
	Status    HealthStatus `json:"status"`
	Message   string       `json:"message,omitempty"`
	CheckedAt time.Time    `json:"checked_at"`
}

type HealthReport struct {
	Status HealthStatus `json:"status"`
	Checks []Check      `json:"checks"`
	Now    time.Time    `json:"now"`
}
