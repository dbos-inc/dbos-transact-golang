package models

import (
	"time"

	"github.com/robfig/cron/v3"
)

type ScheduleStatus string

const (
	ScheduleStatusActive ScheduleStatus = "ACTIVE"
	ScheduleStatusPaused ScheduleStatus = "PAUSED"
)

type WorkflowSchedule struct {
	ScheduleID        string         `json:"schedule_id"`
	ScheduleName      string         `json:"schedule_name"`
	WorkflowName      string         `json:"workflow_name"`
	WorkflowClassName string         `json:"workflow_class_name,omitempty"`
	Schedule          string         `json:"schedule"`
	Status            ScheduleStatus `json:"status"`
	Context           any            `json:"context"`
	LastFiredAt       *time.Time     `json:"last_fired_at,omitempty"`
	AutomaticBackfill bool           `json:"automatic_backfill"`
	CronTimezone      string         `json:"cron_timezone,omitempty"`
	QueueName         string         `json:"queue_name,omitempty"`
}

// ScheduledWorkflowInput is the input type that DB-backed scheduled workflow
// functions must accept. ScheduledTime is the cron tick time; Context carries
// the user-defined value attached to the schedule (nil if none).
type ScheduledWorkflowInput struct {
	ScheduledTime time.Time `json:"scheduled_time"`
	Context       any       `json:"context,omitempty"`
}

// NewScheduleCronParser returns the cron parser used for DBOS schedules.
func NewScheduleCronParser() cron.Parser {
	return cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}
