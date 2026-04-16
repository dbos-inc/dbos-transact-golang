package dbos

import (
	"time"
)

/*******************************/
/******* SCHEDULE TYPES ********/
/*******************************/

type ScheduleStatus string

const (
	ScheduleStatusActive  ScheduleStatus = "ACTIVE"
	ScheduleStatusPaused  ScheduleStatus = "PAUSED"
	ScheduleStatusDeleted ScheduleStatus = "DELETED"
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
}

type CreateScheduleRequest struct {
	ScheduleName      string
	WorkflowFn        WorkflowFunc
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
}

type ApplySchedulesRequest struct {
	ScheduleName      string
	WorkflowFn        WorkflowFunc
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
}

type ClientScheduleInput struct {
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
}
