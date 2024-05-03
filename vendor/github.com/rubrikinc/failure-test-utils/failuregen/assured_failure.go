// Copyright 2024 Rubrik, Inc.

package failuregen

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"
)

// FailurePoint is a named stage in workflow that is of interest wrt
// testing with failures
type FailurePoint string

// Golang does not have a reasonable enum impl, nor a way to iterate on all
// the enum-values here to assert uniqueness. Please be careful when adding
// members here. Uniqueness of string values is _not_ tested.
const (
	// SChTargetStateP1 is an upgrade failure-point
	SChTargetStateP1 FailurePoint = "SChTargetStateP1"
	// BeforeAdditiveSchemaChange is an upgrade failure-point
	BeforeAdditiveSchemaChange = "BeforeAdditiveSchemaChange"
	// AfterAdditiveSchemaChange is an upgrade failure-point
	AfterAdditiveSchemaChange = "AfterAdditiveSchemaChange"
	// SChTargetStateUR2 is an upgrade failure-point
	SChTargetStateUR2 = "SChTargetStateUR2"
	// SChTargetStateUR2Q is an upgrade failure-point
	SChTargetStateUR2Q = "SChTargetStateUR2Q"
	// SChTargetStateMT3 is an upgrade failure-point
	SChTargetStateMT3 = "SChTargetStateMT3"
	// SChTargetStateEM4 is an upgrade failure-point
	SChTargetStateEM4 = "SChTargetStateEM4"
	// BeforeMetadataMigration is an upgrade failure-point
	BeforeMetadataMigration = "BeforeMetadataMigration"
	// AfterMetadataMigration is an upgrade failure-point
	AfterMetadataMigration = "AfterMetadataMigration"
	// SChTargetStateRR5 is an upgrade failure-point
	SChTargetStateRR5 = "SChTargetStateRR5"
	// SChTargetStateC6 is an upgrade failure-point
	SChTargetStateC6 = "SChTargetStateC6"
	// BeforeDestructiveSchemaChange is an upgrade failure-point
	BeforeDestructiveSchemaChange = "BeforeDestructiveSchemaChange"
	// AfterDestructiveSchemaChange is an upgrade failure-point
	AfterDestructiveSchemaChange = "AfterDestructiveSchemaChange"
	// SChTargetStateNU0 is an upgrade failure-point
	SChTargetStateNU0 = "SChTargetStateNU0"
)

const (
	assuredFailureFile = "/var/lib/rubrik/flags/callisto.assured_failure.json"
)

// AssuredFailurePlan is a plan for assured failures
type AssuredFailurePlan interface {
	FailMaybe(FailurePoint) error
}

// AssuredFailurePlanImpl is an implementation of AssuredFailurePlan exposed
// for testing. Please use `NewAssuredFailurePlan` to create an instance of
// AssuredFailurePlan for usage in production.
type AssuredFailurePlanImpl struct {
	// PlanFilePath is exposed for testing, should not be used in production
	PlanFilePath string
}

// FailMaybe injects a failure if the current failure-point is slated for
// failure (as per the plan-file). This should not be used in very busy parts
// of the system (such as processing of every query in a batch or every row in a
// projection) because the implementation is slow and inefficient. This is
// primarily meant for failing / breaking large workflows (such as upgrade).
// Absence of plan-file implies no error.
func (afp *AssuredFailurePlanImpl) FailMaybe(currentPoint FailurePoint) error {
	bytes, err := os.ReadFile(afp.PlanFilePath)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(
			err,
			"Failed to read assured-failure-plan: %s",
			afp.PlanFilePath)
	}
	if len(bytes) == 0 {
		return nil
	}
	var failurePoints []FailurePoint
	err = json.Unmarshal(bytes, &failurePoints)
	if err != nil {
		return errors.Wrapf(err, "Failed to parse assured-failure-plan")
	}
	for _, failurePoint := range failurePoints {
		if failurePoint == currentPoint {
			return errors.Errorf(
				"Injecting failure %s (governed by %s)",
				currentPoint,
				afp.PlanFilePath)
		}
	}
	return nil
}

// NewAssuredFailurePlan creates a new assured-failure-plan
func NewAssuredFailurePlan() AssuredFailurePlan {
	return &AssuredFailurePlanImpl{assuredFailureFile}
}
