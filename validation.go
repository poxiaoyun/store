package store

import (
	"fmt"

	"xiaoshiai.cn/common/errors"
)

// ValidatePreconditions checks exact object identity and version for Get, Patch or Delete.
func ValidatePreconditions(obj Object, preconditions *Preconditions) error {
	if preconditions == nil {
		return nil
	}
	if preconditions.UID != nil && obj.GetUID() != *preconditions.UID {
		return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("UID does not match"))
	}
	if preconditions.ResourceVersion != nil && obj.GetResourceVersion() != *preconditions.ResourceVersion {
		return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("resourceVersion does not match"))
	}
	return nil
}

// ValidateDeleteRequirements checks caller-supplied object selectors.
func ValidateDeleteRequirements(obj Object, labelRequirements, fieldRequirements Requirements) error {
	if err := ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	unstructured, err := ToUnstructured(obj)
	if err != nil {
		return err
	}
	if !MatchLabelReqirements(obj, labelRequirements) ||
		!MatchUnstructuredFieldRequirments(unstructured, fieldRequirements) {
		return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("write conditions did not match"))
	}
	return nil
}
