// +k8s:openapi-gen=true
package controller

import (
	"xiaoshiai.cn/common/meta"
)

type StatusCondition = Condition

// +k8s:openapi-gen=true
type ConditionStatus = string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// +k8s:openapi-gen=true
type Condition struct {
	Type               string          `json:"type" protobuf:"bytes,1,opt,name=type"`
	Status             ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`
	ObservedGeneration int64           `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`
	LastTransitionTime meta.Time       `json:"lastTransitionTime" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	Reason             string          `json:"reason" protobuf:"bytes,5,opt,name=reason"`
	Message            string          `json:"message" protobuf:"bytes,6,opt,name=message"`
}

func IsStatusConditionTrue(conditions []Condition, conditionType string) bool {
	return IsStatusConditionPresentAndEqual(conditions, conditionType, ConditionTrue)
}

// IsStatusConditionFalse returns true when the conditionType is present and set to `ConditionFalse`
func IsStatusConditionFalse(conditions []Condition, conditionType string) bool {
	return IsStatusConditionPresentAndEqual(conditions, conditionType, ConditionFalse)
}

// IsStatusConditionPresentAndEqual returns true when conditionType is present and equal to status.
func IsStatusConditionPresentAndEqual(conditions []Condition, conditionType string, status ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}

func SetStatusCondition(conditions *[]Condition, newCondition Condition) (changed bool) {
	if conditions == nil {
		return false
	}
	existingCondition := FindStatusCondition(*conditions, newCondition.Type)
	if existingCondition == nil {
		if newCondition.LastTransitionTime.IsZero() {
			newCondition.LastTransitionTime = meta.Now()
		}
		*conditions = append(*conditions, newCondition)
		return true
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		if !newCondition.LastTransitionTime.IsZero() {
			existingCondition.LastTransitionTime = newCondition.LastTransitionTime
		} else {
			existingCondition.LastTransitionTime = meta.Now()
		}
		changed = true
	}

	if existingCondition.Reason != newCondition.Reason {
		existingCondition.Reason = newCondition.Reason
		changed = true
	}
	if existingCondition.Message != newCondition.Message {
		existingCondition.Message = newCondition.Message
		changed = true
	}
	if existingCondition.ObservedGeneration != newCondition.ObservedGeneration {
		existingCondition.ObservedGeneration = newCondition.ObservedGeneration
		changed = true
	}

	return changed
}

func RemoveStatusCondition(conditions *[]Condition, conditionType string) (removed bool) {
	if conditions == nil || len(*conditions) == 0 {
		return false
	}
	newConditions := make([]Condition, 0, len(*conditions)-1)
	for _, condition := range *conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}

	removed = len(*conditions) != len(newConditions)
	*conditions = newConditions

	return removed
}

func FindStatusCondition(conditions []Condition, conditionType string) *Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}

	return nil
}
