package common_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
)

// Returns true when EnableVectorAgent is true for NameNodeContainerLoggingSpec
func TestReturnsTrueForNameNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.NameNodeContainerLoggingSpec{
		EnableVectorAgent: true,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.True(t, result)
}

func TestReturnsErrorForUnknownType(t *testing.T) {
	unknownType := struct{}{}

	result, err := common.IsVectorEnable(unknownType)

	assert.EqualError(t, err, "unknown role logging type struct {} to check vector")
	assert.False(t, result)
}

// Returns false when EnableVectorAgent is false for NameNodeContainerLoggingSpec
func TestReturnsFalseForNameNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.NameNodeContainerLoggingSpec{
		EnableVectorAgent: false,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.False(t, result)
}

// Returns true when EnableVectorAgent is true for DataNodeContainerLoggingSpec
func TestReturnsTrueForDataNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.DataNodeContainerLoggingSpec{
		EnableVectorAgent: true,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.True(t, result)
}

// Returns false when EnableVectorAgent is false for DataNodeContainerLoggingSpec
func TestReturnsFalseForDataNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.DataNodeContainerLoggingSpec{
		EnableVectorAgent: false,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.False(t, result)
}

// Returns true when EnableVectorAgent is true for JournalNodeContainerLoggingSpec
func TestReturnsTrueForJournalNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.JournalNodeContainerLoggingSpec{
		EnableVectorAgent: true,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.True(t, result)
}

// Handles nil input gracefully without panicking
func TestHandlesNilInputGracefully(t *testing.T) {
	result, err := common.IsVectorEnable(nil)

	assert.EqualError(t, err, "role logging config is nil")
	assert.False(t, result)
}

// Returns false when EnableVectorAgent is false for JournalNodeContainerLoggingSpec
func TestReturnsFalseForJournalNodeContainerLoggingSpec(t *testing.T) {
	loggingSpec := &v1alpha1.JournalNodeContainerLoggingSpec{
		EnableVectorAgent: false,
	}

	result, err := common.IsVectorEnable(loggingSpec)

	assert.NoError(t, err)
	assert.False(t, result)
}

// Returns an error when roleLoggingConfig is of an unknown type
func TestReturnsErrorForUnknownRoleLoggingType(t *testing.T) {
	unknownType := "unknown type"

	result, err := common.IsVectorEnable(unknownType)

	assert.EqualError(t, err, "unknown role logging type string to check vector")
	assert.False(t, result)
}

// Handles empty struct input without panicking
func TestReturnsFalseForEmptyStructInput(t *testing.T) {
	var emptyStruct v1alpha1.NameNodeContainerLoggingSpec

	result, err := common.IsVectorEnable(&emptyStruct)

	assert.NoError(t, err)
	assert.False(t, result)
}
