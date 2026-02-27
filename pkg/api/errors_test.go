package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnMessageWhenStreamsErrorErrorCalled(t *testing.T) {
	// Arrange
	e := &StreamsError{Code: ErrCodeTransient, Message: "something broke"}

	// Act
	msg := e.Error()

	// Assert
	assert.Equal(t, "something broke", msg)
}

func TestShouldCreateTransientErrorWithCodeWhenNewTransientErrorCalled(t *testing.T) {
	// Arrange
	message := "retry later"

	// Act
	err := NewTransientError(message)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), message)
	var se *StreamsError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, ErrCodeTransient, se.Code)
}

func TestShouldCreatePermanentErrorWithCodeWhenNewPermanentErrorCalled(t *testing.T) {
	// Arrange
	message := "bad request"

	// Act
	err := NewPermanentError(message)

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), message)
	var se *StreamsError
	require.True(t, errors.As(err, &se))
	assert.Equal(t, ErrCodePermanent, se.Code)
}
