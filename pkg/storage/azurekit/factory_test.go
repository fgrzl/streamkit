package azurekit

import (
	"context"
	"strings"
	"testing"

	client "github.com/fgrzl/azkit/tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreFactoryShouldRequireAccountNameWithoutHTTPClient(t *testing.T) {
	factory, err := NewStoreFactory(context.Background(), &AzureStoreOptions{})

	require.Error(t, err)
	assert.Nil(t, factory)
	assert.ErrorContains(t, err, "account name is required")
}

func TestNewStoreFactoryShouldRequireAccountKeyWhenNotUsingManagedIdentity(t *testing.T) {
	factory, err := NewStoreFactory(context.Background(), &AzureStoreOptions{AccountName: "account"})

	require.Error(t, err)
	assert.Nil(t, factory)
	assert.ErrorContains(t, err, "account key is required")
}

func TestNewStoreFactoryShouldAllowManagedIdentityWithoutAccountKey(t *testing.T) {
	factory, err := NewStoreFactory(context.Background(), &AzureStoreOptions{
		AccountName:        "account",
		UseManagedIdentity: true,
	})

	require.NoError(t, err)
	require.NotNil(t, factory)
	assert.True(t, factory.options.UseManagedIdentity)
}

func TestNewStoreFactoryShouldAllowInjectedHTTPClientWithoutCredentials(t *testing.T) {
	factory, err := NewStoreFactory(context.Background(), &AzureStoreOptions{
		HTTPClient: &client.HTTPTableClient{},
	})

	require.NoError(t, err)
	require.NotNil(t, factory)
	assert.NotNil(t, factory.options.HTTPClient)
}

func TestSanitizeTableNameShouldNormalizeAccordingToAzureRules(t *testing.T) {
	longName := "a" + strings.Repeat("b", 80)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty", input: "", expected: ""},
		{name: "non letter prefix", input: "1-foo", expected: "Tfoo"},
		{name: "pads short names", input: "ab", expected: "ab0"},
		{name: "strips punctuation", input: "a-b_c!", expected: "abc"},
		{name: "truncates long names", input: longName, expected: longName[:maxTableNameLength]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, sanitizeTableName(tt.input))
		})
	}
}
