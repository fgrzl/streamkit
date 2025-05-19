package azure

import "github.com/Azure/azure-sdk-for-go/sdk/data/aztables"

func NewSharedKeyCredential(accountName, accountKey string) (*aztables.SharedKeyCredential, error) {
	return aztables.NewSharedKeyCredential(accountName, accountKey)
}
