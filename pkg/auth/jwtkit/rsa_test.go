package jwtkit

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func generateRSAKeyPair(t *testing.T) (*rsa.PrivateKey, *rsa.PublicKey) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return privateKey, &privateKey.PublicKey
}

func TestRSASignAndValidate(t *testing.T) {
	priv, pub := generateRSAKeyPair(t)

	var signer Signer = &RSASigner{PrivateKey: priv}
	var validator Validator = &RSAValidator{PublicKey: pub}

	claims := jwt.MapClaims{"tenant_id": "tenant-xyz"}

	token, err := signer.CreateToken(claims, time.Minute)
	require.NoError(t, err)

	validatedClaims, err := validator.Validate(token)
	require.NoError(t, err)
	require.Equal(t, "tenant-xyz", validatedClaims["tenant_id"])
}

func TestRSAExpiredToken(t *testing.T) {
	priv, pub := generateRSAKeyPair(t)

	var signer Signer = &RSASigner{PrivateKey: priv}
	var validator Validator = &RSAValidator{PublicKey: pub}

	claims := jwt.MapClaims{"tenant_id": "expired-tenant"}

	token, err := signer.CreateToken(claims, 1*time.Second)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	_, err = validator.Validate(token)
	require.Error(t, err)
	require.ErrorContains(t, err, "token is expired")
}

func TestRSAInvalidSignature(t *testing.T) {
	priv1, _ := generateRSAKeyPair(t)
	_, pub2 := generateRSAKeyPair(t)

	var signer Signer = &RSASigner{PrivateKey: priv1}
	var validator Validator = &RSAValidator{PublicKey: pub2}

	claims := jwt.MapClaims{"tenant_id": "bad-sig"}

	token, err := signer.CreateToken(claims, time.Minute)
	require.NoError(t, err)

	_, err = validator.Validate(token)
	require.Error(t, err)
	require.ErrorContains(t, err, "signature is invalid")
}
