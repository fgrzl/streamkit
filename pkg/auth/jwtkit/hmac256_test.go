package jwtkit

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestHMAC256SignAndValidate(t *testing.T) {
	secret := []byte("super-secret-key-for-testing")
	var signer Signer = &HMAC256Signer{Secret: secret}
	var validator Validator = &HMAC256Validator{Secret: secret}

	claims := jwt.MapClaims{"tenant_id": "tenant-xyz"}

	token, err := signer.CreateToken(claims, time.Minute)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	validatedClaims, err := validator.Validate(token)
	require.NoError(t, err)
	require.Equal(t, "tenant-xyz", validatedClaims["tenant_id"])
}

func TestHMAC256ExpiredToken(t *testing.T) {
	secret := []byte("super-secret-key-for-testing")
	var signer Signer = &HMAC256Signer{Secret: secret}
	var validator Validator = &HMAC256Validator{Secret: secret}

	claims := jwt.MapClaims{"tenant_id": "tenant-expired"}

	token, err := signer.CreateToken(claims, 1*time.Second)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	_, err = validator.Validate(token)
	require.Error(t, err)
	require.ErrorContains(t, err, "token is expired")
}

func TestHMAC256InvalidSignature(t *testing.T) {
	var signer Signer = &HMAC256Signer{Secret: []byte("correct-secret")}
	var validator Validator = &HMAC256Validator{Secret: []byte("wrong-secret")}

	claims := jwt.MapClaims{"tenant_id": "invalid-sig"}

	token, err := signer.CreateToken(claims, time.Minute)
	require.NoError(t, err)

	_, err = validator.Validate(token)
	require.Error(t, err)
	require.ErrorContains(t, err, "signature is invalid")
}
