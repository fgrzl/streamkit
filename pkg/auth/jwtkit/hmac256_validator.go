package jwtkit

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

type HMAC256Validator struct {
	Secret []byte
}

func (tv *HMAC256Validator) Validate(tokenStr string) (jwt.MapClaims, error) {
	// Initialize parser with strict decoding
	parser := jwt.NewParser(jwt.WithStrictDecoding())

	// Parse and validate the token
	token, err := parser.Parse(tokenStr, func(token *jwt.Token) (any, error) {
		// Verify signing method is HMAC
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return tv.Secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}
	if err := ValidateStandardClaims(claims); err != nil {
		return nil, err
	}

	return claims, nil
}
