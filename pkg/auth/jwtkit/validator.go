package jwtkit

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ValidateStandardClaims validates exp, nbf, and iat claims inside jwt.MapClaims.
// It returns an error if any are invalid or missing (for exp).
func ValidateStandardClaims(claims jwt.MapClaims) error {
	now := time.Now().Unix()

	// Require and check "exp"
	if exp, ok := claims["exp"].(float64); ok {
		if now > int64(exp) {
			return fmt.Errorf("token has expired")
		}
	} else {
		return fmt.Errorf("expiration claim missing or invalid")
	}

	// Optional: "nbf" (not before)
	if nbf, ok := claims["nbf"].(float64); ok {
		if now < int64(nbf) {
			return fmt.Errorf("token not valid yet")
		}
	}

	// Optional: "iat" (issued at)
	if iat, ok := claims["iat"].(float64); ok {
		if now < int64(iat) {
			return fmt.Errorf("token issued in the future")
		}
	}

	return nil
}
