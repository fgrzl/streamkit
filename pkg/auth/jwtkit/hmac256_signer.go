package jwtkit

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type HMAC256Signer struct {
	Secret []byte
}

func (tm *HMAC256Signer) CreateToken(claims jwt.MapClaims, ttl time.Duration) (string, error) {
	// Inject 'exp' if not already set
	if _, ok := claims["exp"]; !ok && ttl > 0 {
		claims["exp"] = time.Now().Add(ttl).Unix()
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(tm.Secret)
}
