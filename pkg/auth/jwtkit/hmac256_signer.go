package jwtkit

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type HMAC256Signer struct {
	Secret []byte
}

func (tm *HMAC256Signer) CreateToken(claims jwt.MapClaims, ttl time.Duration) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(tm.Secret)
}
