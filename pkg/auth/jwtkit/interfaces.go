package jwtkit

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Validator interface {
	Validate(tokenStr string) (jwt.MapClaims, error)
}

type Signer interface {
	CreateToken(claims jwt.MapClaims, ttl time.Duration) (string, error)
}
