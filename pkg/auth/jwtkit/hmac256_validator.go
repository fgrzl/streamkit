package jwtkit

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

type HMAC256Validator struct {
	Secret []byte
}

func (tv *HMAC256Validator) Validate(tokenStr string) (jwt.MapClaims, error) {
	parser := jwt.NewParser()
	token, err := parser.Parse(tokenStr, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return tv.Secret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}
