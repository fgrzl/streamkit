package jwtkit

import (
	"fmt"
	"strings"

	"github.com/fgrzl/claims"
	"github.com/golang-jwt/jwt/v5"
)

func NewClaimsPrincipal(raw jwt.MapClaims) claims.Principal {
	claimsMap := make(map[string]claims.Claim, len(raw))

	for k, v := range raw {
		switch val := v.(type) {
		case string:
			claimsMap[k] = claims.NewClaim(k, val)
		case float64:
			claimsMap[k] = claims.NewClaim(k, fmt.Sprintf("%v", val))
		case []interface{}:
			strs := make([]string, 0, len(val))
			for _, item := range val {
				strs = append(strs, fmt.Sprint(item))
			}
			claimsMap[k] = claims.NewClaim(k, strings.Join(strs, ","))
		case interface{}:
			claimsMap[k] = claims.NewClaim(k, fmt.Sprint(val))
		default:
			// unknown type, skip
		}
	}

	p := claims.NewClaimsPrincipal(claimsMap)
	return p
}
