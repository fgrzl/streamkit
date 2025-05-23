package wskit

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/fgrzl/claims"
	"github.com/google/uuid"
)

const (
	ScopeAllStores = "streamkit::*"
	ScopePrefix    = "streamkit::"
)

func NewClientMuxerSession() MuxerSession {
	return &muxerSession{allowAll: true}
}

func NewServerMuxerSession(principal claims.Principal) (MuxerSession, error) {
	allowedStores := make(map[uuid.UUID]struct{})

	for _, scope := range principal.Scopes() {
		if scope == ScopeAllStores {
			return &muxerSession{allowAll: true}, nil
		}

		if strings.HasPrefix(scope, ScopePrefix) {
			raw := strings.TrimPrefix(scope, ScopePrefix)
			id, err := uuid.Parse(raw)
			if err != nil {
				slog.Warn("ignoring invalid store scope", "scope", scope, "error", err)
				continue
			}
			allowedStores[id] = struct{}{}
		}
	}

	if len(allowedStores) == 0 {
		return nil, fmt.Errorf("invalid scope: expected %q or %q{storeID}", ScopeAllStores, ScopePrefix)
	}

	return &muxerSession{
		allowAll:      false,
		allowedStores: allowedStores,
	}, nil
}

type MuxerSession interface {
	CanAccessStore(storeID uuid.UUID) bool
	AllowedStores() []uuid.UUID
	AllowAllStores() bool
}

type muxerSession struct {
	allowAll      bool
	allowedStores map[uuid.UUID]struct{}
}

func (s *muxerSession) CanAccessStore(storeID uuid.UUID) bool {
	if s.allowAll {
		return true
	}
	_, ok := s.allowedStores[storeID]
	return ok
}

func (s *muxerSession) AllowedStores() []uuid.UUID {
	if s.allowAll {
		return nil // semantically means all
	}
	ids := make([]uuid.UUID, 0, len(s.allowedStores))
	for id := range s.allowedStores {
		ids = append(ids, id)
	}
	return ids
}

func (s *muxerSession) AllowAllStores() bool {
	return s.allowAll
}
