package test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/storage/azure"
	"github.com/fgrzl/streamkit/pkg/storage/pebble"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var secret = []byte("top-secret")
var tester = claims.NewClaimsList("tenant_id", uuid.NewString()).Add("scopes", "streamkit::*")

var storeID = uuid.New()

func newTestHarness(t *testing.T, factory storage.StoreFactory) *TestHarness {

	validator := &jwtkit.HMAC256Validator{
		Secret: secret,
	}

	nodeManager := node.NewNodeManager(nil, factory)

	router := mux.NewRouter(nil)

	router.UseAuthentication(&mux.AuthenticationOptions{
		Validate: validator.Validate,
	})

	router.UseAuthorization(&mux.AuthorizationOptions{})

	router.Healthz().AllowAnonymous()

	wskit.ConfigureWebSocketServer(router, nodeManager)

	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
		nodeManager.Close()
	})

	httpClient := server.Client()

	resp, err := httpClient.Get(server.URL + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	signer := jwtkit.HMAC256Signer{
		Secret: secret,
	}
	ttl := time.Minute
	token, err := signer.CreateToken(claims.NewPrincipalFromList(tester, &ttl), ttl)
	require.NoError(t, err)

	url, err := url.Parse(server.URL)
	require.NoError(t, err)

	addr := "ws://" + url.Host + "/streamkit"
	provider := wskit.NewBidiStreamProvider(addr, token)
	client := streamkit.NewClient(provider)

	harness := &TestHarness{
		Client: client,
	}
	return harness
}

func azureTestHarness(t *testing.T) *TestHarness {
	// Default Azurite configuration for local testing
	accountName := "devstoreaccount1"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint := "http://127.0.0.1:10002/devstoreaccount1"

	credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	options := &azure.AzureStoreOptions{
		Prefix:              uuid.NewString(),
		Endpoint:            endpoint,
		SharedKeyCredential: credential,
		AllowInsecureHTTP:   true,
	}

	factory, err := azure.NewStoreFactory(options)
	require.NoError(t, err)

	return newTestHarness(t, factory)
}

func pebbleTestHarness(t *testing.T) *TestHarness {
	options := &pebble.PebbleStoreOptions{
		Path: t.TempDir(),
	}
	factory, err := pebble.NewStoreFactory(options)
	require.NoError(t, err)

	return newTestHarness(t, factory)
}

func configurations(t *testing.T) map[string]*TestHarness {
	return map[string]*TestHarness{
		"azure":  azureTestHarness(t),
		"pebble": pebbleTestHarness(t),
	}
}

func TestMultipleCallStreams(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should allow for multiplexed calls "+name, func(t *testing.T) {
			for i := range 3 {
				// Arrange
				ctx := t.Context()
				space, segment := "space0", "segment"+strconv.Itoa(i)

				// Act
				entry, err := h.Client.Peek(ctx, storeID, space, segment)

				// Assert
				assert.NoError(t, err)
				assert.Equal(t, &streamkit.Entry{Space: space, Segment: segment}, entry)
			}
		})
	}
}

func TestProduce(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should produce "+name, func(t *testing.T) {

			for i := range 3 {
				// Arrange
				ctx := t.Context()
				space, segment, records := "space0", "segment"+strconv.Itoa(i), generateRange(0, 5)

				// Act
				results := h.Client.Produce(ctx, storeID, space, segment, records)
				statuses, err := enumerators.ToSlice(results)

				// Assert
				assert.NoError(t, err)
				assert.Len(t, statuses, 1)
			}
		})
	}
}

func TestGetSpaces(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should get spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, h.Client)

			// Act
			enumerator := h.Client.GetSpaces(ctx, storeID)
			spaces, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, spaces, 5)
			assert.Equal(t, "space0", spaces[0])
			assert.Equal(t, "space1", spaces[1])
			assert.Equal(t, "space2", spaces[2])
			assert.Equal(t, "space3", spaces[3])
			assert.Equal(t, "space4", spaces[4])
		})
	}
}

func TestGetSegments(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should get segments "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, h.Client)

			// Act
			enumerator := h.Client.GetSegments(ctx, storeID, "space0")
			segments, err := enumerators.ToSlice(enumerator)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, segments, 5)
			assert.Equal(t, "segment0", segments[0])
			assert.Equal(t, "segment1", segments[1])
			assert.Equal(t, "segment2", segments[2])
			assert.Equal(t, "segment3", segments[3])
			assert.Equal(t, "segment4", segments[4])
		})
	}
}

func TestPeek(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should peek "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, h.Client)

			// Act
			peek, err := h.Client.Peek(ctx, storeID, "space0", "segment0")

			// Assert
			assert.NoError(t, err)
			assert.Equal(t, "space0", peek.Space)
			assert.Equal(t, "segment0", peek.Segment)
			assert.Equal(t, uint64(253), peek.Sequence)
		})
	}
}

func TestConsumeSegment(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume segment "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, h.Client)

			args := &streamkit.ConsumeSegment{
				Space:   "space0",
				Segment: "segment0",
			}

			// Act
			results := h.Client.ConsumeSegment(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 253)
		})
	}
}

func TestConsumeSpace(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume space "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()
			setupConsumerData(t, storeID, h.Client)

			args := &streamkit.ConsumeSpace{
				Space: "space0",
			}

			// Act
			results := h.Client.ConsumeSpace(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 1_265)
		})
	}
}

func TestConsume(t *testing.T) {
	for name, h := range configurations(t) {
		t.Run("should consume interleaved spaces "+name, func(t *testing.T) {
			// Arrange
			ctx := t.Context()

			setupConsumerData(t, storeID, h.Client)
			runtime.Gosched()

			args := &streamkit.Consume{
				Offsets: map[string]lexkey.LexKey{
					"space0": {},
					"space1": {},
					"space2": {},
					"space3": {},
					"space4": {},
				},
			}

			// Act
			results := h.Client.Consume(ctx, storeID, args)
			entries, err := enumerators.ToSlice(results)

			// Assert
			assert.NoError(t, err)
			assert.Len(t, entries, 6_325)
		})
	}
}
