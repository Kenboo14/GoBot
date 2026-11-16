package session

import (
	"bytes"
	"context"
	"errors"
	"testing"

	tdsession "github.com/gotd/td/session"
)

func TestEncryptDecrypt(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	plain := []byte(`{"Version":1,"Data":{"DC":2}}`)

	blob, err := Encrypt(key, plain)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(key, blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("got %q, want %q", got, plain)
	}
}

func TestEncryptedStorage(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	mem := &tdsession.StorageMemory{}
	st, err := NewEncryptedStorage(key, mem)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := st.LoadSession(ctx); !errors.Is(err, tdsession.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	want := []byte("secret-session-payload")
	if err := st.StoreSession(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}

	// Verify Encrypt/Decrypt interop with raw stored blob.
	raw, err := mem.LoadSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decrypt(key, string(raw))
	if err != nil || !bytes.Equal(dec, want) {
		t.Fatalf("interop Decrypt failed: got %q err=%v", dec, err)
	}

	// Test blob callbacks mode.
	var stored string
	bst, err := NewEncryptedBlobStorage(key,
		func(context.Context) (string, error) {
			if stored == "" {
				return "", tdsession.ErrNotFound
			}
			return stored, nil
		},
		func(_ context.Context, blob string) error {
			stored = blob
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := bst.StoreSession(ctx, want); err != nil {
		t.Fatal(err)
	}
	got2, err := bst.LoadSession(ctx)
	if err != nil || !bytes.Equal(got2, want) {
		t.Fatalf("blob storage roundtrip failed: got %q err=%v", got2, err)
	}
}

func TestIsSessionRevoked(t *testing.T) {
	revoked := []error{
		errors.New("rpcDoRequest: rpc error code 401: AUTH_KEY_UNREGISTERED"),
		errors.New("SESSION_REVOKED"),
		errors.New("SESSION_EXPIRED"),
		errors.New("USER_DEACTIVATED"),
		errors.New("AUTH_KEY_PERM_EMPTY"),
		errors.New("AUTH_KEY_DUPLICATED"),
	}
	for _, err := range revoked {
		if !IsSessionRevoked(err) {
			t.Errorf("expected IsSessionRevoked(%v) = true", err)
		}
	}

	transient := []error{
		nil,
		errors.New("context deadline exceeded"),
		errors.New("read: EOF"),
		errors.New("FLOOD_WAIT_30"),
		errors.New("PEER_ID_INVALID"),
	}
	for _, err := range transient {
		if IsSessionRevoked(err) {
			t.Errorf("expected IsSessionRevoked(%v) = false", err)
		}
	}
}
