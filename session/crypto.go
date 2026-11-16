// Package session provides AES-GCM encrypted session storage wrappers and
// MTProto session revocation classification compatible with GotdBot.
package session

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/gotd/td/session"
)

// Encrypt encrypts data using AES-GCM with key and returns a base64-encoded
// nonce||ciphertext blob compatible with GotdBot's session storage.
func Encrypt(key, data []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, data, nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt decodes a base64 AES-GCM blob produced by Encrypt.
func Decrypt(key []byte, input string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(raw) < nonceSize {
		return nil, fmt.Errorf("invalid encrypted session blob")
	}
	return gcm.Open(nil, raw[:nonceSize], raw[nonceSize:], nil)
}

// EncryptedStorage implements gotd's session.Storage by encrypting session
// payloads with AES-GCM before storing them via an underlying Storage or custom
// string blob callbacks.
type EncryptedStorage struct {
	aead      cipher.AEAD
	inner     session.Storage
	loadBlob  func(ctx context.Context) (string, error)
	storeBlob func(ctx context.Context, blob string) error
}

// NewEncryptedStorage wraps an existing gotd session.Storage with AES-GCM encryption.
func NewEncryptedStorage(key []byte, inner session.Storage) (*EncryptedStorage, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	return &EncryptedStorage{aead: aead, inner: inner}, nil
}

// NewEncryptedBlobStorage builds an EncryptedStorage backed by custom base64
// blob load/store callbacks (such as SQLite + MongoDB storage in GotdBot).
func NewEncryptedBlobStorage(
	key []byte,
	load func(ctx context.Context) (string, error),
	store func(ctx context.Context, blob string) error,
) (*EncryptedStorage, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	return &EncryptedStorage{aead: aead, loadBlob: load, storeBlob: store}, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// LoadSession loads and decrypts session bytes.
func (s *EncryptedStorage) LoadSession(ctx context.Context) ([]byte, error) {
	var raw []byte
	if s.loadBlob != nil {
		blob, err := s.loadBlob(ctx)
		if err != nil {
			return nil, err
		}
		if blob == "" {
			return nil, session.ErrNotFound
		}
		raw, err = base64.StdEncoding.DecodeString(blob)
		if err != nil {
			return nil, err
		}
	} else if s.inner != nil {
		b, err := s.inner.LoadSession(ctx)
		if err != nil {
			return nil, err
		}
		raw, err = base64.StdEncoding.DecodeString(string(b))
		if err != nil {
			return nil, err
		}
	} else {
		return nil, session.ErrNotFound
	}

	nonceSize := s.aead.NonceSize()
	if len(raw) < nonceSize {
		return nil, fmt.Errorf("invalid encrypted session blob")
	}
	return s.aead.Open(nil, raw[:nonceSize], raw[nonceSize:], nil)
}

// StoreSession encrypts data and stores it.
func (s *EncryptedStorage) StoreSession(ctx context.Context, data []byte) error {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	sealed := s.aead.Seal(nonce, nonce, data, nil)
	blob := base64.StdEncoding.EncodeToString(sealed)

	if s.storeBlob != nil {
		return s.storeBlob(ctx, blob)
	}
	if s.inner != nil {
		return s.inner.StoreSession(ctx, []byte(blob))
	}
	return fmt.Errorf("no underlying storage configured")
}

// IsSessionRevoked reports whether err indicates a permanently invalidated or
// revoked Telegram authorization key rather than a transient error.
func IsSessionRevoked(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToUpper(err.Error())

	if strings.Contains(errStr, "TIMEOUT") ||
		strings.Contains(errStr, "DEADLINE EXCEEDED") ||
		strings.Contains(errStr, "RESET BY PEER") ||
		strings.Contains(errStr, "BROKEN PIPE") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "NETWORK IS UNREACHABLE") ||
		strings.Contains(errStr, "CONNECTION REFUSED") ||
		strings.Contains(errStr, "NO SUCH HOST") {
		return false
	}

	if strings.Contains(errStr, "FLOOD_WAIT") ||
		strings.Contains(errStr, "CHAT_WRITE_FORBIDDEN") ||
		strings.Contains(errStr, "PEER_ID_INVALID") ||
		strings.Contains(errStr, "CHANNEL_PRIVATE") ||
		strings.Contains(errStr, "CHAT_ID_INVALID") {
		return false
	}

	return strings.Contains(errStr, "AUTH_KEY_UNREGISTERED") ||
		strings.Contains(errStr, "SESSION_REVOKED") ||
		strings.Contains(errStr, "SESSION_EXPIRED") ||
		strings.Contains(errStr, "USER_DEACTIVATED") ||
		strings.Contains(errStr, "AUTH_KEY_PERM_EMPTY") ||
		strings.Contains(errStr, "AUTH_KEY_DUPLICATED")
}
