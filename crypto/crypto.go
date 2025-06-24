package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

// CredentialEncryptor handles encryption and decryption of S3 credentials
type CredentialEncryptor struct {
	key []byte
}

// NewCredentialEncryptor creates a new encryptor from a base64-encoded key
func NewCredentialEncryptor(base64Key string) (*CredentialEncryptor, error) {
	if base64Key == "" {
		return nil, fmt.Errorf("encryption key cannot be empty")
	}

	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}

	// Derive a 32-byte key using SHA-256
	hash := sha256.Sum256(key)

	return &CredentialEncryptor{
		key: hash[:],
	}, nil
}

// Encrypt encrypts a credential string using AES-256-GCM
func (ce *CredentialEncryptor) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil // Empty strings remain empty
	}

	block, err := aes.NewCipher(ce.key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate a random nonce
	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the plaintext
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	// Return base64-encoded result (nonce + ciphertext)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts a credential string using AES-256-GCM
func (ce *CredentialEncryptor) Decrypt(encryptedBase64 string) (string, error) {
	if encryptedBase64 == "" {
		return "", nil // Empty strings remain empty
	}

	// Decode from base64
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedBase64)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %w", err)
	}

	block, err := aes.NewCipher(ce.key)
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	// Extract nonce and ciphertext
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	// Decrypt
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

// EncryptCredentials encrypts both access key and secret key
func (ce *CredentialEncryptor) EncryptCredentials(accessKey, secretKey string) (encryptedAccessKey, encryptedSecretKey string, err error) {
	encryptedAccessKey, err = ce.Encrypt(accessKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to encrypt access key: %w", err)
	}

	encryptedSecretKey, err = ce.Encrypt(secretKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to encrypt secret key: %w", err)
	}

	return encryptedAccessKey, encryptedSecretKey, nil
}

// DecryptCredentials decrypts both access key and secret key
func (ce *CredentialEncryptor) DecryptCredentials(encryptedAccessKey, encryptedSecretKey string) (accessKey, secretKey string, err error) {
	accessKey, err = ce.Decrypt(encryptedAccessKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to decrypt access key: %w", err)
	}

	secretKey, err = ce.Decrypt(encryptedSecretKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to decrypt secret key: %w", err)
	}

	return accessKey, secretKey, nil
}
