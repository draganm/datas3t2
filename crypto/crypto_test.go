package crypto

import (
	"crypto/rand"
	"encoding/base64"
	"testing"
)

func TestCredentialEncryptor(t *testing.T) {
	// Generate a test key
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		t.Fatal(err)
	}
	keyBase64 := base64.StdEncoding.EncodeToString(key)

	encryptor, err := NewCredentialEncryptor(keyBase64)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		plaintext string
	}{
		{"Empty string", ""},
		{"Short string", "hello"},
		{"AWS access key", "AKIAIOSFODNN7EXAMPLE"},
		{"AWS secret key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		{"Long string", "This is a very long string that should still be encrypted and decrypted correctly"},
		{"Special characters", "!@#$%^&*()_+-=[]{}|;':\",./<>?"},
		{"Unicode", "🔐🔑🛡️🚀"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test encryption
			encrypted, err := encryptor.Encrypt(tt.plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Empty strings should remain empty
			if tt.plaintext == "" && encrypted != "" {
				t.Errorf("Empty string should remain empty, got: %s", encrypted)
				return
			}

			// Non-empty strings should be encrypted
			if tt.plaintext != "" && encrypted == tt.plaintext {
				t.Error("Encrypted text should not equal plaintext")
			}

			// Test decryption
			decrypted, err := encryptor.Decrypt(encrypted)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}

			if decrypted != tt.plaintext {
				t.Errorf("Decryption failed: expected %q, got %q", tt.plaintext, decrypted)
			}
		})
	}
}

func TestEncryptCredentials(t *testing.T) {
	// Generate a test key
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		t.Fatal(err)
	}
	keyBase64 := base64.StdEncoding.EncodeToString(key)

	encryptor, err := NewCredentialEncryptor(keyBase64)
	if err != nil {
		t.Fatal(err)
	}

	accessKey := "AKIAIOSFODNN7EXAMPLE"
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// Test encryption
	encryptedAccessKey, encryptedSecretKey, err := encryptor.EncryptCredentials(accessKey, secretKey)
	if err != nil {
		t.Fatalf("EncryptCredentials failed: %v", err)
	}

	// Test decryption
	decryptedAccessKey, decryptedSecretKey, err := encryptor.DecryptCredentials(encryptedAccessKey, encryptedSecretKey)
	if err != nil {
		t.Fatalf("DecryptCredentials failed: %v", err)
	}

	if decryptedAccessKey != accessKey {
		t.Errorf("Access key decryption failed: expected %q, got %q", accessKey, decryptedAccessKey)
	}

	if decryptedSecretKey != secretKey {
		t.Errorf("Secret key decryption failed: expected %q, got %q", secretKey, decryptedSecretKey)
	}
}

func TestInvalidKey(t *testing.T) {
	// Test with invalid base64 key
	_, err := NewCredentialEncryptor("invalid-base64!")
	if err == nil {
		t.Error("Expected error for invalid base64 key")
	}

	// Test with empty key
	_, err = NewCredentialEncryptor("")
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestDecryptInvalidData(t *testing.T) {
	// Generate a test key
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		t.Fatal(err)
	}
	keyBase64 := base64.StdEncoding.EncodeToString(key)

	encryptor, err := NewCredentialEncryptor(keyBase64)
	if err != nil {
		t.Fatal(err)
	}

	// Test with invalid base64
	_, err = encryptor.Decrypt("invalid-base64!")
	if err == nil {
		t.Error("Expected error for invalid base64")
	}

	// Test with too short ciphertext
	_, err = encryptor.Decrypt(base64.StdEncoding.EncodeToString([]byte("short")))
	if err == nil {
		t.Error("Expected error for too short ciphertext")
	}
}

// TestUniqueEncryption verifies that the same plaintext produces different ciphertext each time
func TestUniqueEncryption(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		t.Fatal(err)
	}
	keyBase64 := base64.StdEncoding.EncodeToString(key)

	encryptor, err := NewCredentialEncryptor(keyBase64)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := "test-data"

	// Encrypt the same data multiple times
	encrypted1, err := encryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	encrypted2, err := encryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	// The encrypted values should be different (due to random nonces)
	if encrypted1 == encrypted2 {
		t.Error("Multiple encryptions of the same data should produce different results")
	}

	// But both should decrypt to the same plaintext
	decrypted1, err := encryptor.Decrypt(encrypted1)
	if err != nil {
		t.Fatal(err)
	}

	decrypted2, err := encryptor.Decrypt(encrypted2)
	if err != nil {
		t.Fatal(err)
	}

	if decrypted1 != plaintext || decrypted2 != plaintext {
		t.Error("Both encrypted values should decrypt to the original plaintext")
	}
}
