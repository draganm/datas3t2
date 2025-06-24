package main

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "datas3t2-keygen",
		Usage: "Generate AES-256 encryption keys for datas3t2",
		Description: `Generate a cryptographically secure 32-byte (256-bit) random key for AES-256 encryption.
		
This key can be used with the datas3t2-server --encryption-key flag or ENCRYPTION_KEY environment variable
to encrypt S3 credentials stored in the database.

Keep this key secure and backed up - if you lose it, you won't be able to decrypt your stored credentials!`,
		Action: func(c *cli.Context) error {
			// Generate a 32-byte (256-bit) random key for AES-256
			key := make([]byte, 32)
			_, err := rand.Read(key)
			if err != nil {
				return fmt.Errorf("failed to generate random key: %w", err)
			}

			// Encode the key as base64 for easy storage/transmission
			keyBase64 := base64.StdEncoding.EncodeToString(key)

			fmt.Println("Generated AES-256 encryption key:")
			fmt.Println(keyBase64)
			fmt.Println()
			fmt.Println("You can use this key with the --encryption-key flag or ENCRYPTION_KEY environment variable.")
			fmt.Println("Keep this key secure and backed up - if you lose it, you won't be able to decrypt your stored credentials!")

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
