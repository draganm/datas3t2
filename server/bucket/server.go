package bucket

import (
	"github.com/draganm/datas3t2/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BucketServer struct {
	db        *pgxpool.Pool
	encryptor *crypto.CredentialEncryptor
}

func NewServer(db *pgxpool.Pool, encryptionKey string) (*BucketServer, error) {
	encryptor, err := crypto.NewCredentialEncryptor(encryptionKey)
	if err != nil {
		return nil, err
	}

	return &BucketServer{
		db:        db,
		encryptor: encryptor,
	}, nil
}
