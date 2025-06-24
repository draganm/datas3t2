package dataranges

import (
	"github.com/draganm/datas3t2/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UploadDatarangeServer struct {
	db        *pgxpool.Pool
	encryptor *crypto.CredentialEncryptor
}

func NewServer(db *pgxpool.Pool, encryptionKey string) (*UploadDatarangeServer, error) {
	encryptor, err := crypto.NewCredentialEncryptor(encryptionKey)
	if err != nil {
		return nil, err
	}

	return &UploadDatarangeServer{
		db:        db,
		encryptor: encryptor,
	}, nil
}
