package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"io"
	"io/ioutil"
)

// written by Aleksandr Fofanov (aleks-fofanov) on behalf of Speedscale

const (
	// EncryptedJSONFileCacheType represents the storage type as encrypted file
	EncryptedJSONFileCacheType = "encryptedjsonfilecache"
)

type EncryptedJSONFileStore struct {
	JSONFileStore
}

func NewEncryptedJSONFileStore(path string, encryptionKey []byte, options *Options) (StoreInterface, error) {
	if err := validateFile(path); err != nil {
		return nil, err
	}

	s := &EncryptedJSONFileStore{
		JSONFileStore: JSONFileStore{
			path:    path,
			options: options,
			fileIO:  newEncryptedFileIO(path, encryptionKey),
		},
	}

	return s, nil
}

func (s *EncryptedJSONFileStore) GetType() string {
	return EncryptedJSONFileCacheType
}

type encryptedFileIO struct {
	path   string
	encKey []byte
}

func newEncryptedFileIO(path string, encryptionKey []byte) *encryptedFileIO {
	return &encryptedFileIO{path: path, encKey: encryptionKey}
}

func (f *encryptedFileIO) readFile() ([]byte, error) {
	encrypted, err := ioutil.ReadFile(f.path)
	if err != nil {
		return nil, err
	}
	if len(encrypted) > 0 {
		decoded := make([]byte, hex.DecodedLen(len(encrypted)))
		l, err := hex.Decode(decoded, encrypted)
		if err != nil {
			return nil, err
		}

		return decrypt(f.encKey, decoded[:l])
	}

	return encrypted, nil
}

func (f *encryptedFileIO) writeFile(content []byte) error {
	encrypted, err := encrypt(f.encKey, content)
	if err != nil {
		return err
	}

	encoded := make([]byte, hex.EncodedLen(len(encrypted)))
	hex.Encode(encoded, encrypted)

	return ioutil.WriteFile(f.path, encoded, JSONFileCachePermission)
}

func encrypt(key, text []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aesGCM.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := aesGCM.Seal(nonce, nonce, text, nil)

	return ciphertext, nil
}

func decrypt(key, text []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := aesGCM.NonceSize()
	nonce, ciphertext := text[:nonceSize], text[nonceSize:]

	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
