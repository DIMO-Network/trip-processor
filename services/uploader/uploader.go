package uploader

import (
	"archive/zip"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

func PrepareData(data []byte, deviceID, start, end string) ([]byte, error) {
	compressedData, err := compress(data, deviceID, start, end)
	if err != nil {
		return []byte{}, err
	}

	// generating random 32 byte key for AES-256
	// this will change with PRO-1867 encryption keys created for minted devices
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return []byte{}, err
	}

	return encrypt(compressedData, bytes)
}

func Upload(data []byte) error {
	// TODO
	return nil
}

func compress(data []byte, deviceID, start, end string) ([]byte, error) {
	b := new(bytes.Buffer)
	zw := zip.NewWriter(b)

	file, err := zw.Create(fmt.Sprintf("%s_%s.json", start, end))
	if err != nil {
		return nil, err
	}

	_, err = file.Write(data)
	if err != nil {
		return nil, err
	}

	err = zw.Close()
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func encrypt(data, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return []byte{}, err
	}

	aesGCM, err := cipher.NewGCM(block)
	nonce := make([]byte, aesGCM.NonceSize())

	return aesGCM.Seal(nonce, nonce, data, nil), nil
}
