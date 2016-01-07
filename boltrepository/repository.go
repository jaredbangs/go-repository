package boltrepository

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
)

type Repository struct {
	FilePath  string
	GetObject func([]byte) interface{}
}

func NewRepository(filePath string) *Repository {
	repository := &Repository{
		FilePath: filePath,
	}
	return repository
}

func (r *Repository) Deserialize(jsonBytes []byte, target interface{}) error {

	err := json.Unmarshal(jsonBytes, &target)

	return err
}

func (r *Repository) ForEach(bucketName string, action func(string, interface{})) error {

	db, err := bolt.Open(r.FilePath, 0644, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// retrieve the data
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", []byte(bucketName))
		}

		bucket.ForEach(func(k, v []byte) error {

			key := string(k)
			value := r.GetObject(v)

			action(key, value)

			return nil
		})
		return nil

	})

	return err
}

func (r *Repository) Read(bucketName string, keyName string) (obj interface{}, err error) {

	db, err := bolt.Open(r.FilePath, 0644, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// retrieve the data
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", []byte(bucketName))
		}

		val := bucket.Get([]byte(keyName))

		obj = r.GetObject(val)

		return nil
	})

	return obj, err
}

func (r *Repository) ReadInto(bucketName string, keyName string, target interface{}) error {

	db, err := bolt.Open(r.FilePath, 0644, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// retrieve the data
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("Bucket %q not found!", []byte(bucketName))
		}

		val := bucket.Get([]byte(keyName))

		return r.Deserialize(val, &target)
	})

	return err
}

func (r *Repository) Save(bucketName string, keyName string, value interface{}) {

	db, err := bolt.Open(r.FilePath, 0644, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	serialized, _ := r.Serialize(value)

	// store some data
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		err = bucket.Put([]byte(keyName), serialized)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (r *Repository) Serialize(value interface{}) ([]byte, error) {

	jsonBytes, err := json.Marshal(value)

	return jsonBytes, err
}