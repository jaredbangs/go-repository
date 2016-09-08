package boltrepository

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

type Sample struct {
	Bool   bool
	Number int
	Text   string
}

func TestDeleteRoundTrip(t *testing.T) {

	filePath := randomString(10) + ".boltdb"

	repository := NewRepository(filePath)

	sample := &Sample{Text: "Hello", Bool: true, Number: 42}

	repository.Save("Bucket1", "Item1", sample)

	deserialized := &Sample{}

	repository.ReadInto("Bucket1", "Item1", &deserialized)

	assert.Equal(t, "Hello", deserialized.Text, "Text should match")
	assert.Equal(t, true, deserialized.Bool, "Bool should match")
	assert.Equal(t, 42, deserialized.Number, "Number should match")

	repository.Delete("Bucket1", "Item1")

	deserialized = &Sample{}

	repository.ReadInto("Bucket1", "Item1", &deserialized)

	assert.Equal(t, "", deserialized.Text, "Text should match")
	assert.Equal(t, false, deserialized.Bool, "Bool should match")
	assert.Equal(t, 0, deserialized.Number, "Number should match")

	os.Remove(filePath)
}

func TestForEach(t *testing.T) {

	filePath := randomString(10) + ".boltdb"

	repository := NewRepository(filePath)

	sample := &Sample{Text: "Hello", Bool: true, Number: 42}
	repository.Save("Bucket1", "Item1", sample)

	sample2 := &Sample{Text: "Goodbye", Bool: false, Number: 3}
	repository.Save("Bucket1", "Item2", sample2)

	repository.GetObject = func(val []byte) interface{} {
		sample := &Sample{}
		repository.Deserialize(val, &sample)
		return *sample
	}

	var ds1, ds2 Sample

	repository.ForEach("Bucket1", func(key string, val interface{}) {

		if key == "Item1" {
			ds1 = val.(Sample)
		}
		if key == "Item2" {
			ds2 = val.(Sample)
		}
	})

	assert.Equal(t, "Hello", ds1.Text, "Text should match")
	assert.Equal(t, true, ds1.Bool, "Bool should match")
	assert.Equal(t, 42, ds1.Number, "Number should match")

	assert.Equal(t, "Goodbye", ds2.Text, "Text should match")
	assert.Equal(t, false, ds2.Bool, "Bool should match")
	assert.Equal(t, 3, ds2.Number, "Number should match")

	os.Remove(filePath)
}

func TestRead(t *testing.T) {

	filePath := randomString(10) + ".boltdb"

	repository := NewRepository(filePath)

	sample := &Sample{Text: "Hello", Bool: true, Number: 42}

	repository.Save("Bucket1", "Item1", sample)

	repository.GetObject = func(val []byte) interface{} {
		sample := &Sample{}
		repository.Deserialize(val, &sample)
		return *sample
	}

	obj, _ := repository.Read("Bucket1", "Item1")

	deserialized := obj.(Sample)

	assert.Equal(t, "Hello", deserialized.Text, "Text should match")
	assert.Equal(t, true, deserialized.Bool, "Bool should match")
	assert.Equal(t, 42, deserialized.Number, "Number should match")

	os.Remove(filePath)
}

func TestSaveRoundTrip(t *testing.T) {

	filePath := randomString(10) + ".boltdb"

	repository := NewRepository(filePath)

	sample := &Sample{Text: "Hello", Bool: true, Number: 42}

	repository.Save("Bucket1", "Item1", sample)

	deserialized := &Sample{}

	repository.ReadInto("Bucket1", "Item1", &deserialized)

	assert.Equal(t, "Hello", deserialized.Text, "Text should match")
	assert.Equal(t, true, deserialized.Bool, "Bool should match")
	assert.Equal(t, 42, deserialized.Number, "Number should match")

	os.Remove(filePath)
}

func TestSerializeRoundTrip(t *testing.T) {

	filePath := randomString(10) + ".boltdb"

	repository := NewRepository(filePath)

	sample := &Sample{Text: "Hello", Bool: true, Number: 42}

	serialized, _ := repository.Serialize(sample)

	deserialized := &Sample{}
	repository.Deserialize(serialized, &deserialized)

	assert.Equal(t, "Hello", deserialized.Text, "Text should match")
	assert.Equal(t, true, deserialized.Bool, "Bool should match")
	assert.Equal(t, 42, deserialized.Number, "Number should match")

	os.Remove(filePath)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}
