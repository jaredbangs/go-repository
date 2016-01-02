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
