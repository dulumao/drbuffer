package drbuffer

import (
	"testing"
	"os"
)

func Test_new_file(t *testing.T) {
	assert := NewAssert(t)
	buffer := openNew(assert)
	defer buffer.Close()
}

func Test_open_existing_file(t *testing.T) {
	assert := NewAssert(t)
	buffer := openNew(assert)
	defer buffer.Close()
	buffer.PushOne([]byte("Hello"))
	assert(buffer.Close(), "==", nil)
	buffer, err := Open("/tmp/drbuffer", 1)
	assert(err, "==", nil)
	assert(string(buffer.PopOne()), "==", "Hello")
	assert(buffer.PopOne(), "==", nil)
}

func openNew(assert Assert) *durableRingBuffer {
	assert(ensureFileNotExist("/tmp/drbuffer"), "==", nil)
	buffer, err := Open("/tmp/drbuffer", 1)
	assert(err, "==", nil)
	assert(buffer, "!=", nil)
	return buffer
}

func ensureFileNotExist(filePath string) error {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// file does not exist
		return nil
	} else if err != nil {
		return err
	} else {
		err = os.Remove(filePath)
		if err != nil {
			return err
		} else {
			return nil
		}
	}
}