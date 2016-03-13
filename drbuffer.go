package drbuffer

import (
	"os"
	"syscall"
	"fmt"
)

type durableRingBuffer struct {
	ringBuffer
	file *os.File
	mmappedFile []byte
}

type annotatedError struct {
	originalError error
	annotation	string
}

func (err annotatedError) Error() string {
	return fmt.Sprintf("%s: %s", err.annotation, err.originalError.Error())
}

func Open(filePath string, nkiloBytes int) (*durableRingBuffer, error) {
	fileObj, fileSize, err := openOrCreateFile(filePath, nkiloBytes)
	if err != nil {
		return nil, annotatedError{err, "failed to open or create file"}
	}
	mmappedFile, err := syscall.Mmap(int(fileObj.Fd()), 0, fileSize, syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, annotatedError{err, "failed to mmap"}
	}
	syscall.Madvise(mmappedFile, syscall.MADV_SEQUENTIAL)
	return &durableRingBuffer{
		ringBuffer: *NewRingBuffer(mmappedFile[:META_SECTION_SIZE], mmappedFile[META_SECTION_SIZE:]),
		file: fileObj,
		mmappedFile: mmappedFile,
	}, nil
}

func (buffer *durableRingBuffer) Close() error {
	err := syscall.Munmap(buffer.mmappedFile)
	if err != nil {
		return annotatedError{err, "failed to munmap"}
	}
	return buffer.file.Close()
}

func openOrCreateFile(filePath string, nkiloBytes int) (*os.File, int, error) {
	fileObj, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			fileObj, err = os.OpenFile(filePath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0644)
			if err != nil {
				return nil, 0, annotatedError{err, "failed to create new file"}
			}
			emptyBytes := make([]byte, 1024)
			for i := 0; i < nkiloBytes; i++ {
				n, err := fileObj.Write(emptyBytes)
				if n != 1024 {
					panic("initialize disk file unsuccessful")
				}
				if err != nil {
					return nil, 0, annotatedError{err, "failed to write empty bytes"}
				}
			}
			err = fileObj.Close()
			if err != nil {
				return nil, 0, annotatedError{err, "failed to close new file"}
			}
			fileObj, err = os.OpenFile(filePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, 0, annotatedError{err, "failed to open newly created file"}
			}
		} else {
			return nil, 0, annotatedError{err, "failed to open existing file"}
		}
	}
	fi, err := fileObj.Stat()
	if err != nil {
		return nil, 0, annotatedError{err, "failed to get file size"}
	}
	return fileObj, int(fi.Size()), nil
}