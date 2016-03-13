package drbuffer

import (
	"os"
	"syscall"
	"fmt"
	"errors"
	"unsafe"
	"reflect"
)

type DurableRingBuffer interface {
	PushN(packets [][]byte)
	PushOne(packet []byte)
	PopN(n int) [][]byte
	PopOne() []byte
	Flush() error
	Close() error
}

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

func Open(filePath string, nkiloBytes int) (DurableRingBuffer, error) {
	isNewFile, fileObj, fileSize, err := openOrCreateFile(filePath, nkiloBytes)
	if err != nil {
		return nil, annotatedError{err, "failed to open or create file"}
	}
	mmappedFile, err := syscall.Mmap(int(fileObj.Fd()), 0, fileSize, syscall.PROT_READ | syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, annotatedError{err, "failed to mmap"}
	}
	syscall.Madvise(mmappedFile, syscall.MADV_SEQUENTIAL)
	buffer := &durableRingBuffer{
		ringBuffer: *NewRingBuffer(mmappedFile[:META_SECTION_SIZE], mmappedFile[META_SECTION_SIZE:]),
		file: fileObj,
		mmappedFile: mmappedFile,
	}
	if isNewFile {
		*buffer.version = 1
	} else {
		if *buffer.version != 1 {
			return nil, errors.New(fmt.Sprintf("unsupported file version: %s", *buffer.version))
		}
	}
	return buffer, nil
}

func (buffer *durableRingBuffer) Close() error {
	err := syscall.Munmap(buffer.mmappedFile)
	if err != nil {
		return annotatedError{err, "failed to munmap"}
	}
	return buffer.file.Close()
}

func (buffer *durableRingBuffer) Flush() error {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&buffer.mmappedFile))
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, header.Data, uintptr(header.Len), syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	} else {
		return nil
	}
}

func openOrCreateFile(filePath string, nkiloBytes int) (bool, *os.File, int, error) {
	isNewFile := false
	fileObj, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			isNewFile = true
			fileObj, err = os.OpenFile(filePath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0644)
			if err != nil {
				return isNewFile, nil, 0, annotatedError{err, "failed to create new file"}
			}
			emptyBytes := make([]byte, 1024)
			for i := 0; i < nkiloBytes; i++ {
				n, err := fileObj.Write(emptyBytes)
				if n != 1024 {
					panic("initialize disk file unsuccessful")
				}
				if err != nil {
					return isNewFile, nil, 0, annotatedError{err, "failed to write empty bytes"}
				}
			}
			err = fileObj.Close()
			if err != nil {
				return isNewFile, nil, 0, annotatedError{err, "failed to close new file"}
			}
			fileObj, err = os.OpenFile(filePath, os.O_RDWR, 0644)
			if err != nil {
				return isNewFile, nil, 0, annotatedError{err, "failed to open newly created file"}
			}
		} else {
			return isNewFile, nil, 0, annotatedError{err, "failed to open existing file"}
		}
	}
	fi, err := fileObj.Stat()
	if err != nil {
		return isNewFile, nil, 0, annotatedError{err, "failed to get file size"}
	}
	return isNewFile, fileObj, int(fi.Size()), nil
}