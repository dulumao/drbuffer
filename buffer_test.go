package drbuffer

import (
	"testing"
	"math/rand"
)

func Test_push_to_empty(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	assert(*buffer.nextWriteFrom, "==", uint32(0))
	assert(*buffer.nextReadFrom, "==", uint32(0))
	assert(*buffer.lastReadTo, "==", uint32(0))
	assert(*buffer.wrapAt, "==", uint32(0))
	buffer.PushOne([]byte("A"))
	assert(*buffer.nextWriteFrom, "==", uint32(3)) // 3 bytes used to store packet size and "A"
	assert(*buffer.nextReadFrom, "==", uint32(0)) // because not popped yet
	assert(*buffer.lastReadTo, "==", uint32(0)) // because not popped yet
	assert(*buffer.wrapAt, "==", uint32(0))  // break not moved, as not wrapped around yet
}

func Test_pop_from_empty(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	packet := buffer.PopOne()
	assert(packet, "==", nil)
	assert(*buffer.nextWriteFrom, "==", uint32(0)) // nothing moved yet
	assert(*buffer.nextReadFrom, "==", uint32(0)) // nothing moved yet
	assert(*buffer.lastReadTo, "==", uint32(0)) // nothing moved yet
	assert(*buffer.wrapAt, "==", uint32(0))  // nothing moved yet
}

func Test_push_pop(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	packet := buffer.PopOne()
	assert(string(packet), "==", "A")
	assert(*buffer.nextWriteFrom, "==", uint32(3)) // stored "A"
	assert(*buffer.nextReadFrom, "==", uint32(3)) // "A" already read
	assert(*buffer.lastReadTo, "==", uint32(0)) // last read not committed yet
	assert(*buffer.wrapAt, "==", uint32(0))  // not wrapped around, do not need to update this
}

func Test_push_pop_pop(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	packet := buffer.PopOne()
	assert(string(packet), "==", "A")
	packet = buffer.PopOne()
	assert(packet, "==", nil)
	assert(*buffer.nextWriteFrom, "==", uint32(3)) // stored "A"
	assert(*buffer.nextReadFrom, "==", uint32(3)) // "A" already read
	assert(*buffer.lastReadTo, "==", uint32(3)) // last read is committed now
	assert(*buffer.wrapAt, "==", uint32(0))  // not wrapped around, do not need to update this
}

func Test_pushN_popN(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushN([][]byte{
		[]byte("A"),
		[]byte("B"),
	})
	packets := buffer.PopN(1024)
	assert(len(packets), "==", 2)
	assert(string(packets[0]), "==", "A")
	assert(string(packets[1]), "==", "B")
	assert(*buffer.nextWriteFrom, "==", uint32(6)) // stored "A", "B"
	assert(*buffer.nextReadFrom, "==", uint32(6)) // "A", "B" already read
	assert(*buffer.lastReadTo, "==", uint32(0)) // last read not committed yet
	assert(*buffer.wrapAt, "==", uint32(0))  // not wrapped around, do not need to update this
}

func Test_push_wrapped(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushN([][]byte{
		[]byte("A"),
		[]byte("B"),
		[]byte("C"),
	})
	assert(*buffer.nextWriteFrom, "==", uint32(9)) // stored "A", "B", "C"
	buffer.PopN(1024) // move nextReadFrom
	buffer.PopN(1024) // move lastReadTo
	buffer.PushOne([]byte("DD"))
	assert(*buffer.nextWriteFrom, "==", uint32(4)) // overwrite "A", "B", stored "C", "DD"
	assert(*buffer.wrapAt, "==", uint32(9))  // wrap at 9 not 10, leave a marker for read to catch up
	assert(buffer.data, "==", []byte{
		2, 0, byte('D'), byte('D'), // 4th packet
		0, byte('B'), // 2nd packet, partially overwrite
		1, 0, byte('C'), // 3th packet
		0, // excluded by wrap at pointer
	})
}

func Test_push_should_not_override_lastReadTo_after_wrap(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	buffer.PopOne()
	buffer.PushOne([]byte("B"))
	buffer.PopOne()
	assert(*buffer.nextWriteFrom, "==", uint32(6))
	assert(*buffer.nextReadFrom, "==", uint32(6))
	assert(*buffer.lastReadTo, "==", uint32(3))
	assert(*buffer.wrapAt, "==", uint32(0))
	assert(buffer.data, "==", []byte{
		1, 0, byte('A'), // 1st packet
		1, 0, byte('B'), // 2nd packet <-- lastReadTo
		0, 0, 0, 0, // not used yet
	})
	buffer.PushOne([]byte("C"))
	buffer.PushOne([]byte("DD"))
	assert(*buffer.nextWriteFrom, "==", uint32(4))
	assert(*buffer.nextReadFrom, "==", uint32(6))
	assert(*buffer.lastReadTo, "==", uint32(0)) // can not point to 3 as it is invalid region now
	assert(*buffer.wrapAt, "==", uint32(9))
	assert(buffer.data, "==", []byte{
		2, 0, byte('D'), byte('D'), // 4th packet <-- lastReadTo
		0, byte('B'), // 2nd packet, partially overwrite
		1, 0, byte('C'), // 3th packet
		0, // excluded by wrap at pointer
	})
}

func Test_push_should_not_override_lastReadTo_before_wrap(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	buffer.PopOne()
	buffer.PushOne([]byte("B"))
	buffer.PopOne()
	assert(*buffer.nextWriteFrom, "==", uint32(6))
	assert(*buffer.nextReadFrom, "==", uint32(6))
	assert(*buffer.lastReadTo, "==", uint32(3))
	assert(*buffer.wrapAt, "==", uint32(0))
	assert(buffer.data, "==", []byte{
		1, 0, byte('A'), // 1st packet
		1, 0, byte('B'), // 2nd packet
		0, 0, 0, 0, // not used yet
	})
	buffer.PushOne([]byte("C"))
	buffer.PushOne([]byte("D"))
	assert(*buffer.nextWriteFrom, "==", uint32(3))
	assert(*buffer.nextReadFrom, "==", uint32(6))
	assert(*buffer.lastReadTo, "==", uint32(3)) // still not overwrite yet
	assert(*buffer.wrapAt, "==", uint32(9))
	assert(buffer.data, "==", []byte{
		1, 0, byte('D'), // 4th packet
		1, 0, byte('B'), // 2nd packet <-- lastReadTo
		1, 0, byte('C'), // 3th packet
		0, // excluded by wrap at pointer
	})
	buffer.PushOne([]byte("E"))
	assert(*buffer.nextWriteFrom, "==", uint32(6))
	assert(*buffer.nextReadFrom, "==", uint32(6))
	assert(*buffer.lastReadTo, "==", uint32(0)) // index 3 was overwritten
	assert(*buffer.wrapAt, "==", uint32(9))
	assert(buffer.data, "==", []byte{
		1, 0, byte('D'), // 4th packet <-- lastReadTo
		1, 0, byte('E'), // 5th packet
		1, 0, byte('C'), // 3th packet
		0, // excluded by wrap at pointer
	})
}

func Test_push_should_not_override_nextReadFrom(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	buffer.PopOne()
	assert(*buffer.nextWriteFrom, "==", uint32(3))
	assert(*buffer.nextReadFrom, "==", uint32(3))
	assert(*buffer.lastReadTo, "==", uint32(0))
	assert(*buffer.wrapAt, "==", uint32(0))
	assert(buffer.data, "==", []byte{
		1, 0, byte('A'), // 1st packet <-- nextReadFrom
		0, 0, 0, 0, 0, 0, 0, // not used yet
	})
	buffer.PushOne([]byte("B"))
	buffer.PushOne([]byte("C"))
	buffer.PushOne([]byte("DD"))
	assert(*buffer.nextWriteFrom, "==", uint32(4))
	assert(*buffer.nextReadFrom, "==", uint32(0)) // can not point to 3 as it is invalid region now
	assert(*buffer.lastReadTo, "==", uint32(0))
	assert(*buffer.wrapAt, "==", uint32(9))
	assert(buffer.data, "==", []byte{
		2, 0, byte('D'), byte('D'), // 4th packet <-- nextReadFrom
		0, byte('B'), // 2nd packet, partially overwrite
		1, 0, byte('C'), // 3th packet
		0, // excluded by wrap at pointer
	})
}

func Test_pop_should_follow_wrapAt(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(10)
	buffer.PushOne([]byte("A"))
	buffer.PopOne()
	buffer.PushOne([]byte("B"))
	buffer.PushOne([]byte("C"))
	buffer.PushOne([]byte("D"))
	assert(*buffer.nextWriteFrom, "==", uint32(3))
	assert(*buffer.nextReadFrom, "==", uint32(3))
	assert(*buffer.lastReadTo, "==", uint32(0))
	assert(*buffer.wrapAt, "==", uint32(9))
	packets := buffer.PopN(1024)
	assert(len(packets), "==", 3)
}

func Test_random_push_pop(t *testing.T) {
	assert := NewAssert(t)
	buffer := newBuffer(1000)
	for i := 0; i < 1024; i++ {
		packets := make([][]byte, 1+rand.Int31n(10))
		for j := 0; j < len(packets); j++ {
			packets[j] = make([]byte, rand.Int31n(998))
		}
		buffer.PushN(packets)
		packets = buffer.PopN(1024)
		assert(len(packets), ">", 0)
	}
}

func newBuffer(size int) *ringBuffer {
	return NewRingBuffer(make([]byte, META_SECTION_SIZE), make([]byte, size), 0)
}
