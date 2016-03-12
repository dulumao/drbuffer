package drbuffer

import (
	"fmt"
	"math"
	"unsafe"
)

const META_SECTION_SIZE = 16 // 4 for version 4 for nextWriteFrom 4 for lastReadTo 4 for wrapAt
const MAX_PACKETS_READ_ONE_TIME = 1024
const IS_DEBUG = false

type ringBuffer struct {
	meta          []byte // store the head/tail/wrap pointers
	data          []byte // store packets
	version       *uint32
	nextWriteFrom *uint32
	lastReadTo    *uint32
	wrapAt        *uint32
	nextReadFrom  *uint32
	nextReadAtSameLap  bool
	shouldResetWrapAt  bool
	reusablePacketList [][]byte
}

func NewRingBuffer(meta []byte, buffer []byte, nextReadAt uint32) *ringBuffer {
	if len(meta) != META_SECTION_SIZE {
		panic(fmt.Sprintf("meta should of size: %s", META_SECTION_SIZE))
	}
	return &ringBuffer{
		meta:       meta,
		data:       buffer,
		version: (*uint32)(unsafe.Pointer(&meta[0])),
		nextWriteFrom: (*uint32)(unsafe.Pointer(&meta[4])),
		lastReadTo: (*uint32)(unsafe.Pointer(&meta[8])),
		wrapAt: (*uint32)(unsafe.Pointer(&meta[12])),
		nextReadFrom: new(uint32),
		reusablePacketList: make([][]byte, MAX_PACKETS_READ_ONE_TIME),
		nextReadAtSameLap: true,
		shouldResetWrapAt: false,
	}
}

type packet []byte

func (p packet) size() uint16 {
	packetSizePtr := (*uint16)(unsafe.Pointer(&p[0]))
	packetSize := *packetSizePtr
	return packetSize
}

func (p packet) write(bytes []byte) {
	if len(bytes) > math.MaxUint16 {
		panic(fmt.Sprintf("packet too large: %s", len(bytes)))
	}
	packetSize := uint16(len(bytes))
	packetSizePtr := (*uint16)(unsafe.Pointer(&p[0]))
	*packetSizePtr = packetSize
	copy(p[2:], bytes)
}

func (p packet) read() packet {
	return p[2 : 2 + p.size()]
}

func (buffer *ringBuffer) PushN(pList [][]byte) {
	writeFrom := *buffer.nextWriteFrom
	for _, p := range pList {
		buffer.PushOne(p)
	}
	writeTo := *buffer.nextWriteFrom
	if IS_DEBUG {
		fmt.Println("write", len(pList), "[", writeFrom, ",", writeTo, ")")
	}
}

func (buffer *ringBuffer) PushOne(p []byte) {
	if len(p) > len(buffer.data) - 2 {
		panic(fmt.Sprintf("packet to push is too large: %s", len(p)))
	}
	writeFrom := *buffer.nextWriteFrom
	writeTo := writeFrom + 2 + uint32(len(p))
	if !buffer.nextReadAtSameLap {
		// first lap is immune
		// read pointer in range [writeFrom, writeTo) will be repelled to safe harbour (0)
		buffer.repelReadPointers(writeFrom, writeTo)
	}
	if writeTo > uint32(len(buffer.data)) {
		*buffer.wrapAt = writeFrom
		buffer.nextReadAtSameLap = false
		if IS_DEBUG {
			fmt.Println("wrap at:", writeFrom)
		}
		writeFrom = 0
		writeTo = 2 + uint32(len(p))
		// [writeFrom, writeTo) changed, repel again
		buffer.repelReadPointers(writeFrom, writeTo)
	}
	// write data first before moving nw pointer to ensure the pointing region is valid
	packet(buffer.data[writeFrom:]).write(p)
	*buffer.nextWriteFrom = writeTo
}

func (buffer *ringBuffer) repelReadPointers(writeFrom, writeTo uint32) {
	if writeFrom <= *buffer.lastReadTo && *buffer.lastReadTo < writeTo {
		// move lastReadTo to avoid overwrite, 0 always point to a valid packet
		*buffer.lastReadTo = 0
		*buffer.wrapAt = 0
		*buffer.nextReadFrom = 0
		buffer.nextReadAtSameLap = true
	}
	// do not need to check buffer.nextReadFrom as buffer.lastReadTo will always be encountered first
}

func (buffer *ringBuffer) PopN(maxPacketsCount int) [][]byte {
	if maxPacketsCount > MAX_PACKETS_READ_ONE_TIME {
		maxPacketsCount = MAX_PACKETS_READ_ONE_TIME
	}
	if buffer.shouldResetWrapAt {
		buffer.shouldResetWrapAt = false
		*buffer.wrapAt = 0
	}
	*buffer.lastReadTo = *buffer.nextReadFrom
	r1From, r1To := uint32(0), uint32(0) // [r1From, r1To) the first region to read
	r2From, r2To := uint32(0), uint32(0) // [r2From, r2To) the second region to read, might not needed
	if buffer.nextReadAtSameLap {
		// first lap is simple and special
		r1From = *buffer.nextReadFrom
		r1To = *buffer.nextWriteFrom
	} else {
		if *buffer.nextReadFrom >= *buffer.nextWriteFrom {
			// write is in the next lap now, we finish the first lap at wrapAt
			r1From = *buffer.nextReadFrom
			r1To = *buffer.wrapAt
			// catch up the second lap
			r2From = 0
			r2To = *buffer.nextWriteFrom
		} else {
			// we are at the same lap
			r1From = *buffer.nextReadFrom
			r1To = *buffer.nextWriteFrom
		}
	}
	packetsCount := 0
	if r2From == r2To {
		packetsCount = buffer.readRegion(r1From, r1To, 0, maxPacketsCount)
		*buffer.nextReadFrom = r1To
	} else {
		packetsCount = buffer.readRegion(r1From, r1To, 0, maxPacketsCount)
		buffer.nextReadAtSameLap = true
		buffer.shouldResetWrapAt = true
		packetsCount = buffer.readRegion(r2From, r2To, packetsCount, maxPacketsCount)
		*buffer.nextReadFrom = r2To
	}
	return buffer.reusablePacketList[:packetsCount]
}

func (buffer *ringBuffer) readRegion(readFrom, readTo uint32, packetsCount int, maxPacketsCount int) int {
	if IS_DEBUG {
		fmt.Println("read [", readFrom, ",", readTo, ")")
	}
	for pos := readFrom; pos < readTo && packetsCount < maxPacketsCount; {
		if IS_DEBUG {
			fmt.Println("read packet of size: ", packet(buffer.data[pos:]).size())
		}
		p := packet(buffer.data[pos:]).read()
		buffer.reusablePacketList[packetsCount] = p
		pos = pos + 2 + uint32(len(p))
		packetsCount += 1
	}
	return packetsCount
}

func (buffer *ringBuffer) PopOne() []byte {
	packets := buffer.PopN(1)
	if len(packets) > 0 {
		return packets[0]
	} else {
		return nil
	}
}
