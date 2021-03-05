package generators

import (
	"crypto/md5"
	"encoding/hex"
	"os"
	"sync/atomic"
	"time"

	"github.com/MichaelTJones/pcg"
)

// Generator for creating bson.ObjectId
type objectIDGenerator struct {
	base
}

func newObjectIDGenerator(base base) (Generator, error) {
	return &objectIDGenerator{base: base}, nil
}

// Value add a bson.ObjectId to the DocBuffer.
func (g *objectIDGenerator) EncodeValue() {
	g.buffer.Write(g.randomObjectID())
}

func (g *objectIDGenerator) EncodeValueAsString() {

	dst := make([]byte, hex.EncodedLen(12))
	hex.Encode(dst, g.randomObjectID())
	g.buffer.Write(dst)
}

var (
	// machine ID to generate unique object ID
	machineID = readMachineID()
	// process ID to generate unique object ID
	processID = os.Getpid()
	// objectIdCounter is atomically incremented when generating a new ObjectId
	objectIDCounter = getRandomUint32()
)

func (g *objectIDGenerator) randomObjectID() []byte {

	t := uint32(time.Now().Unix())
	i := atomic.AddUint32(&objectIDCounter, 1)

	return []byte{
		byte(t >> 24),
		byte(t >> 16),
		byte(t >> 8),
		byte(t),
		machineID[0], // Machine, first 3 bytes of md5(hostname)
		machineID[1],
		machineID[2],
		byte(processID >> 8), // Pid, 2 bytes, specs don't specify endianness, but we use big endian.
		byte(processID),
		byte(i >> 16), // Increment, 3 bytes, big endian
		byte(i >> 8),
		byte(i),
	}
}

// readMachineId generates and returns a machine id.
func readMachineID() []byte {
	id := uint32Bytes(getRandomUint32())
	hostname, err := os.Hostname()
	if err == nil {
		h := md5.New()
		h.Write([]byte(hostname))
		id = h.Sum(nil)
	}
	return id[0:3]
}

func getRandomUint32() uint32 {
	seed := uint64(time.Now().Unix())
	pcg32 := pcg.NewPCG32().Seed(seed, seed)
	return pcg32.Random()
}
