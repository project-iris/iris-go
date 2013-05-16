// Iris Go Binding
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// The current language binding is an official support library of the Iris
// decentralized messaging framework, and as such, the same licensing terms
// hold. For details please see http://github.com/karalabe/iris/LICENSE.md
//
// Author: peterke@gmail.com (Peter Szilagyi)

// Contains the wire protocol for communicating with the Iris node.

package iris

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"
)

const (
	opReq byte = iota
	opRep
	opBcast
	opTun
	opSub
	opPub
	opUnsub
	opClose
)

//
type relay struct {
	sock net.Conn    // Network relay to the systen entry node
	conn *connection // Application iris connection

	outVarBuf []byte // Buffer for variable int encoding
	inByteBuf []byte // Buffer for byte decoding
	inVarBuf  []byte // Buffer for variable int decoding

	quit chan struct{}
	lock sync.Mutex
}

// Connects to a local relay endpoint on port and logs in with id app.
func connect(port int, app string, conn *connection) (*relay, error) {
	// Connect to the iris relay node
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	// Create the relay object and initialize
	rel := &relay{
		sock:      sock,
		conn:      conn,
		outVarBuf: make([]byte, binary.MaxVarintLen64),
		inByteBuf: make([]byte, 1),
		inVarBuf:  make([]byte, binary.MaxVarintLen64),
	}
	if err := rel.sendInit(app); err != nil {
		return nil, err
	}
	// All ok, start processing messages and return
	go rel.process()
	return rel, nil
}

// Serializes a single byte into the relay.
func (r *relay) sendByte(data byte) error {
	if n, err := r.sock.Write([]byte{data}); n != 1 || err != nil {
		return err
	}
	return nil
}

// Serializes a variable int into the relay.
func (r *relay) sendVarint(data uint64) error {
	size := binary.PutUvarint(r.outVarBuf, data)
	if n, err := r.sock.Write(r.outVarBuf[:size]); n != size || err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged binary array into the relay.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if n, err := r.sock.Write([]byte(data)); n != len(data) || err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay.
func (r *relay) sendString(data string) error {
	if err := r.sendBinary([]byte(data)); err != nil {
		return err
	}
	return nil
}

// Initializes the connection by sending the requested app identifier.
func (r *relay) sendInit(app string) error {
	return r.sendString(app)
}

// Atomically sends a request message into the relay.
func (r *relay) sendRequest(reqId uint64, app string, req []byte, time int) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opReq); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	if err := r.sendBinary(req); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(time)); err != nil {
		return err
	}
	return nil
}

// Atomically sends a reply message into the relay.
func (r *relay) sendReply(reqId uint64, rep []byte) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opRep); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendBinary(rep); err != nil {
		return err
	}
	return nil
}

// Atomically sends an application broadcast message into the relay.
func (r *relay) sendBroadcast(app string, msg []byte) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opBcast); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return nil
}

// Atomically sends a tunneling message into the relay.
func (r *relay) sendTunnel(tunId uint64, app string, time uint64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opTun); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	if err := r.sendVarint(time); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic subscription message into the relay.
func (r *relay) sendSubscribe(topic string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opSub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendPublish(topic string, msg []byte) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opPub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic unsubscription message into the relay.
func (r *relay) sendUnsubscribe(topic string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opUnsub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return nil
}

// Atomically sends a close message into the relay.
func (r *relay) sendClose() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	return nil
}

// Retrieves a single byte from the relay.
func (r *relay) recvByte() (byte, error) {
	if n, err := r.sock.Read(r.inByteBuf); n != 1 || err != nil {
		return 0, err
	}
	return r.inByteBuf[0], nil
}

// Retrieves a variable int from the relay.
func (r *relay) recvVarint() (uint64, error) {
	// Retrieve the varint bytes one at a time
	index := 0
	for {
		// Retreive the next byte of the varint
		b, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		// Save it and terminate loop if last byte
		r.inVarBuf[index] = b
		index++
		if b&0x80 == 0 {
			break
		}
	}
	if num, n := binary.Uvarint(r.inVarBuf[:index]); n <= 0 {
		return 0, fmt.Errorf("failed to decode varint %v", r.inVarBuf[:index])
	} else {
		return num, nil
	}
}

// Retrieves a length-tagged binary array from the relay.
func (r *relay) recvBinary() ([]byte, error) {
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if n, err := r.sock.Read(data); n != int(size) || err != nil {
		return nil, err
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), err
	}
}

// Retrieves a remote request from the relay and processes it on a new thread.
func (r *relay) procRequest() error {
	// Retrieve the message parts
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	req, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Handle the message
	go r.conn.handleRequest(reqId, req)
	return nil
}

// Retrieves a remote reply from the relay and processes it.
func (r *relay) procReply() error {
	// Retrieve the message parts
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	rep, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the reply to the pending handler routine
	go r.conn.handleReply(reqId, rep)
	return nil
}

// Retrieves a remote broadcast message from the relay and processes it.
func (r *relay) procBroadcast() error {
	// Retrieve the message parts
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.conn.handleBroadcast(msg)
	return nil
}

// Retrieves a topic publish message from the relay and processes it.
func (r *relay) procPublish() error {
	// Retrieve the message parts
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.conn.handlePublish(topic, msg)
	return nil
}

// Retrieves messages from the client connection and keeps processing them until
// either side closes the socket.
func (r *relay) process() {
	defer r.sock.Close()

	var op byte
	var err error
	for err == nil {
		// Retrieve the next opcode and call the specific handler for the rest
		if op, err = r.recvByte(); err == nil {
			switch op {
			case opReq:
				err = r.procRequest()
			case opRep:
				err = r.procReply()
			case opBcast:
				err = r.procBroadcast()
			case opPub:
				err = r.procPublish()
			case opClose:
				return
			default:
				err = fmt.Errorf("unknown opcode: %v", op)
			}
		}
	}
	// Log the error and terminate
	log.Printf("relay: protocol error: %v", err)
}
