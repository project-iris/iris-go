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
)

const (
	opReq byte = iota
	opRep
	opBcast
	opTunReq
	opTunRep
	opTunClose
	opSub
	opPub
	opUnsub
	opClose
)

// Serializes a single byte into the relay.
func (r *relay) sendByte(data byte) error {
	if n, err := r.sock.Write([]byte{data}); n != 1 || err != nil {
		return &relayError{
			message:   err.Error(),
			temporary: false,
			timeout:   false,
		}
	}
	return nil
}

// Serializes a variable int into the relay.
func (r *relay) sendVarint(data uint64) error {
	size := binary.PutUvarint(r.outVarBuf, data)
	if n, err := r.sock.Write(r.outVarBuf[:size]); n != size || err != nil {
		return &relayError{
			message:   err.Error(),
			temporary: false,
			timeout:   false,
		}
	}
	return nil
}

// Serializes a length-tagged binary array into the relay.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if n, err := r.sock.Write([]byte(data)); n != len(data) || err != nil {
		return &relayError{
			message:   err.Error(),
			temporary: false,
			timeout:   false,
		}
	}
	return nil
}

// Serializes a length-tagged string into the relay.
func (r *relay) sendString(data string) error {
	return r.sendBinary([]byte(data))
}

// Initializes the connection by sending the requested app identifier.
func (r *relay) sendInit(app string) error {
	if err := r.sendString(Version()); err != nil {
		return err
	}
	return r.sendString(app)
}

// Atomically sends a request message into the relay.
func (r *relay) sendRequest(reqId uint64, app string, req []byte, time int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

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
	return r.sendVarint(uint64(time))
}

// Atomically sends a reply message into the relay.
func (r *relay) sendReply(reqId uint64, rep []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opRep); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	return r.sendBinary(rep)
}

// Atomically sends an application broadcast message into the relay.
func (r *relay) sendBroadcast(app string, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opBcast); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	return r.sendBinary(msg)
}

// Atomically sends a tunneling message into the relay.
func (r *relay) sendTunnelRequest(tunId uint64, app string, time int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunReq); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	return r.sendVarint(uint64(time))
}

// Atomically sends a topic subscription message into the relay.
func (r *relay) sendSubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opSub); err != nil {
		return err
	}
	return r.sendString(topic)
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendPublish(topic string, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opPub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return r.sendBinary(msg)
}

// Atomically sends a topic unsubscription message into the relay.
func (r *relay) sendUnsubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opUnsub); err != nil {
		return err
	}
	return r.sendString(topic)
}

// Atomically sends a close message into the relay.
func (r *relay) sendClose() error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()
	return r.sendByte(opClose)
}

// Retrieves a single byte from the relay.
func (r *relay) recvByte() (byte, error) {
	if n, err := r.sock.Read(r.inByteBuf); n != 1 || err != nil {
		return 0, &relayError{
			message:   err.Error(),
			temporary: false,
			timeout:   false,
		}
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
		return 0, &relayError{
			message:   fmt.Sprintf("iris: protocol violation: invalid varint %v", r.inVarBuf[:index]),
			temporary: false,
			timeout:   false,
		}
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
		return "", &relayError{
			message:   err.Error(),
			temporary: false,
			timeout:   false,
		}
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
	go r.handleRequest(reqId, req)
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
	go r.handleReply(reqId, rep)
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
	go r.handleBroadcast(msg)
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
	go r.handlePublish(topic, msg)
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
	// Check whether failure is due to termination request
	select {
	case <-r.quit:
		// Quit in progress, all ok
	default:
		log.Printf("iris: protocol error: %v", err)
		go r.handleDrop()
	}
}
