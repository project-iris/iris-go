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
	"fmt"
)

const (
	opInit byte = iota
	opBcast
	opReq
	opRep
	opSub
	opPub
	opUnsub
	opClose
	opTunReq
	opTunRep
	opTunAck
	opTunData
	opTunPoll
	opTunClose
)

// Relay protocol version
var relayVersion = "v1.0a"

// Serializes a single byte into the relay.
func (r *relay) sendByte(data byte) error {
	if err := r.sockBuf.WriteByte(data); err != nil {
		return permError(err)
	}
	return nil
}

// Serializes a boolean into the relay.
func (r *relay) sendBool(data bool) error {
	if data {
		return r.sendByte(1)
	} else {
		return r.sendByte(0)
	}
}

// Serializes a variable int into the relay.
func (r *relay) sendVarint(data uint64) error {
	for {
		if data > 127 {
			// Internalt byte, set the continuation flag and send
			if err := r.sendByte(byte(128 + data%128)); err != nil {
				return err
			}
			data /= 128
		} else {
			// Final byte, send and return
			return r.sendByte(byte(data))
		}
	}
}

// Serializes a length-tagged binary array into the relay.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if n, err := r.sockBuf.Write([]byte(data)); n != len(data) || err != nil {
		return permError(err)
	}
	return nil
}

// Serializes a length-tagged string into the relay.
func (r *relay) sendString(data string) error {
	return r.sendBinary([]byte(data))
}

// Flushes the output buffer into the network stream.
func (r *relay) sendFlush() error {
	if err := r.sockBuf.Flush(); err != nil {
		return permError(err)
	}
	return nil
}

// Assembles and serializes an init packet into the relay.
func (r *relay) sendInit(app string) error {
	if err := r.sendByte(opInit); err != nil {
		return err
	}
	if err := r.sendString(relayVersion); err != nil {
		return err
	}
	if err := r.sendString(app); err != nil {
		return err
	}
	return r.sendFlush()
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
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sendFlush()
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
	if err := r.sendVarint(uint64(time)); err != nil {
		return err
	}
	return r.sendFlush()
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
	if err := r.sendBinary(rep); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a topic subscription message into the relay.
func (r *relay) sendSubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opSub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return r.sendFlush()
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
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a topic unsubscription message into the relay.
func (r *relay) sendUnsubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opUnsub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a close message into the relay.
func (r *relay) sendClose() error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunneling request into the relay.
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
	if err := r.sendVarint(uint64(time)); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends the acknowledgement for a tunneling request into the relay.
func (r *relay) sendTunnelReply(tmpId, tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunRep); err != nil {
		return err
	}
	if err := r.sendVarint(tmpId); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunnel data message into the relay.
func (r *relay) sendTunnelData(tunId uint64, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunData); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunnel data poll into the relay.
func (r *relay) sendTunnelPoll(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunPoll); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunnel termination message into the relay.
func (r *relay) sendTunnelClose(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunClose); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendFlush()
}

// Retrieves a single byte from the relay.
func (r *relay) recvByte() (byte, error) {
	b, err := r.sockBuf.ReadByte()
	if err != nil {
		return 0, permError(err)
	}
	return b, nil
}

// Retrieves a boolean from the relay.
func (r *relay) recvBool() (bool, error) {
	b, err := r.recvByte()
	if err != nil {
		return false, err
	}
	return b == 1, nil
}

// Retrieves a variable int from the relay.
func (r *relay) recvVarint() (uint64, error) {
	var num uint64
	for {
		// Retreive the next byte of the varint
		b, err := r.recvByte()
		if err != nil {
			return 0, permError(err)
		}
		// Save it and terminate if last byte
		if b > 127 {
			num += uint64(b - 128)
		} else {
			num += uint64(b)
			break
		}
	}
	return num, nil
}

// Retrieves a length-tagged binary array from the relay.
func (r *relay) recvBinary() ([]byte, error) {
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if n, err := r.sockBuf.Read(data); n != int(size) || err != nil {
		return nil, permError(err)
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Retrieves a connection initialization response and returns whether ok.
func (r *relay) procInit() error {
	if op, err := r.recvByte(); err != nil {
		return err
	} else if op != opInit {
		return permError(fmt.Errorf("protocol violation"))
	}
	return nil
}

// Retrieves a remote request from the relay and processes it.
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

// Retrieves a remote broadcast message from the relay and notifies the handler.
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

// Retrieves a remote tunneling request from the relay and processes it.
func (r *relay) procTunnelRequest() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	win, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Handle the message
	go r.handleTunnelRequest(tunId, int(win))
	return nil
}

// Retrieves a remote tunneling request from the relay and processes it.
func (r *relay) procTunnelReply() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	win, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Handle the message
	go r.handleTunnelReply(tunId, int(win))
	return nil
}

// Retrieves a remote tunnel message ack and processes it.
func (r *relay) procTunnelAck() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	ack, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Handle the message
	go r.handleTunnelAck(tunId, ack)
	return nil
}

// Retrieves a remote tunnel data message and processes it.
func (r *relay) procTunnelData() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Handle the message
	go r.handleTunnelData(tunId, msg)
	return nil
}

// Retrieves a remote tunneling request from the relay and processes it.
func (r *relay) procTunnelClose() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Handle the message
	go r.handleTunnelClose(tunId)
	return nil
}

// Retrieves messages from the client connection and keeps processing them until
// either the relay closes (graceful close) or the connection drops.
func (r *relay) process() {
	var op byte
	var err error
	for closed := false; !closed && err == nil; {
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
			case opTunReq:
				err = r.procTunnelRequest()
			case opTunRep:
				err = r.procTunnelReply()
			case opTunAck:
				err = r.procTunnelAck()
			case opTunData:
				err = r.procTunnelData()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				closed = true
			default:
				err = fmt.Errorf("unknown opcode: %v", op)
			}
		}
	}
	// Nofity the application if the connection failed
	if err != nil {
		go r.handleDrop(err)
	}
	// Close the socket and return error (if any) when requested
	err = r.sock.Close()
	errc := <-r.quit
	errc <- err
}