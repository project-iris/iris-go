// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Contains the wire protocol for communicating with the Iris relay endpoint.

// The specification version implemented is v1.0-draft2, available at:
// http://iris.karalabe.com/specs/relay-protocol-v1.0-draft2.pdf

package iris

import (
	"fmt"
	"io"
)

// Packet opcodes
const (
	opInit  byte = 0x00 // Out: connection initiation           | In: connection acceptance
	opDeny       = 0x01 // Out: <never sent>                    | In: connection refusal
	opClose      = 0x02 // Out: connection tear-down initiation | In: connection tear-down notification

	opBroadcast = 0x03 // Out: application broadcast initiation | In: application broadcast delivery
	opRequest   = 0x04 // Out: application request initiation   | In: application request delivery
	opReply     = 0x05 // Out: application reply initiation     | In: application reply delivery

	opSubscribe   = 0x06 // Out: topic subscription             | In: <never received>
	opUnsubscribe = 0x07 // Out: topic subscription removal     | In: <never received>
	opPublish     = 0x08 // Out: topic event publish            | In: topic event delivery

	opTunBuild    = 0x09 // Out: tunnel construction request    | In: tunnel initiation
	opTunConfirm  = 0x0a // Out: tunnel confirmation            | In: tunnel construction result
	opTunAllow    = 0x0b // Out: tunnel transfer allowance      | In: <same as out>
	opTunTransfer = 0x0c // Out: tunnel data exchange           | In: <same as out>
	opTunClose    = 0x0d // Out: tunnel termination request     | In: tunnel termination notification
)

// Protocol constants
var (
	protoVersion = "v1.0-draft2"
	clientMagic  = "iris-client-magic"
	relayMagic   = "iris-relay-magic"
)

// Serializes a single byte into the relay connection.
func (r *relay) sendByte(data byte) error {
	return r.sockBuf.WriteByte(data)
}

// Serializes a boolean into the relay connection.
func (r *relay) sendBool(data bool) error {
	if data {
		return r.sendByte(1)
	}
	return r.sendByte(0)
}

// Serializes a variable int into the relay using base 128 encoding into the relay connection.
func (r *relay) sendVarint(data uint64) error {
	for data > 127 {
		// Internal byte, set the continuation flag and send
		if err := r.sendByte(byte(128 + data%128)); err != nil {
			return err
		}
		data /= 128
	}
	// Final byte, send and return
	return r.sendByte(byte(data))
}

// Serializes a length-tagged binary array into the relay connection.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if _, err := r.sockBuf.Write([]byte(data)); err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay connection.
func (r *relay) sendString(data string) error {
	return r.sendBinary([]byte(data))
}

// Sends a connection initiation.
func (r *relay) sendInit(cluster string) error {
	if err := r.sendByte(opInit); err != nil {
		return err
	}
	if err := r.sendString(clientMagic); err != nil {
		return err
	}
	if err := r.sendString(protoVersion); err != nil {
		return err
	}
	if err := r.sendString(cluster); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a connection tear-down initiation.
func (r *relay) sendClose() error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends an application broadcast initiation.
func (r *relay) sendBroadcast(cluster string, message []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opBroadcast); err != nil {
		return err
	}
	if err := r.sendString(cluster); err != nil {
		return err
	}
	if err := r.sendBinary(message); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends an application request initiation.
func (r *relay) sendRequest(id uint64, cluster string, request []byte, timeout int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opRequest); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	if err := r.sendString(cluster); err != nil {
		return err
	}
	if err := r.sendBinary(request); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(timeout)); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends an application reply initiation.
func (r *relay) sendReply(id uint64, reply []byte, fault string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opReply); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	if len(fault) == 0 {
		if err := r.sendBinary(reply); err != nil {
			return err
		}
	} else {
		if err := r.sendString(fault); err != nil {
			return err
		}
	}
	return r.sockBuf.Flush()
}

// Sends a topic subscription.
func (r *relay) sendSubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opSubscribe); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a topic subscription removal.
func (r *relay) sendUnsubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opUnsubscribe); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a topic event publish.
func (r *relay) sendPublish(topic string, event []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opPublish); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	if err := r.sendBinary(event); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a tunnel construction request.
func (r *relay) sendTunnelRequest(id uint64, cluster string, timeout int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunBuild); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	if err := r.sendString(cluster); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(timeout)); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a tunnel confirmation.
func (r *relay) sendTunnelConfirm(buildId, tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunConfirm); err != nil {
		return err
	}
	if err := r.sendVarint(buildId); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a tunnel transfer allowance.
func (r *relay) sendTunnelAllowance(id uint64, space uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunAllow); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	if err := r.sendVarint(space); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a tunnel data exchange.
func (r *relay) sendTunnelData(id uint64, size uint64, payload []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunTransfer); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	if err := r.sendVarint(size); err != nil {
		return err
	}
	if err := r.sendBinary(payload); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a tunnel termination request.
func (r *relay) sendTunnelClose(id uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunClose); err != nil {
		return err
	}
	if err := r.sendVarint(id); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Retrieves a single byte from the relay connection.
func (r *relay) recvByte() (byte, error) {
	return r.sockBuf.ReadByte()
}

// Retrieves a boolean from the relay connection.
func (r *relay) recvBool() (bool, error) {
	b, err := r.recvByte()
	if err != nil {
		return false, err
	}
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("protocol violation: invalid boolean value: %v", b)
	}
}

// Retrieves a variable int from the relay in base 128 encoding from the relay connection.
func (r *relay) recvVarint() (uint64, error) {
	var num uint64
	for i := uint(0); ; i++ {
		chunk, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		num += uint64(chunk&127) << (7 * i)
		if chunk <= 127 {
			break
		}
	}
	return num, nil
}

// Retrieves a length-tagged binary array from the relay connection.
func (r *relay) recvBinary() ([]byte, error) {
	// Fetch the length of the binary blob
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	// Fetch the blob itself
	data := make([]byte, size)
	if _, err := io.ReadFull(r.sockBuf, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay connection.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Retrieves a connection initiation response (either accept or deny).
func (r *relay) procInit() (string, error) {
	// Retrieve the response opcode
	op, err := r.recvByte()
	if err != nil {
		return "", err
	}
	// Verify the opcode validity and relay magic string
	switch {
	case op == opInit || op == opDeny:
		if magic, err := r.recvString(); err != nil {
			return "", err
		} else if magic != relayMagic {
			return "", fmt.Errorf("protocol violation: invalid relay magic: %s", magic)
		}
	default:
		return "", fmt.Errorf("protocol violation: invalid init response opcode: %v", op)
	}
	// Depending on success or failure, proceed and return
	switch op {
	case opInit:
		// Read the highest supported protocol version
		if version, err := r.recvString(); err != nil {
			return "", err
		} else {
			return version, nil
		}
	case opDeny:
		// Read the reason for connection denial
		if reason, err := r.recvString(); err != nil {
			return "", err
		} else {
			return "", fmt.Errorf("connection denied: %s", reason)
		}
	default:
		panic("unreachable code")
	}
}

// Retrieves a connection tear-down notification.
func (r *relay) procClose() (string, error) {
	return r.recvString()
}

// Retrieves a broadcast delivery.
func (r *relay) procBroadcast() error {
	message, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleBroadcast(message)
	return nil
}

// Retrieves a request delivery.
func (r *relay) procRequest() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	request, err := r.recvBinary()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleRequest(id, request, timeout)
	return nil
}

// Retrieves a reply delivery.
func (r *relay) procReply() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := r.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		go r.handleReply(id, nil, "")
		return nil
	}
	// The request didn't time out, get the result
	success, err := r.recvBool()
	if err != nil {
		return err
	}
	if success {
		reply, err := r.recvBinary()
		if err != nil {
			return err
		}
		go r.handleReply(id, reply, "")
	} else {
		fault, err := r.recvString()
		if err != nil {
			return err
		}
		go r.handleReply(id, nil, fault)
	}
	return nil
}

// Retrieves a topic event delivery.
func (r *relay) procPublish() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	event, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handlePublish(topic, event)
	return nil
}

// Retrieves a tunnel initiation message.
func (r *relay) procTunnelRequest() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	chunk, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelRequest(id, chunk)
	return nil
}

// Retrieves a tunnel construction result.
func (r *relay) procTunnelReply() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := r.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		go r.handleTunnelReply(id, 0)
		return nil
	}
	// The tunnel didn't time out, proceed
	chunk, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelReply(id, chunk)
	return nil
}

// Retrieves a tunnel transfer allowance message.
func (r *relay) procTunnelAllowance() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	space, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelAllowance(id, space)
	return nil
}

// Retrieves a tunnel data exchange message.
func (r *relay) procTunnelData() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	size, err := r.recvVarint()
	if err != nil {
		return err
	}
	payload, err := r.recvBinary()
	if err != nil {
		return err
	}
	r.handleTunnelData(id, size, payload) // Note, NOT separate go-routine!
	return nil
}

// Retrieves a tunnel closure notification.
func (r *relay) procTunnelClose() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	reason, err := r.recvString()
	if err != nil {
		return err
	}
	go r.handleTunnelClose(id, reason)
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
			case opBcast:
				err = r.procBroadcast()
			case opReq:
				err = r.procRequest()
			case opRep:
				err = r.procReply()
			case opPub:
				err = r.procPublish()
			case opTunReq:
				err = r.procTunnelRequest()
			case opTunRep:
				err = r.procTunnelReply()
			case opTunAck:
				err = r.procTunnelAllowance()
			case opTunData:
				err = r.procTunnelData()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				// Retrieve any reason for remote closure
				if reason, cerr := r.procClose(); cerr != nil {
					err = cerr
				} else if len(reason) > 0 {
					err = fmt.Errorf("connection dropped: %s", reason)
				} else {
					closed = true
				}
			default:
				err = fmt.Errorf("protocol violation: unknown opcode: %v", op)
			}
		}
	}
	// Close the socket and signal termination to all blocked threads
	r.sock.Close()
	close(r.term)

	// Notify the application if the connection dropped
	r.handleDrop(err)

	// Wait for termination sync
	errc := <-r.quit
	errc <- err
}
