// Copyright (c) 2013 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Contains the wire protocol for communicating with the Iris node.

package iris

import "fmt"

const (
	opInit     byte = iota // Connection initialization
	opDeny                 // Connection denial
	opBcast                // Application broadcast
	opReq                  // Application request
	opRep                  // Application reply
	opSub                  // Topic subscription
	opPub                  // Topic publish
	opUnsub                // Topic subscription removal
	opClose                // Connection closing
	opTunReq               // Tunnel building request
	opTunRep               // Tunnel building reply
	opTunData              // Tunnel data transfer
	opTunAck               // Tunnel data acknowledgment
	opTunClose             // Tunnel closing
)

// Protocol constants
var (
	protoVersion = "v1.0-draft2"
	clientMagic  = "iris-client-magic"
	relayMagic   = "iris-relay-magic"
)

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

// Serializes a variable int into the relay using base 128 encoding.
func (r *relay) sendVarint(data uint64) error {
	for {
		if data > 127 {
			// Internal byte, set the continuation flag and send
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

// Atomically sends a topic un-subscription message into the relay.
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
func (r *relay) sendTunnelRequest(tunId uint64, app string, buf int, time int) error {
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
	if err := r.sendVarint(uint64(buf)); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(time)); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends the acknowledgment for a tunneling request into the relay.
func (r *relay) sendTunnelReply(tmpId, tunId uint64, buf int) error {
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
	if err := r.sendVarint(uint64(buf)); err != nil {
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

// Atomically sends a tunnel data acknowledgment into the relay.
func (r *relay) sendTunnelAck(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunAck); err != nil {
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
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		panic("protocol violation")
	}
}

// Retrieves a variable int from the relay in base 128 encoding.
func (r *relay) recvVarint() (uint64, error) {
	var num uint64
	for i := uint(0); ; i++ {
		// Retrieve the next byte of the varint
		b, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		// Save it and terminate if last byte
		if b > 127 {
			num += uint64(b-128) << (7 * i)
		} else {
			num += uint64(b) << (7 * i)
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
	read := uint64(0)
	for read < size {
		if n, err := r.sockBuf.Read(data[read:]); err != nil {
			return nil, err
		} else {
			read += uint64(n)
		}
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

// Retrieves a connection initialization response.
func (r *relay) procInit() (string, error) {
	// Retrieve the success flag
	op, err := r.recvByte()
	if err != nil {
		return "", err
	}
	// Verify the relay magic string
	switch {
	case op == opInit || op == opDeny:
		if magic, err := r.recvString(); err != nil {
			return "", err
		} else if magic != relayMagic {
			return "", fmt.Errorf("protocol violation: invalid relay magic: %s", magic)
		}
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
		// Read the denial reason
		if reason, err := r.recvString(); err != nil {
			return "", err
		} else {
			return "", fmt.Errorf("connection denied: %s", reason)
		}
	default:
		panic("Unreachable code")
	}
}

// Retrieves a remote broadcast message from the relay and notifies the handler.
func (r *relay) procBroadcast() error {
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleBroadcast(msg)
	return nil
}

// Retrieves a remote request from the relay.
func (r *relay) procRequest() error {
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	req, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleRequest(reqId, req)
	return nil
}

// Retrieves a remote reply to a local request from the relay.
func (r *relay) procReply() error {
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := r.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		go r.handleReply(reqId, nil)
	} else {
		// The request didn't time out, get the reply
		rep, err := r.recvBinary()
		if err != nil {
			return err
		}
		go r.handleReply(reqId, rep)
	}
	return nil
}

// Retrieves a topic publish message from the relay.
func (r *relay) procPublish() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handlePublish(topic, msg)
	return nil
}

// Retrieves a remote tunneling request from the relay.
func (r *relay) procTunnelRequest() error {
	tmpId, err := r.recvVarint()
	if err != nil {
		return err
	}
	buf, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelRequest(tmpId, int(buf))
	return nil
}

// Retrieves the remote reply to a local tunneling request from the relay.
func (r *relay) procTunnelReply() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := r.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		go r.handleTunnelReply(tunId, 0, true)
	} else {
		// The tunnel didn't time out, proceed
		buf, err := r.recvVarint()
		if err != nil {
			return err
		}
		go r.handleTunnelReply(tunId, int(buf), false)
	}
	return nil
}

// Retrieves a remote tunnel data message.
func (r *relay) procTunnelData() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	r.handleTunnelData(tunId, msg) // Note, NOT separate go-routine!
	return nil
}

// Retrieves a remote tunnel message acknowledgment.
func (r *relay) procTunnelAck() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelAck(tunId)
	return nil
}

// Retrieves the remote closure of a tunnel.
func (r *relay) procTunnelClose() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
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
				err = r.procTunnelAck()
			case opTunData:
				err = r.procTunnelData()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				// Graceful close
				closed = true
			default:
				err = fmt.Errorf("unknown opcode: %v", op)
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
