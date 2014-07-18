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
	"sync/atomic"
	"time"
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

	opTunInit     = 0x09 // Out: tunnel construction request    | In: tunnel initiation
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
func (c *Connection) sendByte(data byte) error {
	return c.sockBuf.WriteByte(data)
}

// Serializes a boolean into the relay connection.
func (c *Connection) sendBool(data bool) error {
	if data {
		return c.sendByte(1)
	}
	return c.sendByte(0)
}

// Serializes a variable int using base 128 encoding into the relay connection.
func (c *Connection) sendVarint(data uint64) error {
	for data > 127 {
		// Internal byte, set the continuation flag and send
		if err := c.sendByte(byte(128 + data%128)); err != nil {
			return err
		}
		data /= 128
	}
	// Final byte, send and return
	return c.sendByte(byte(data))
}

// Serializes a length-tagged binary array into the relay connection.
func (c *Connection) sendBinary(data []byte) error {
	if err := c.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if _, err := c.sockBuf.Write([]byte(data)); err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay connection.
func (c *Connection) sendString(data string) error {
	return c.sendBinary([]byte(data))
}

// Serializes a packet through a closure into the relay connection.
func (c *Connection) sendPacket(closure func() error) error {
	// Increment the pending write count
	atomic.AddInt32(&c.sockWait, 1)

	// Acquire the socket lock
	c.sockLock.Lock()
	defer c.sockLock.Unlock()

	// Send the packet itself
	if err := closure(); err != nil {
		// Decrement the pending count and error out
		atomic.AddInt32(&c.sockWait, -1)
		return err
	}
	// Flush the stream if no more messages are pending
	if atomic.AddInt32(&c.sockWait, -1) == 0 {
		return c.sockBuf.Flush()
	}
	return nil
}

// Sends a connection initiation.
func (c *Connection) sendInit(cluster string) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opInit); err != nil {
			return err
		}
		if err := c.sendString(clientMagic); err != nil {
			return err
		}
		if err := c.sendString(protoVersion); err != nil {
			return err
		}
		return c.sendString(cluster)
	})
}

// Sends a connection tear-down initiation.
func (c *Connection) sendClose() error {
	return c.sendPacket(func() error {
		return c.sendByte(opClose)
	})
}

// Sends an application broadcast initiation.
func (c *Connection) sendBroadcast(cluster string, message []byte) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opBroadcast); err != nil {
			return err
		}
		if err := c.sendString(cluster); err != nil {
			return err
		}
		return c.sendBinary(message)
	})
}

// Sends an application request initiation.
func (c *Connection) sendRequest(id uint64, cluster string, request []byte, timeout int) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opRequest); err != nil {
			return err
		}
		if err := c.sendVarint(id); err != nil {
			return err
		}
		if err := c.sendString(cluster); err != nil {
			return err
		}
		if err := c.sendBinary(request); err != nil {
			return err
		}
		return c.sendVarint(uint64(timeout))
	})
}

// Sends an application reply initiation.
func (c *Connection) sendReply(id uint64, reply []byte, fault string) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opReply); err != nil {
			return err
		}
		if err := c.sendVarint(id); err != nil {
			return err
		}
		success := (len(fault) == 0)
		if err := c.sendBool(success); err != nil {
			return err
		}
		if success {
			return c.sendBinary(reply)
		} else {
			return c.sendString(fault)
		}
	})
}

// Sends a topic subscription.
func (c *Connection) sendSubscribe(topic string) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opSubscribe); err != nil {
			return err
		}
		return c.sendString(topic)
	})
}

// Sends a topic subscription removal.
func (c *Connection) sendUnsubscribe(topic string) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opUnsubscribe); err != nil {
			return err
		}
		return c.sendString(topic)
	})
}

// Sends a topic event publish.
func (c *Connection) sendPublish(topic string, event []byte) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opPublish); err != nil {
			return err
		}
		if err := c.sendString(topic); err != nil {
			return err
		}
		return c.sendBinary(event)
	})
}

// Sends a tunnel construction request.
func (c *Connection) sendTunnelInit(id uint64, cluster string, timeout int) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opTunInit); err != nil {
			return err
		}
		if err := c.sendVarint(id); err != nil {
			return err
		}
		if err := c.sendString(cluster); err != nil {
			return err
		}
		return c.sendVarint(uint64(timeout))
	})
}

// Sends a tunnel confirmation.
func (c *Connection) sendTunnelConfirm(buildId, tunId uint64) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opTunConfirm); err != nil {
			return err
		}
		if err := c.sendVarint(buildId); err != nil {
			return err
		}
		return c.sendVarint(tunId)
	})
}

// Sends a tunnel transfer allowance.
func (c *Connection) sendTunnelAllowance(id uint64, space int) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opTunAllow); err != nil {
			return err
		}
		if err := c.sendVarint(id); err != nil {
			return err
		}
		return c.sendVarint(uint64(space))
	})
}

// Sends a tunnel data exchange.
func (c *Connection) sendTunnelTransfer(id uint64, sizeOrCont int, payload []byte) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opTunTransfer); err != nil {
			return err
		}
		if err := c.sendVarint(id); err != nil {
			return err
		}
		if err := c.sendVarint(uint64(sizeOrCont)); err != nil {
			return err
		}
		return c.sendBinary(payload)
	})
}

// Sends a tunnel termination request.
func (c *Connection) sendTunnelClose(id uint64) error {
	return c.sendPacket(func() error {
		if err := c.sendByte(opTunClose); err != nil {
			return err
		}
		return c.sendVarint(id)
	})
}

// Retrieves a single byte from the relay connection.
func (c *Connection) recvByte() (byte, error) {
	return c.sockBuf.ReadByte()
}

// Retrieves a boolean from the relay connection.
func (c *Connection) recvBool() (bool, error) {
	b, err := c.recvByte()
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

// Retrieves a variable int in base 128 encoding from the relay connection.
func (c *Connection) recvVarint() (uint64, error) {
	var num uint64
	for i := uint(0); ; i++ {
		chunk, err := c.recvByte()
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
func (c *Connection) recvBinary() ([]byte, error) {
	// Fetch the length of the binary blob
	size, err := c.recvVarint()
	if err != nil {
		return nil, err
	}
	// Fetch the blob itself
	data := make([]byte, size)
	if _, err := io.ReadFull(c.sockBuf, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay connection.
func (c *Connection) recvString() (string, error) {
	if data, err := c.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Retrieves a connection initiation response (either accept or deny).
func (c *Connection) procInit() (string, error) {
	// Retrieve the response opcode
	op, err := c.recvByte()
	if err != nil {
		return "", err
	}
	// Verify the opcode validity and relay magic string
	switch {
	case op == opInit || op == opDeny:
		if magic, err := c.recvString(); err != nil {
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
		if version, err := c.recvString(); err != nil {
			return "", err
		} else {
			return version, nil
		}
	case opDeny:
		// Read the reason for connection denial
		if reason, err := c.recvString(); err != nil {
			return "", err
		} else {
			return "", fmt.Errorf("connection denied: %s", reason)
		}
	default:
		panic("unreachable code")
	}
}

// Retrieves a connection tear-down notification.
func (c *Connection) procClose() (string, error) {
	return c.recvString()
}

// Retrieves an application broadcast delivery.
func (c *Connection) procBroadcast() error {
	message, err := c.recvBinary()
	if err != nil {
		return err
	}
	c.handleBroadcast(message)
	return nil
}

// Retrieves an application request delivery.
func (c *Connection) procRequest() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	request, err := c.recvBinary()
	if err != nil {
		return err
	}
	timeout, err := c.recvVarint()
	if err != nil {
		return err
	}
	c.handleRequest(id, request, time.Duration(timeout)*time.Millisecond)
	return nil
}

// Retrieves an application reply delivery.
func (c *Connection) procReply() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := c.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		c.handleReply(id, nil, "")
		return nil
	}
	// The request didn't time out, get the result
	success, err := c.recvBool()
	if err != nil {
		return err
	}
	if success {
		reply, err := c.recvBinary()
		if err != nil {
			return err
		}
		c.handleReply(id, reply, "")
	} else {
		fault, err := c.recvString()
		if err != nil {
			return err
		}
		c.handleReply(id, nil, fault)
	}
	return nil
}

// Retrieves a topic event delivery.
func (c *Connection) procPublish() error {
	topic, err := c.recvString()
	if err != nil {
		return err
	}
	event, err := c.recvBinary()
	if err != nil {
		return err
	}
	go c.handlePublish(topic, event)
	return nil
}

// Retrieves a tunnel initiation message.
func (c *Connection) procTunnelInit() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	chunkLimit, err := c.recvVarint()
	if err != nil {
		return err
	}
	c.handleTunnelInit(id, int(chunkLimit))
	return nil
}

// Retrieves a tunnel construction result.
func (c *Connection) procTunnelResult() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := c.recvBool()
	if err != nil {
		return err
	}
	if timeout {
		c.handleTunnelResult(id, 0)
		return nil
	}
	// The tunnel didn't time out, proceed
	chunkLimit, err := c.recvVarint()
	if err != nil {
		return err
	}
	c.handleTunnelResult(id, int(chunkLimit))
	return nil
}

// Retrieves a tunnel transfer allowance message.
func (c *Connection) procTunnelAllowance() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	space, err := c.recvVarint()
	if err != nil {
		return err
	}
	c.handleTunnelAllowance(id, int(space))
	return nil
}

// Retrieves a tunnel data exchange message.
func (c *Connection) procTunnelTransfer() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	size, err := c.recvVarint()
	if err != nil {
		return err
	}
	payload, err := c.recvBinary()
	if err != nil {
		return err
	}
	c.handleTunnelTransfer(id, int(size), payload)
	return nil
}

// Retrieves a tunnel closure notification.
func (c *Connection) procTunnelClose() error {
	id, err := c.recvVarint()
	if err != nil {
		return err
	}
	reason, err := c.recvString()
	if err != nil {
		return err
	}
	go c.handleTunnelClose(id, reason)
	return nil
}

// Retrieves messages from the client connection and keeps processing them until
// either the relay closes (graceful close) or the connection drops.
func (c *Connection) process() {
	var op byte
	var err error
	for closed := false; !closed && err == nil; {
		// Retrieve the next opcode and call the specific handler for the rest
		if op, err = c.recvByte(); err == nil {
			switch op {
			case opBroadcast:
				err = c.procBroadcast()
			case opRequest:
				err = c.procRequest()
			case opReply:
				err = c.procReply()
			case opPublish:
				err = c.procPublish()
			case opTunInit:
				err = c.procTunnelInit()
			case opTunConfirm:
				err = c.procTunnelResult()
			case opTunAllow:
				err = c.procTunnelAllowance()
			case opTunTransfer:
				err = c.procTunnelTransfer()
			case opTunClose:
				err = c.procTunnelClose()
			case opClose:
				// Retrieve any reason for remote closure
				if reason, cerr := c.procClose(); cerr != nil {
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
	c.sock.Close()
	close(c.term)

	// Notify the application of the connection closure
	c.handleClose(err)

	// Wait for termination sync
	errc := <-c.quit
	errc <- err
}
