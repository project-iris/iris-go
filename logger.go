// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

// Contains the user configurable logger instance.

package iris

import (
	"fmt"
	"time"

	"gopkg.in/inconshreveable/log15.v2"
)

// User configurable leveled logger.
var Log = log15.New()

// Disables logging by default.
func init() {
	Log.SetHandler(log15.DiscardHandler())

	//Log.SetHandler(log15.LvlFilterHandler(log15.LvlCrit, log15.StdoutHandler))
	//Log.SetHandler(log15.LvlFilterHandler(log15.LvlError, log15.StdoutHandler))
	//Log.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StdoutHandler))
	Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StdoutHandler))
}

// Creates a lazy value that flattens and truncates a data blob for logging.
func logLazyBlob(data []byte) log15.Lazy {
	return log15.Lazy{func() string {
		if len(data) > 256 {
			return fmt.Sprintf("%v ...", data[:256])
		}
		return fmt.Sprintf("%v", data)
	}}
}

// Creates a lazy value that flattens a timeout, with the possibility of having
// infinity as the result (timeout == 0).
func logLazyTimeout(timeout time.Duration) log15.Lazy {
	return log15.Lazy{func() string {
		if timeout == 0 {
			return "infinity"
		}
		return fmt.Sprintf("%v", timeout)
	}}
}
