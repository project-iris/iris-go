// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

package iris

import (
	"sync/atomic"

	"github.com/project-iris/iris/pool"
	"gopkg.in/inconshreveable/log15.v2"
)

// Callback interface for processing events from a single subscribed topic.
type TopicHandler interface {
	// Callback invoked whenever an event is published to the topic subscribed to
	// by this particular handler.
	HandleEvent(event []byte)
}

// Topic subscription, responsible for enforcing the quality of service limits.
type topic struct {
	// Application layer fields
	handler TopicHandler // Handler for topic events

	// Quality of service fields
	limits *TopicLimits // Limits on the inbound message processing

	eventIdx  uint64           // Index to assign to inbound events for logging purposes
	eventPool *pool.ThreadPool // Queue and concurrency limiter for the event handlers
	eventUsed int32            // Actual memory usage of the event queue

	// Bookkeeping fields
	logger log15.Logger
}

// Creates a new topic subscription.
func newTopic(handler TopicHandler, limits *TopicLimits, logger log15.Logger) *topic {
	top := &topic{
		// Application layer
		handler: handler,

		// Quality of service
		limits:    limits,
		eventPool: pool.NewThreadPool(limits.EventThreads),

		// Bookkeeping
		logger: logger,
	}
	// Start the event processing and return
	top.eventPool.Start()
	return top
}

// Merges the user requested limits with the defaults.
func finalizeTopicLimits(user *TopicLimits) *TopicLimits {
	// If the user didn't specify anything, load the full default set
	if user == nil {
		return &defaultTopicLimits
	}
	// Check each field and merge only non-specified ones
	limits := new(TopicLimits)
	*limits = *user

	if user.EventThreads == 0 {
		limits.EventThreads = defaultTopicLimits.EventThreads
	}
	if user.EventMemory == 0 {
		limits.EventMemory = defaultTopicLimits.EventMemory
	}
	return limits
}

// Schedules a topic event for the subscription handler to process.
func (t *topic) handlePublish(event []byte) {
	id := int(atomic.AddUint64(&t.eventIdx, 1))
	t.logger.Debug("scheduling arrived event", "event", id, "data", logLazyBlob(event))

	// Make sure there is enough memory for the event
	used := int(atomic.LoadInt32(&t.eventUsed)) // Safe, since only 1 thread increments!
	if used+len(event) <= t.limits.EventMemory {
		// Increment the memory usage of the queue and schedule the event
		atomic.AddInt32(&t.eventUsed, int32(len(event)))
		t.eventPool.Schedule(func() {
			// Start the processing by decrementing the memory usage
			atomic.AddInt32(&t.eventUsed, -int32(len(event)))
			t.logger.Debug("handling scheduled event", "event", id)
			t.handler.HandleEvent(event)
		})
		return
	}
	// Not enough memory in the event queue
	t.logger.Error("event exceeded memory allowance", "event", id, "limit", t.limits.EventMemory, "used", used, "size", len(event))
}

// Terminates a topic subscription's internal processing pool.
func (t *topic) terminate() {
	// Wait for queued events to finish running
	t.eventPool.Terminate(false)
}
