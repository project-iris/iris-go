// Copyright (c) 2014 Project Iris. All rights reserved.
//
// The current language binding is an official support library of the Iris
// cloud messaging framework, and as such, the same licensing terms apply.
// For details please see http://iris.karalabe.com/downloads#License

/*
Package iris contains the Go binding to the Iris cloud messaging framework.

Note, since GoDoc doesn't support any markup, a significant amount of visual
polishes on the documentation were lost. For the fully marked up version of
the introductory docs, please consult the package readme file.

    https://github.com/project-iris/iris-go/blob/v1/README.md

Background

Iris is an attempt at bringing the simplicity and elegance of cloud computing to
the application layer. Consumer clouds provide unlimited virtual machines at the
click of a button, but leaves it to developer to wire them together. Iris ensures
that you can forget about networking challenges and instead focus on solving your
own domain problems.

It is a completely decentralized messaging solution for simplifying the design
and implementation of cloud services. Among others, Iris features zero-
configuration (i.e. start it up and it will do its magic), semantic addressing
(i.e. application use textual names to address each other), clusters as units
(i.e. automatic load balancing between apps of the same name) and perfect secrecy
(i.e. all network traffic is encrypted).

You can find further infos on the Iris website [http://iris.karalabe.com] and
details of the above features in the core concepts [http://iris.karalabe.com/book/core_concepts]
section of the book of Iris [http://iris.karalabe.com/book]. For the scientifically
inclined, a small collection of papers [http://iris.karalabe.com/papers] is also
available featuring Iris. Slides and videos of previously given public presentations
are published in the talks [http://iris.karalabe.com/talks] page.

There is a growing community on Twitter @iriscmf [https://twitter.com/iriscmf],
Google groups project-iris [https://groups.google.com/group/project-iris] and
GitHub project-iris [https://github.com/project-iris].

Installation

To get the package, execute:

    go get gopkg.in/project-iris/iris-go.v1

To import this package, add the following line to your code:

    import "gopkg.in/project-iris/iris-go.v1"

Refer to it as iris.

Quickstart

Iris uses a relaying architecture, where client applications do not communicate
directly with one another, but instead delegate all messaging operations to a
local relay process responsible for transferring the messages to the correct
destinations. The first step hence to using Iris through any binding is setting
up the local relay node [http://iris.karalabe.com/downloads]. You can find detailed
infos in the Run, Forrest, Run [http://iris.karalabe.com/book/run_forrest_run]
section of the book of Iris [http://iris.karalabe.com/book], but a very simple
way would be to start a developer node.

    > iris -dev
    Entering developer mode
    Generating random RSA key... done.
    Generating random network name... done.

    2014/06/13 18:13:47 main: booting iris overlay...
    2014/06/13 18:13:47 scribe: booting with id 369650985814.
    2014/06/13 18:13:57 main: iris overlay converged with 0 remote connections.
    2014/06/13 18:13:57 main: booting relay service...
    2014/06/13 18:13:57 main: iris successfully booted, listening on port 55555.

Since it generates random credentials, a developer node will not be able to
connect with other remote nodes in the network. However, it provides a quick
solution to start developing without needing to configure a network name and
associated access key. Should you wish to interconnect multiple nodes, please
provide the -net and -rsa flags.

Attaching to the relay

After successfully booting, the relay opens a local TCP endpoint (port 55555 by
default, configurable using -port) through which arbitrarily many entities may
attach. Each connecting entity may also decide whether it becomes a simple client
only consuming the services provided by other participants, or a full fledged
service, also making functionality available to others for consumption.

Connecting as a client can be done trivially by invoking iris.Connect with the
port number of the local relay's client endpoint. After the attachment is
completed, an iris.Connection instance is returned through which messaging can
begin. A client cannot accept inbound requests, broadcasts and tunnels, only
initiate them.

    conn, err := iris.Connect(55555)
    if err != nil {
      log.Fatalf("failed to connect to the Iris relay: %v.", err)
    }
    defer conn.Close()

To provide functionality for consumption, an entity needs to register as a
service. This is slightly more involved, as beside initiating a registration
request, it also needs to specify a callback handler to process inbound events.
First, the callback handler needs to implement the iris.ServiceHandler interface.
After creating the handler, registration can commence by invoking iris.Register
with the port number of the local relay's client endpoint; sub-service cluster
this entity will join as a member; and handler itself to process inbound messages.

    type EchoHandler struct {}

    func (b *EchoHandler) Init(conn *Connection) error              { return nil }
    func (b *EchoHandler) HandleBroadcast(msg []byte)               { }
    func (b *EchoHandler) HandleRequest(req []byte) ([]byte, error) { return req, nil }
    func (b *EchoHandler) HandleTunnel(tun *Tunnel)                 { }
    func (b *EchoHandler) HandleDrop(reason error)                  { }

    func main() {
      service, err := iris.Register(55555, "echo", new(EchoHandler))
      if err != nil {
        log.Fatalf("failed to register to the Iris relay: %v.", err)
      }
      defer service.Unregister()
    }

Upon successful registration, Iris invokes the handler's Init method with the
live iris.Connection object - the service's client connection - through which
the service itself can initiate outbound requests. Init is called only once and
is synchronized before any other handler method is invoked.

Messaging through Iris

Iris supports four messaging schemes: request/reply, broadcast, tunnel and
publish/subscribe. The first three schemes always target a specific cluster:
send a request to one member of a cluster and wait for the reply; broadcast a
message to all members of a cluster; open a streamed, ordered and throttled
communication tunnel to one member of a cluster. The publish/subscribe is
similar to broadcast, but any member of the network may subscribe to the same
topic, hence breaking cluster boundaries.

    Schemes overview figure: https://dl.dropboxusercontent.com/u/10435909/Iris/messaging_schemes.png

Presenting each primitive is out of scope, but for illustrative purposes the
request/reply was included. Given the echo service registered above, we can send
it requests and wait for replies through any client connection. Iris will
automatically locate, route and load balanced between all services registered
under the addressed name.

    request := []byte("some request binary")
    if reply, err := conn.Request("echo", request, time.Second); err != nil {
      log.Printf("failed to execute request: %v.", err)
    } else {
      fmt.Printf("reply arrived: %v.", string(reply))
    }

An expanded summary of the supported messaging schemes can be found in the core
concepts [http://iris.karalabe.com/book/core_concepts] section of the book of
Iris [http://iris.karalabe.com/book]. A detailed presentation and analysis of
each individual primitive will be added soon.

Error handling

The binding uses the idiomatic Go error handling mechanisms of returning error
instances whenever a failure occurs. However, there are a few common cases that
need to be individually checkable, hence a few special errors values and types
have been introduced.

Many operations - such as requests and tunnels - can time out. To allow checking
for this particular failure, Iris returns iris.ErrTimeout in such scenarios.
Similarly, connections, services and tunnels may fail, in the case of which all
pending operations terminate with iris.ErrClosed.

Additionally, the requests/reply pattern supports sending back an error instead of
a reply to the caller. To enable the originating node to check whether a request
failed locally or remotely, all remote errors are wrapped in an iris.RemoteError
type.

    _, err := conn.Request("cluster", request, timeout)
    switch err {
      case nil:
        // Request completed successfully
      case iris.ErrTimeout:
        // Request timed out
      case iris.ErrClosed:
        // Connection terminated
      default:
        if _, ok := err.(*iris.RemoteError); ok {
          // Request failed remotely
        } else {
          // Requesting failed locally
        }
    }

Logging

For logging purposes, the Go binding uses inconshreveable's [https://github.com/inconshreveable]
log15 [https://github.com/inconshreveable/log15] library (version v2). By default,
INFO level logs are collected and printed to stderr. This level allows tracking
life-cycle events such as client and service attachments, topic subscriptions and
tunnel establishments. Further log entries can be requested by lowering the level
to DEBUG, effectively printing all messages passing through the binding.

The binding's logger can be fine-tuned through the iris.Log variable. Below are
a few common configurations.

    // Discard all log entries
    iris.Log.SetHandler(log15.DiscardHandler())

    // Log DEBUG level entries to STDERR
    iris.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StderrHandler))

Each iris.Connection, iris.Service and iris.Tunnel has an embedded logger, through
which contextual log entries may be printed (i.e. tagged with the specific id of
the attached entity).

    conn, _ := iris.Connect(55555)
    defer conn.Close()

    conn.Log.Debug("debug entry, hidden by default")
    conn.Log.Info("info entry, client context included")
    conn.Log.Warn("warning entry", "extra", "some value")
    conn.Log.Crit("critical entry", "bool", false, "int", 1, "string", "two")

As you can see below, all log entries have been automatically tagged with the
'client' attribute, set to the id of the current connection. Since the default
log level is INFO, the conn.Log.Debug invocation has no effect. Additionally,
arbitrarily many key-value pairs may be included in the entry.

    INFO[06-22|18:39:49] connecting new client                    client=1 relay_port=55555
    INFO[06-22|18:39:49] client connection established            client=1
    INFO[06-22|18:39:49] info entry, client context included      client=1
    WARN[06-22|18:39:49] warning entry                            client=1 extra="some value"
    CRIT[06-22|18:39:49] critical entry                           client=1 bool=false int=1 string=two
    INFO[06-22|18:39:49] detaching from relay                     client=1

For further capabilities, configurations and details about the logger, please
consult the log15 docs [https://godoc.org/github.com/inconshreveable/log15].

Additional goodies

You can find a teaser presentation, touching on all the key features of the
library through a handful of challenges and their solutions. The recommended
version is the playground [http://play.iris.karalabe.com/talks/binds/go.slide],
containing modifiable and executable code snippets, but a read only version
[http://iris.karalabe.com/talks/binds/go.slid]) is also available.

*/
package iris
