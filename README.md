  Iris Go binding
===================

This is the official Go language binding for the Iris cloud messaging framework. If you are unfamiliar with Iris, please read the next introductory section. It contains a short summary, as well as some valuable pointers on where you can discover more.

  Background
-------------------

Iris is an attempt at bringing the simplicity and elegance of cloud computing to the application layer. Consumer clouds provide unlimited virtual machines at the click of a button, but leaves it to developer to wire them together. Iris ensures that you can forget about networking challenges and instead focus on solving your own domain problems.

It is a completely decentralized messaging solution for simplifying the design and implementation of cloud services. Among others, Iris features zero-configuration (i.e. start it up and it will do its magic), semantic addressing (i.e. application use textual names to address each other), clusters as units (i.e. automatic load balancing between apps of the same name) and perfect secrecy (i.e. all network traffic is encrypted).

You can find further infos on the [Iris website](http://iris.karalabe.com) and details of the above features in the [core concepts](http://iris.karalabe.com/book/core_concepts) section of [the book of Iris](http://iris.karalabe.com/book). For the scientifically inclined, a small collection of [papers](http://iris.karalabe.com/papers) is also available featuring Iris. Slides and videos of previously given public presentations are published in the [talks](http://iris.karalabe.com/talks) page.

There is a growing community on Twitter [@iriscmf](https://twitter.com/iriscmf), Google groups [project-iris](https://groups.google.com/group/project-iris) and GitHub [project-iris](https://github.com/project-iris).

  Installation
----------------

To get the package, execute:

    go get gopkg.in/project-iris/iris-go.v1

To import this package, add the following line to your code:

    import "gopkg.in/project-iris/iris-go.v1"

Refer to it as _iris_.

  Quickstart
--------------

Iris uses a relaying architecture, where client applications do not communicate directly with one another, but instead delegate all messaging operations to a local relay process responsible for transferring the messages to the correct destinations. The first step hence to using Iris through any binding is setting up the local [_relay_ _node_](http://iris.karalabe.com/downloads). You can find detailed infos in the [Run, Forrest, Run](http://iris.karalabe.com/book/run_forrest_run) section of [the book of Iris](http://iris.karalabe.com/book), but a very simple way would be to start a _developer_ node.

    > iris -dev
    Entering developer mode
    Generating random RSA key... done.
    Generating random network name... done.

    2014/06/13 18:13:47 main: booting iris overlay...
    2014/06/13 18:13:47 scribe: booting with id 369650985814.
    2014/06/13 18:13:57 main: iris overlay converged with 0 remote connections.
    2014/06/13 18:13:57 main: booting relay service...
    2014/06/13 18:13:57 main: iris successfully booted, listening on port 55555.

Since it generates random credentials, a developer node will not be able to connect with other remote nodes in the network. However, it provides a quick solution to start developing without needing to configure a _network_ _name_ and associated _access_ _key_. Should you wish to interconnect multiple nodes, please provide the `-net` and `-rsa` flags.

### Attaching to the relay

After successfully booting, the relay opens a _local_ TCP endpoint (port `55555` by default, configurable using `-port`) through which arbitrarily many entities may attach. Each connecting entity may also decide whether it becomes a simple _client_ only consuming the services provided by other participants, or a full fledged _service_, also making functionality available to others for consumption.

Connecting as a client can be done trivially by invoking [`iris.Connect`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Connect) with the port number of the local relay's client endpoint. After the attachment is completed, an [`iris.Connection`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Connection) instance is returned through which messaging can begin. A client cannot accept inbound requests, broadcasts and tunnels, only initiate them.

```go
conn, err := iris.Connect(55555)
if err != nil {
  log.Fatalf("failed to connect to the Iris relay: %v.", err)
}
defer conn.Close()
```

To provide functionality for consumption, an entity needs to register as a service. This is slightly more involved, as beside initiating a registration request, it also needs to specify a callback handler to process inbound events. First, the callback handler needs to implement the [`iris.ServiceHandler`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#ServiceHandler) interface. After creating the handler, registration can commence by invoking [`iris.Register`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Register) with the port number of the local relay's client endpoint; sub-service cluster this entity will join as a member; and handler itself to process inbound messages.

```go
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
```

Upon successful registration, Iris invokes the handler's `Init` method with the live [`iris.Connection`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Connection) object - the service's client connection - through which the service itself can initiate outbound requests. `Init` is called only once and is synchronized before any other handler method is invoked.

### Messaging through Iris

Iris supports four messaging schemes: request/reply, broadcast, tunnel and publish/subscribe. The first three schemes always target a specific cluster: send a request to _one_ member of a cluster and wait for the reply; broadcast a message to _all_ members of a cluster; open a streamed, ordered and throttled communication tunnel to _one_ member of a cluster. The publish/subscribe is similar to broadcast, but _any_ member of the network may subscribe to the same topic, hence breaking cluster boundaries.

<img src="http://iris.karalabe.com/talks/fosdem/schemes.png" style="height: 175px; display: block; margin-left: auto; margin-right: auto;" \>

Presenting each primitive is out of scope, but for illustrative purposes the request/reply was included. Given the echo service registered above, we can send it requests and wait for replies through any client connection. Iris will automatically locate, route and load balanced between all services registered under the addressed name.

```go
request := []byte("some request binary")
if reply, err := conn.Request("echo", request, time.Second); err != nil {
  log.Printf("failed to execute request: %v.", err)
} else {
  fmt.Printf("reply arrived: %v.", string(reply))
}
```

### Logging

For logging purposes, the Go binding uses [inconshreveable](https://github.com/inconshreveable)'s [log15](https://github.com/inconshreveable/log15) library (version v2). By default, _INFO_ level logs are collected and printed to _stderr_. This level allows tracking life-cycle events such as client and service attachments, topic subscriptions and tunnel establishments. Further log entries can be requested by lowering the level to _DEBUG_, effectively printing all messages passing through the binding.

The binding's logger can be fine-tuned through the `iris.Log` variable. Below are a few common configurations.

```go
// Discard all log entries
iris.Log.SetHandler(log15.DiscardHandler())

// Log DEBUG level entries to STDERR
iris.Log.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, log15.StderrHandler))
```

Each [`iris.Connection`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Connection), [`iris.Service`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Service) and [`iris.Tunnel`](http://godoc.org/gopkg.in/project-iris/iris-go.v1#Tunnel) has a public embedded logger, through which contextual log entries may be printed (i.e. tagged with the specific ID of the attached entity).

```go
conn, err := iris.Connect(55555)
if err != nil {
  log.Fatalf("failed to connect to Iris: %v.", err)
}
defer conn.Close()

conn.Log.Warn("log entry with connection context")
```

Looking at the output, you can see the custom log entry being tagged with the ID of the client connection.

```
INFO[06-22|14:03:05] connecting new client                    client=1 relay_port=55555
INFO[06-22|14:03:05] client connection established            client=1
WARN[06-22|14:03:05] log entry with connection context        client=1
INFO[06-22|14:03:05] detaching from relay                     client=1
```

### Additional goodies

A demo presentation is also available, touching on all the features of the library through a handful of challenges and their solutions. The recommended version is the [playground](http://play.iris.karalabe.com/talks/binds/go.slide), containing modifiable and executable code snippets. A [read only version](http://iris.karalabe.com/talks/binds/go.slide) is also available.

  Contributions
-----------------

Currently my development aims are to stabilize the project and its language bindings. Hence, although I'm open and very happy for any and all contributions, the most valuable ones are tests, benchmarks and actual binding usage to reach a high enough quality.

Due to the already significant complexity of the project (Iris in general), I kindly ask anyone willing to pinch in to first file an [issue](https://github.com/project-iris/iris-go/issues) with their plans to achieve a best possible integration :).

Additionally, to prevent copyright disputes and such, a signed contributor license agreement is required to be on file before any material can be accepted into the official repositories. These can be filled online via either the [Individual Contributor License Agreement](http://iris.karalabe.com/icla) or the [Corporate Contributor License Agreement](http://iris.karalabe.com/ccla).
