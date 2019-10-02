# Goethe

Goethe is a lightweight append-only event log server. It's meant to be used in event driven systems to enable patterns like event sourcing.

## TODOs

| Feature                   | Description                                                                                                                                                                                      | State  |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| Time based event index    | We need an event index to allow for seeking to a given point in time. This index needs to be on a configurable basis---like 1h, 1d---where we capture the last event and store them in the db    | _TODO_ |
| Nearest Cursor selection  | If a consumer provides a non-existing cursor position, the cursor should automatically point to the nearest event. This might be useful for replays from a certain point in time like 2019-05-03 | _TODO_ |                                                                                    | _TODO_ |
| Service discovery K8s     | Goethe server instances need to be able to discover members of a cluster. At first we will stick to K8s features to do so.                                                                       | _TODO_ |
| Leader election K8s       | Goethe server instances should be able to select a leader which is the master writer in the cluster. At first we will stick to K8s features to do so.                                            | _TODO_ |
| Leader write distribution | Goethe Leaders have to make sure writes are persisted on all Geother instances in the cluster.                                                                                                   | _TODO_ |
| Gossip                    | Goethe server instances need to at least inform each others about there health state                                                                                                             | _TODO_ |

## Architecture

### Concepts

Goethe is meant to be the event store in event driven systems. This section shall guide you through the top level concepts.

#### Events

Event driven systems use events as their main communication channels. Events are basically facts about things that happend in the system. Naturally, events are immutable, they are facts. This is why having an append-only event log in an event driven system seems to make sense.

Goethe does not put any constraints on the events you want to be flowing through your system, and you will find a lot of good advise like in the [CloudEvents Spec](https://github.com/cloudevents/spec).
We consider events as plain old byte arrays, for you to have the complete freedom about what you are doing.

#### Cursors

We want to deliver maximum ease to Goethe clients. The default usecase for a topic consumer is to start we she left off before.
Of course there are times when replays are necessary and a consumer wants to read a complete topic from its beginnig.

This is why we think about cursors owned by a certain service pointing to a certain event in a certain topic.

A consumer is able to tell Goethe the a specific event to start consuming from. This might be because the consumer stored her last known position or to replay events since a certain occurence.
For consumers' convenience we also enable reading from where a consumer left off by leaving the cursor's `current_event` field blank and Goethe starts from server's the last known cursor position of the service.

__Future Features__:  

If a consumer wants to read events from a topic at a specific point in time, she can just provide a cursor pointing to that time. Goethe will select the next event which was commited after that time.

In cases where consumers start to become slow, you might start scaling the consumer service horizontally to increase performance and share the load among the instances.
This is also something which can be addressed by the use of proper cursors. If multiple instances use the same serviceID, the service's cursor is moved for all instances.

### Storage

Each Goethe instance uses a local [LevelDB](https://github.com/syndtr/goleveldb) (the Go implementation) as its storage backend.
LevelDB is incredibly fast in sequential reading and writing and therefore performs very well in an append-only log.
LevelDB's default key sorting algorithm uses bytewise comparison. 

To achieve maximum performance, our event keys need to be poperly sorted by their occurence.
This is why we store them behind keys with the following pattern: `{TOPIC_ID}:{EVENT_ID}` where `EVENT_ID` is the UNIX nano timestamp when the event was originally received.
Nano timestamps are defacto unique in a single topic, hence, collisions are very unlikely.
Having event keys with topic prefixex, all events of a certain topics are arranged in a sequence.

That results in reading sequentially from a topic and therefore getting maximum speed.

## Build

To build the server simply run: `make build`, to build the CLI use the `make cli` command.

### Generate Spec sources

This service provides a grpc API. To generate the new stubs after changing it:
* make sure to have [protoc](https://github.com/protocolbuffers/protobuf/releases) installed
* make sure to have [protoc-gen-go](https://github.com/golang/protobuf) installed; run `install_proto.sh`
* run `make spec`

## License
Unless otherwise noted, the Goethe source files are distributed under the MIT license found in the LICENSE file.