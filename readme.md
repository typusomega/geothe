# Goethe

Goethe is a lightweight append only log server. It's meant to be used in event driven systems to enable patterns like event sourcing.

## Roadmap

| Feature                  | Description                                                                                                                                                                                      | State  |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| Cursor storage           | Consumer cursors should be stored in the server to remember the consumer's current cursor position                                                                                               | _TODO_ |
| Nearest Cursor selection | If a consumer provides a non-existing cursor position, the cursor should automatically point to the nearest event. This might be useful for replays from a certain point in time like 2019-05-03 | _TODO_ |
| Distribution             | Goether server instances should be scalable and therefore need some sort of synchronization mechanism                                                                                            | _TODO_ |

## Build

To build the server simply run: `make build`, to build the CLI use the `make cli` command.

### Generate Spec sources

This service provides a grpc API. To generate the new stubs after changing it:
* make sure to have [protoc](https://github.com/protocolbuffers/protobuf/releases) installed
* make sure to have [protoc-gen-go](https://github.com/golang/protobuf) installed; run `install_proto.sh`
* run `make spec`

## License
Unless otherwise noted, the Goethe source files are distributed under the MIT license found in the LICENSE file.