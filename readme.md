# distributed-llm-dissemination

## Usage

1. Create a configuration JSON file.
1. Build with the following command: `go build -o bin/distributor ./cmd`.
1. Launch the distributor with a command `./bin/distributor -id [id] -f [path to the configuration file] -s [path to save dummy layers] -m [distribution mode]`.
    - Type `-m 0` for the simplest algorithm (coordinator sends all layers to receivers).
    - Type `-m 1` for layer retransmission (the coordinator asks other nodes to send layers if possible, randomly selecting a node from the list of nodes which has each layer the coordinator should send).

### An example of configuration file

You can also get an example of a configuration file by just typing `./bin/distributor`, without any flags.

```json
{
    "Nodes": [
        {
            "Id": 0,
            "Addr": "172.31.32.125:8080",
            "IsLeader": true,
            "InitialLayers": {
                "1": {},
                "3": {}
            }
        },
        {
            "Id": 1,
            "Addr": "172.31.39.132:8080",
            "IsLeader": false,
            "InitialLayers": {
                "1": {}
            }
        },
        {
            "Id": 2,
            "Addr": "172.31.40.58:8080",
            "IsLeader": false,
            "InitialLayers": {}
        },
        {
            "Id": 3,
            "Addr": "172.31.36.101:8080",
            "IsLeader": false,
            "InitialLayers": {
                "3": {}
            }
        }
    ],
    "Assignment": {
        "1": {
            "1": {}
        },
        "2": {
            "1": {},
            "3": {}
        },
        "3": {
            "3": {}
        }
    },
    "LayerSize": 1048576
}
```
