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

## deploy.sh

You can also use `deploy.sh` for easier setup.

```sh
./conf/deploy.sh 3.70.180.207 63.182.167.8 63.177.62.38 3.71.95.252 3.69.250.26
```

Here is an example:

```sh
ubuntu@ip-172-31-2-47:~$ lsblk
NAME         MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
loop0          7:0    0  27.6M  1 loop /snap/amazon-ssm-agent/11797
loop1          7:1    0    74M  1 loop /snap/core22/2163
loop2          7:2    0  50.9M  1 loop /snap/snapd/25577
nvme0n1      259:0    0     8G  0 disk
├─nvme0n1p1  259:2    0     7G  0 part /
├─nvme0n1p14 259:3    0     4M  0 part
├─nvme0n1p15 259:4    0   106M  0 part /boot/efi
└─nvme0n1p16 259:5    0   913M  0 part /boot
nvme1n1      259:1    0 220.7G  0 disk

ubuntu@ip-172-31-2-47:~$  sudo sh init.sh nvme1n1
ubuntu@ip-172-31-2-47:~$  sudo sh exe.sh 0 2 1 1 # ID, mode, is_disk, is_setup (if true, it writes dummy files to the disk)
```
