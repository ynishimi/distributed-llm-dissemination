import seaborn as sns
import matplotlib.pyplot as plt
from dataclasses import dataclass
import cvxpy as cp
import numpy as np
import pandas as pd

np.set_printoptions(precision=3)

sns.set_theme()
sns.set_context("paper")


@dataclass
class Config:
    name: str
    '''name of the config'''
    n: int
    '''num of nodes'''
    l: int
    '''num of layers'''
    layer_size: float
    '''size of layers (= |l|)'''
    disk_sizes: list[int]
    '''size of disks'''
    ram_sizes: list[int]
    '''size of rams'''


def calc(disk_size, ram_size, config: Config):

    # problem data

    NETWORKBW = (12.5/8 * 10**9) / (2**30)  # 12.5 Gbps
    # DISKBW = 200 / (2**10)  # 200 MiB/s
    DISKBW = 1  # 1 GiB/s
    RAMBW = 1  # 1 GiB/s
    CLIENTBW = 15.5 / (2**10)  # 15.5 MiB
    # CLIENTBW = 0

    # client is connected to node 0 or 1
    CLIENT_OWNER_PRIMARY = 0
    CLIENT_OWNER_SECONDERY = 1

    n = config.n
    assigned_layer = config.l
    layer_size = config.layer_size

    LAMBDA_BASE = 1
    '''One crash per unit time [/time]'''
    # PI_STABLE = 0.8
    PI_STABLE = 0
    PI_UNSTABLE = 0
    # PI_UNSTABLE = 0.8

    LAMBDA = np.array([LAMBDA_BASE for _ in range(n)])
    '''The expected Poisson count (mean) for node'''
    PI = np.array([PI_UNSTABLE if i == 0 else PI_STABLE for i in range(n)])
    '''Probability that the node is fault-tolerant'''

    S = np.array([layer_size for _ in range(assigned_layer)])
    '''size of layers (= |l|)'''
    C = np.array([disk_size for _ in range(n)])
    '''size of disks'''
    C_RAM = np.array([ram_size for _ in range(n)])
    '''size of rams'''
    B = np.array([NETWORKBW for _ in range(n)])
    '''network bandwidth of nodes'''
    D = np.array([DISKBW for _ in range(n)])
    '''rate of disks'''
    D_RAM = np.array([RAMBW for _ in range(n)])
    '''rate of rams'''

    INIT_ASSIGNMENT = [[assigned_layer//n * i + j for j in range(assigned_layer//n)]
                       for i in range(n)]
    DUMMY_ASSIGNMENT = [[assigned_layer//(n-1) * i + j for j in range(assigned_layer//(n-1))]
                        for i in range(n-1)]
    ALT_ASSIGNMENTS = [DUMMY_ASSIGNMENT[:i] + [[]] +
                       DUMMY_ASSIGNMENT[i:] for i in range(n)]

    # variables

    # a proportion of the layer l stored to node k
    x = cp.Variable((n, assigned_layer), nonneg=True)
    x_ram = cp.Variable((n, assigned_layer), nonneg=True)
    t = cp.Variable(n, nonneg=True)  # downtime for each node's crash scenario
    f = {}  # edges in the flow graph
    for crashed_node in range(n):  # for each crash scenario:
        f[crashed_node] = {}
        f[crashed_node]['src_sender'] = cp.Variable(n, nonneg=True)
        f[crashed_node]['sender_disk'] = cp.Variable(
            n, nonneg=True)
        f[crashed_node]['sender_ram'] = cp.Variable(
            n, nonneg=True)
        f[crashed_node]['disk_layer'] = cp.Variable(
            (n, assigned_layer), nonneg=True)
        f[crashed_node]['ram_layer'] = cp.Variable(
            (n, assigned_layer), nonneg=True)
        f[crashed_node]['layer_receiver'] = cp.Variable(
            assigned_layer, nonneg=True)
        f[crashed_node]['receiver_sink'] = cp.Variable(n, nonneg=True)

        # client
        f[crashed_node]['sender_client'] = cp.Variable(1, nonneg=True)
        f[crashed_node]['client_layer'] = cp.Variable(
            (1, assigned_layer), nonneg=True)

    # constraints

    constraints = []
    constraints.append(x <= 1)
    constraints.append(x @ S <= C)  # disk capacity constraint

    constraints.append(x_ram <= 1)
    constraints.append(x_ram @ S <= C_RAM)  # ram capacity constraint

    # proportion constraint: cannot hold more than 1 layer combining disk and ram
    constraints.append(x + x_ram <= 1)

    for crashed_node in range(n):
        # network bw
        constraints.append(f[crashed_node]['src_sender'] <=
                           cp.multiply(B, t[crashed_node]))

        client_node = CLIENT_OWNER_PRIMARY if crashed_node != CLIENT_OWNER_PRIMARY else CLIENT_OWNER_SECONDERY

        # missing layers should be sent, but existing layers should not be sent or saved
        local_disk_consumed = {}
        local_ram_consumed = {}
        local_client_consumed = {}
        for receiver in range(n):
            assigned_layers = ALT_ASSIGNMENTS[crashed_node][receiver]
            for assigned_layer in assigned_layers:
                if assigned_layer in INIT_ASSIGNMENT[receiver]:
                    # already has l
                    constraints.append(
                        f[crashed_node]['layer_receiver'][assigned_layer] == 0)
                    constraints.append(x[receiver, assigned_layer] == 0)
                    constraints.append(x_ram[receiver, assigned_layer] == 0)
                else:
                    # missing l
                    constraints.append(
                        f[crashed_node]['layer_receiver'][assigned_layer] == S[assigned_layer])

            # layers that the receiver node should receive for given crashed_node
            layers_to_receive = [
                assigned_layer for assigned_layer in assigned_layers if assigned_layer not in INIT_ASSIGNMENT[receiver]
            ]

            # amount of data directly loaded from the local disk/ram/client
            local_disk_consumed[receiver] = cp.sum(
                f[crashed_node]['disk_layer'][receiver, layers_to_receive]) if layers_to_receive else 0
            local_ram_consumed[receiver] = cp.sum(
                f[crashed_node]['ram_layer'][receiver, layers_to_receive]) if layers_to_receive else 0
            local_client_consumed[receiver] = cp.sum(f[crashed_node]['client_layer'][0, layers_to_receive]) if (
                layers_to_receive and receiver == client_node) else 0

            if layers_to_receive:
                constraints.append(
                    # flow: each layer is sent to a receiver, based on an alternative assignment. Ignore local disk and ram's read, as it doesn't use the network bandwidth.
                    cp.sum(f[crashed_node]['layer_receiver'][layers_to_receive]) - local_disk_consumed[receiver] - local_ram_consumed[receiver] == f[crashed_node]['receiver_sink'][receiver])
            else:
                constraints.append(
                    f[crashed_node]['receiver_sink'][receiver] == 0)

        for sender in range(n):
            # flow (sender)
            if sender == client_node:
                # local disk/ram/client's read do not involve transmission over the network.
                constraints.append(cp.sum(f[crashed_node]['src_sender'][sender]) ==
                                   f[crashed_node]['sender_disk'][sender] + f[crashed_node]['sender_ram'][sender] + f[crashed_node]['sender_client'] - local_disk_consumed[sender] - local_ram_consumed[sender] - local_client_consumed[sender])
            else:
                constraints.append(f[crashed_node]['src_sender'][sender]
                                   == f[crashed_node]['sender_disk'][sender] + f[crashed_node]['sender_ram'][sender] - local_disk_consumed[sender] - local_ram_consumed[sender])

        # disk bw
        constraints.append(f[crashed_node]['sender_disk'] <=
                           cp.multiply(D, t[crashed_node]))

        # ram bw
        constraints.append(f[crashed_node]['sender_ram'] <=
                           cp.multiply(D_RAM, t[crashed_node]))

        # client bw
        constraints.append(f[crashed_node]['sender_client']
                           <= CLIENTBW * t[crashed_node])

        # flow
        constraints.append(f[crashed_node]['sender_disk'] ==
                           cp.sum(f[crashed_node]['disk_layer'], axis=1))

        constraints.append(f[crashed_node]['sender_ram'] ==
                           cp.sum(f[crashed_node]['ram_layer'], axis=1))

        # client flow
        constraints.append(f[crashed_node]['sender_client'] ==
                           cp.sum(f[crashed_node]['client_layer'], axis=1))

        # data obtained by each disk
        constraints.append(f[crashed_node]['disk_layer'] <= cp.multiply(x, S))

        # data obtained by each ram
        constraints.append(f[crashed_node]['ram_layer']
                           <= cp.multiply(x_ram, S))

        # flow
        for node_asgn in range(len(ALT_ASSIGNMENTS[crashed_node])):
            if len(ALT_ASSIGNMENTS[crashed_node][node_asgn]) == 0:
                continue
            constraints.append(cp.sum(f[crashed_node]['disk_layer'][:, ALT_ASSIGNMENTS[crashed_node][node_asgn]], axis=0) +
                               cp.sum(f[crashed_node]['ram_layer'][:, ALT_ASSIGNMENTS[crashed_node][node_asgn]], axis=0) +
                               cp.sum(f[crashed_node]['client_layer'][:, ALT_ASSIGNMENTS[crashed_node][node_asgn]], axis=0) ==
                               f[crashed_node]['layer_receiver'][ALT_ASSIGNMENTS[crashed_node][node_asgn]])

        # network bw
        constraints.append(f[crashed_node]['receiver_sink']
                           <= cp.multiply(B, t[crashed_node]))
        # network bandwidth should be shared among the same node (sender k and receiver k)
        constraints.append(f[crashed_node]['src_sender'] + f[crashed_node]
                           ['receiver_sink'] <= cp.multiply(B, t[crashed_node]))
        # crashed node doesn't send layers
        constraints.append(f[crashed_node]['src_sender'][crashed_node] == 0)

    # objective function
    # obj = cp.Minimize(LAMBDA * (1 - PI) @ t)
    obj = cp.Minimize(LAMBDA * (1 - PI) @ t + 1e-5 *
                      cp.sum(x) + 1e-5 * cp.sum(x_ram))

    # Form and solve problem.
    prob = cp.Problem(obj, constraints)

    prob.solve(solver=cp.HIGHS, canon_backend=cp.SCIPY_CANON_BACKEND)

    def plot_assignment(x_val, y_val):
        '''creates a heatmap image'''

        plt.figure(figsize=(16, 8))
        plt.subplot(2, 1, 1)
        sns.heatmap(x_val*100, cmap="Blues",
                    )
        plt.xlabel("Layer")
        plt.ylabel("Node")
        plt.title(
            f"Backup layer placement on Disk (disk={disk_size:02d}GiB)")

        plt.subplot(2, 1, 2)
        sns.heatmap(y_val*100, cmap="Oranges",
                    )
        plt.xlabel("Layer")
        plt.ylabel("Node")
        plt.title(
            f"Backup layer placement on RAM (ram={ram_size:02d}GiB)")

        plt.tight_layout()
        plt.savefig(
            f"results/{config.name}/images/heatmap_disk{disk_size:02d}GiB_ram{ram_size:02d}GiB.png", bbox_inches='tight')
        # plt.show()
        plt.close('all')

    def print_results():
        print(
            f"result(config={config.name}, disk={disk_size:03d}GiB, ram={ram_size:03d}GiB):", prob.status)
        # if prob.status == 'optimal':
        #     print(f"expected value of downtime per unit time[s]: {prob.value}")
        #     # print("proportion of layers for each node", x.value)
        #     print("downtime for each node's crash[s]", t.value)
        #     print(
        #         f"occupied disk space for each node[%]: {(x @ S).value/disk_size * 100 if disk_size != 0 else "N/A"}")
        #     print(
        #         f"occupied RAM space for each node[%]: {(x_ram @ S).value/ram_size * 100 if ram_size != 0 else "N/A"}")
        #     print(f"solve time[s]: {prob.solver_stats.solve_time}")

        #     plot_assignment(x.value, x_ram.value)

    print_results()

    return {
        "name": config.name,
        "n": config.n,
        "l": config.l,
        "layer_size": config.layer_size,
        "disk_size": disk_size,
        "ram_size": ram_size,
        "expected_time": prob.value,
        "downtime": t.value,
        "solve_time": prob.solver_stats.solve_time,
        "disk_usage": (x @ S).value,
        "ram_usage": (x_ram @ S).value,
    }


config_mid = Config(name="mid", n=4, l=36, layer_size=1.81, disk_sizes=[
                    i for i in range(0, 2**5 + 1)], ram_sizes=[0] + [2 ** i for i in range(0, 7 + 1)])
config_large = Config("large", n=8, l=61, layer_size=10.18, disk_sizes=[
    i for i in range(0, 2**7 + 1)], ram_sizes=[0] + [2 ** i for i in range(0, 7 + 1)])

configs = [config_mid, config_large]

rows = []

# try with different disk and ram sizes
for config in configs:
    for disk_size in config.disk_sizes:
        for ram_size in config.ram_sizes:
            rows.append(calc(disk_size, ram_size, config))

df = pd.DataFrame(rows)

dfs = []

for config in configs:
    df_config = df[df['name'] == config.name]
    df_config.to_csv(f'results/{config.name}/result.csv')
