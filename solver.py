import seaborn as sns
import matplotlib.pyplot as plt
import cvxpy as cp
import numpy as np

sns.set_theme()
sns.set_context("paper")


def calc(disk_size):

    # problem data

    LAYERSIZE = 1.81 * 2**30  # 1.81 GiB
    # DISKSIZE = 20 * 2**30  # 20 GiB
    NETWORKBW = 12.5/8 * 10**9  # 12.5 Gbps
    DISKBW = 200 * 2**20  # 200 MiB

    N = 4
    '''num of nodes'''
    L = 36
    '''num of layers'''
    P = np.array([1/N for _ in range(N)])
    '''probability of crash'''
    S = np.array([LAYERSIZE for _ in range(L)])
    '''size of layers (= |l|)'''
    C = np.array([disk_size for _ in range(N)]
                 )
    '''size of disks'''
    B = np.array([NETWORKBW for _ in range(N)])
    '''network bandwidth of nodes'''
    D = np.array([DISKBW for _ in range(N)])
    '''rate of disks'''

    INIT_ASSIGNMENT = [[L//N * i + j for j in range(L//N)]
                       for i in range(N)]
    DUMMY_ASSIGNMENT = [[L//(N-1) * i + j for j in range(L//(N-1))]
                        for i in range(N-1)]
    ALT_ASSIGNMENTS = [DUMMY_ASSIGNMENT[:i] + [[]] +
                       DUMMY_ASSIGNMENT[i:] for i in range(N)]

    # variables

    # a proportion of the layer l stored to node k
    x = cp.Variable((N, L), nonneg=True)
    t = cp.Variable(N, nonneg=True)  # downtime for each node's crash scenario
    f = {}  # edges in the flow graph
    for crashed_node in range(N):  # for each crash scenario:
        f[crashed_node] = {}
        f[crashed_node]['src_sender'] = cp.Variable(N, nonneg=True)
        f[crashed_node]['sender_disk'] = cp.Variable(
            N, nonneg=True)  # TODO: add a client?
        f[crashed_node]['disk_layer'] = cp.Variable((N, L), nonneg=True)
        f[crashed_node]['layer_receiver'] = cp.Variable(L, nonneg=True)
        f[crashed_node]['receiver_sink'] = cp.Variable(N, nonneg=True)

    # constraints

    constraints = []
    constraints.append(x <= 1)
    constraints.append(x @ S <= C)  # disk capacity constraint

    for crashed_node in range(N):
        # network bw
        constraints.append(f[crashed_node]['src_sender'] <=
                           cp.multiply(B, t[crashed_node]))
        # flow
        constraints.append(f[crashed_node]['src_sender'] ==
                           f[crashed_node]['sender_disk'])
        # disk bw
        constraints.append(f[crashed_node]['sender_disk'] <=
                           cp.multiply(D, t[crashed_node]))
        # flow
        constraints.append(f[crashed_node]['sender_disk'] ==
                           cp.sum(f[crashed_node]['disk_layer'], axis=1))
        # data obtained by each disk
        constraints.append(f[crashed_node]['disk_layer'] <= cp.multiply(x, S))
        # flow
        for node_asgn in range(len(ALT_ASSIGNMENTS[crashed_node])):
            if len(ALT_ASSIGNMENTS[crashed_node][node_asgn]) == 0:
                continue
            constraints.append(cp.sum(f[crashed_node]['disk_layer'][:, ALT_ASSIGNMENTS[crashed_node][node_asgn]],
                                      axis=0) == f[crashed_node]['layer_receiver'][ALT_ASSIGNMENTS[crashed_node][node_asgn]])

        # missing layers should be sent, but existing layers should not be sent or saved
        for receiver in range(N):
            for l in ALT_ASSIGNMENTS[crashed_node][receiver]:
                if l in INIT_ASSIGNMENT[receiver]:
                    constraints.append(
                        f[crashed_node]['layer_receiver'][l] == 0)
                    constraints.append(x[receiver, l] == 0)
                else:
                    constraints.append(
                        f[crashed_node]['layer_receiver'][l] == S[l])

        # flow
        constraints.append(
            cp.sum(f[crashed_node]['layer_receiver']) == f[crashed_node]['receiver_sink'])
        # network bw
        constraints.append(f[crashed_node]['receiver_sink']
                           <= cp.multiply(B, t[crashed_node]))
        # network bandwidth should be shared among the same node (sender k and receiver k)
        constraints.append(f[crashed_node]['src_sender'] + f[crashed_node]
                           ['receiver_sink'] <= cp.multiply(B, t[crashed_node]))
        # crashed node doesn't send layers
        constraints.append(f[crashed_node]['src_sender'][crashed_node] == 0)

    # objective function
    obj = cp.Minimize(P @ t)

    # Form and solve problem.
    prob = cp.Problem(obj, constraints)

    prob.solve(solver=cp.HIGHS, canon_backend=cp.SCIPY_CANON_BACKEND)

    print("status:", prob.status)
    print("expected value of downtime[s]", prob.value)
    print("proportion of layers for each node", x.value)
    print("downtime for each node's crash[s]", t.value)
    print("occupied disk space for each node", (x @ S).value)

    def plot_assignment(x_val):
        '''creates a heatmap image'''

        plt.figure(figsize=(16, 4))
        sns.heatmap(x_val*100, cmap="Blues",
                    )
        plt.xlabel("Layer")
        plt.ylabel("Node")
        plt.title(f"Backup layer placement(disk={disk_size/(2**30)}GiB)")
        plt.savefig(f"heatmap_{disk_size/(2**30)}GiB.png", bbox_inches='tight')
        # plt.show()
        plt.close('all')

    plot_assignment(x.value)


# try with different disk size
for i in range(1, 2):
    calc(20 * i * 2**30)
