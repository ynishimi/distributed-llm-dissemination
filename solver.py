import cvxpy as cp
import numpy as np

# problem data

N = 4
'''num of nodes'''
L = 36
'''num of layers'''
P = np.array([0.5 for _ in range(N)])
'''probability of crash'''
S = np.array([1e9 for _ in range(L)])
'''size of layers (= |l|)'''
C = np.array([1e9 * L / (N - 1) for _ in range(N)]
             )  # this time, setting the capacity to minimum.
'''size of disks'''
B = np.array([1e9 for _ in range(N)])
'''network bandwidth of nodes'''

D = np.array([1e6 for _ in range(N)])
'''rate of disks'''


# variables

# a proportion of the layer l stored to node k
x = cp.Variable((N, L), nonneg=True)
t = cp.Variable(N, nonneg=True)  # downtime for each node's crash scenario
f = {}  # edges in the flow graph
for k in range(N):  # for each crash scenario:
    f[k] = {}
    f[k]['src_sender'] = cp.Variable(N, nonneg=True)
    f[k]['sender_disk'] = cp.Variable(N, nonneg=True)
    f[k]['disk_layer'] = cp.Variable((N, L), nonneg=True)
    f[k]['layer_receiver'] = cp.Variable(L, nonneg=True)
    f[k]['receiver_sink'] = cp.Variable(nonneg=True)

# constraints

constraints = []

constraints.append(x <= 1)
constraints.append(x @ S <= C)  # disk capacity constraint

for k in range(N):
    constraints.append(f[k]['src_sender'] <= cp.multiply(B, t[k]))
    constraints.append(f[k]['src_sender'] == f[k]['sender_disk'])
    constraints.append(f[k]['sender_disk'] <= cp.multiply(D, t[k]))
    constraints.append(f[k]['sender_disk'] ==
                       cp.sum(f[k]['disk_layer'], axis=1))
    constraints.append(f[k]['disk_layer'] <= cp.multiply(x, S))

    # TODO: refer to a newer layer assignment
    constraints.append(
        cp.sum(f[k]['disk_layer'], axis=0) == f[k]['layer_receiver'])

    constraints.append(f[k]['layer_receiver'] == S)
    constraints.append(cp.sum(f[k]['layer_receiver']) == f[k]['receiver_sink'])
    constraints.append(f[k]['receiver_sink'] <= B[k] * t[k])

    # crashed node doesn't send layers
    constraints.append(f[k]['src_sender'][k] == 0)

# objective function
obj = cp.Minimize(P @ t)

# Form and solve problem.
prob = cp.Problem(obj, constraints)
prob.solve(solver=cp.HIGHS)  # Returns the optimal value.
print("status:", prob.status)
print("optimal value", prob.value)
print("optimal var", x.value, t.value)
