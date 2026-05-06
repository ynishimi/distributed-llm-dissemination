import cvxpy as cp

# problem data

N = 4  # num of nodes
L = 36  # num of layers
P = [0.5 for _ in range(N)]  # probability of crash
S = [1e9 for _ in range(L)]  # size of layers (= |l|)
C = [1e12 for _ in range(N)]  # size of disks
B = [1e9 for _ in range(N)]  # network bandwidth of nodes
D = [1e6 for _ in range(N)]  # rate of disks


# variables

x = cp.Variable((N, L))  # a proportion of the layer l stored to node k
t = cp.Variable(N)  # downtime for each node's crash scenario
f = {}  # edges in the flow graph
for k in range(N):  # for each crash scenario:
    f[k] = {}
    f[k]['src_sender'] = cp.Variable(N)
    f[k]['sender_disk'] = cp.Variable(N)
    # f[k]['disk_layer'] = cp.Variable(N, L)
    f[k]['layer_receiver'] = cp.Variable(L)
    f[k]['receiver_sink'] = cp.Variable()

# constraints

constraints = []

for k in range(N):
    constraints.append(x @ S <= C)  # disk capacity constraint
    constraints.append(f[k]['src_sender'] <= cp.multiply(B, t[k]))
    constraints.append(f[k]['sender_disk'] <= cp.multiply(D, t[k]))
    # constraints.append(f[k]['disk_layer'])
    constraints.append(f[k]['layer_receiver'] == S)
    constraints.append(f[k]['receiver_sink'] <= B[k] * t[k])


# objective function
obj = cp.Minimize(P @ t)

# Form and solve problem.
prob = cp.Problem(obj, constraints)
prob.solve(solver=cp.HIGHS)  # Returns the optimal value.
print("status:", prob.status)
print("optimal value", prob.value)
print("optimal var", x.value, t.value)
