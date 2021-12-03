import csv 
import numpy as np
from scipy.linalg import fractional_matrix_power, eigh
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

edges = []

with open('data/example2.dat') as graphFile:
	csvReader = csv.reader(graphFile)

	for row in csvReader:
		edges.append((int(row[0]), int(row[1])))


nodeCount = max(max(node1, node2) for node1, node2 in edges)

A = np.zeros((nodeCount, nodeCount))
for node1, node2 in edges:
	A[node1-1][node2-1] = 1
	A[node2-1][node1-1] = 1


D = np.diag(np.sum(A, axis=0))
L = fractional_matrix_power(D, -0.5) @ A @ fractional_matrix_power(D, -0.5)

v, X = eigh(L)
X = np.fliplr(X)
v = np.flip(v)

gaps = np.absolute(np.diff(v))
print(gaps)
k = np.argmax(gaps) + 1
print(f'Most suitable k: {k}')
print(v)

v = v[:k]
X = X[:, :k]

Y = X / np.sum(X ** 2) * 0.5
kmeans = KMeans(n_clusters=k, random_state=38).fit(Y)
print(kmeans.labels_)
print(len(kmeans.labels_))

rowIndexes = np.argsort(kmeans.labels_)
print(rowIndexes)

C = A.copy()

for i in range(0, C.shape[0]):
	for j in range(0, C.shape[1]):
		if C[i][j] > 0:
			C[i][j] = kmeans.labels_[i] + 1


C2 = C.copy()
for i in range(0, C.shape[0]):
	for j in range(0, C.shape[1]):
		C[i][j] = C2[rowIndexes[i]][rowIndexes[j]]


colors = ["white", "darkorange", "red", "lawngreen", "blue"]
cmap1 = LinearSegmentedColormap.from_list("mycmap", colors)

plt.imshow(C, cmap=cmap1)
plt.show()
