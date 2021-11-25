from kafka import KafkaConsumer
from collections import defaultdict
import random

CONST_MAX_EDGE_COUNT = 10000

edgeSet = set()
counters = defaultdict(int)


def sampleEdge(rawPair, t):
	if t <= CONST_MAX_EDGE_COUNT:
		return True

	coinflip = random.randint(1, t)
	if coinflip < CONST_MAX_EDGE_COUNT:
		removedPair = random.choices(list(edgeSet))[0]
		edgeSet.remove(removedPair)

		updateCounters(-1, removedPair[0], removedPair[1])
		return True

	return False  


def main():
	random.seed(38)
	consumer = KafkaConsumer('DataStreamsAssignment', bootstrap_servers=['localhost:9092'])

	t = 0
	for msg in consumer:
		t += 1
		pair = tuple(sorted([int(u) for u in msg.value.decode('utf-8').split('\t')]))
		if pair in edgeSet:
			continue

		if sampleEdge(msg, t):
			edgeSet.add(pair)
			updateCounters(+1, pair[0], pair[1])


def buildNodeNeighbourhood(u):
	return set([pair[0] for pair in edgeSet if pair[1] == u]).union(set([pair[1] for pair in edgeSet if pair[0] == u]))


def updateCounters(step, u, v):	
	nuv = buildNodeNeighbourhood(u).intersection(buildNodeNeighbourhood(v))

	for c in nuv:
		counters['global'] += step
		counters[c] += step
		counters[u] += step
		counters[v] += step

	for k, v in dict(counters).items(): # make a copy of the counters dict to avoid iterating and deleting
		if v == 0:
			del counters[k]

	if len(nuv) > 0:
		print(f'New global triangle count: {counters["global"]}')


if __name__ == '__main__':
	main()