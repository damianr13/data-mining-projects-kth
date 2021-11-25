from kafka import KafkaConsumer
from collections import defaultdict
import random

CONST_MAX_EDGE_COUNT = 10000 # number edges that is in memory

edgeSet = set() # useful to select edge at random
counters = defaultdict(int)


def sampleEdge(t):
    if t <= CONST_MAX_EDGE_COUNT: # t <= max memory
        return True # save edge

    # t > max memory
    # reservoir sampling
    coinflip = random.randint(1, t) # Max_memory / t chance of entrance
    if coinflip < CONST_MAX_EDGE_COUNT:
        removedPair = random.choices(list(edgeSet))[0] # select pair to remove
        edgeSet.remove(removedPair)  # remove pair at random

        updateCounters(-1, removedPair[0], removedPair[1])
        return True  # save edge

    return False


def buildNodeNeighbourhood(u):
    return set([pair[0] for pair in edgeSet if pair[1] == u]).union(set([pair[1] for pair in edgeSet if pair[0] == u]))


def updateCounters(step, u, v): # update triangle counters
    # take all the neighbors of u in S --> intersect with all neighbors of v in S (common neighbors)
    # stores all the nodes included in triangle with edge between u and v (refered to as c)
    nuv = buildNodeNeighbourhood(u).intersection(buildNodeNeighbourhood(v))

    for c in nuv:
        counters['global'] += step # triangles in whole graph
        counters[c] += step
        counters[u] += step
        counters[v] += step

    # remove all keys for which value is 0
    for k, v in dict(counters).items(): # make a copy of the counters dict to avoid iterating and deleting
        if v == 0:
            del counters[k]

    if len(nuv) > 0:
        print(f'New global triangle count: {counters["global"]}')

def main():
    random.seed(38)
    consumer = KafkaConsumer('DataStreamsAssignment', bootstrap_servers=['localhost:9092'])  # consumer for Kafka

    t = 0  # count of messages received
    for msg in consumer:

        pair = tuple(sorted([int(u) for u in msg.value.decode('utf-8').split('\t')]))
        if pair in edgeSet: # not do anything if edge already in memory
            continue

        t += 1
        if sampleEdge(t):
            edgeSet.add(pair)
            updateCounters(+1, pair[0], pair[1])


if __name__ == '__main__':
    main()
