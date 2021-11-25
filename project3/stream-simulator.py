from time import sleep
from kafka import KafkaProducer
import threading
import random


CONST_THREAD_COUNT = 4
CONST_TOPIC = 'DataStreamsAssignment'


class CancellationFlag:
	def __init__(self):
		self.isCancelled = False

	def cancel(self):
		self.isCancelled = True


def isVeryHelpful(edge, knownNodes):
	edgeNodes = [int(node) for node in edge.split('\t')]
	return edgeNodes[0] in knownNodes and edgeNodes[1] in knownNodes


def sendGraphsMessages(producer, graph, index, cancellationFlag):
	knownNodes = set()
	while not cancellationFlag.isCancelled:
		randomness = random.randint(0, 9)
		if randomness <= -1 and len(knownNodes) > 0: # operation are edges between 2 known nodes
			pair = random.choice(list(filter(lambda x: isVeryHelpful(x, knownNodes), graph))).strip()
		elif randomness <= -1 and len(knownNodes) > 0: # operations contain at least one known node
			targetNode = str(random.choice(list(knownNodes)))
			pair = next(filter(lambda x: x.startswith(targetNode + '\t') or x.endswith('\t' + targetNode), graph), None)
			if not pair: # if no pair starting with given node was found, move on
				continue

			pair = pair.strip()
		else: # operation are random insertions of edges and nodes
			edgeIndex = min(random.randrange(0, len(graph), CONST_THREAD_COUNT) + index, len(graph) - 1)
			pair = graph[edgeIndex].strip()

		knownNodes.update([int(node) for node in pair.split('\t')])
		producer.send(CONST_TOPIC, value=bytes(pair, 'utf-8'))

		sleepTime = random.randrange(5, 40) / 100
		sleep(sleepTime)


def main():
	random.seed(38) # our group number
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

	with open('data/CA-HepPh.txt') as file:
		lines = file.readlines()

		threads = []
		cancellationFlag = CancellationFlag()
		for index in range(CONST_THREAD_COUNT):
			x = threading.Thread(target=sendGraphsMessages, args=(producer, lines, index, cancellationFlag))
			threads.append(x)
			x.start()

		command = input('Type [stop] to finish sending the messages\n')
		while command != 'stop':
			command = input('Type [stop] to finish sending the messages\n')

		cancellationFlag.cancel()
		for thread in threads:
			thread.join()


if __name__ == '__main__':
	main()