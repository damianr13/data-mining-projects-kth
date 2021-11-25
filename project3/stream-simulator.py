from time import sleep
from kafka import KafkaProducer
import threading
import random

CONST_THREAD_COUNT = 4
CONST_TOPIC = 'DataStreamsAssignment'

# stop execution or not
class CancellationFlag:
	def __init__(self):
		self.isCancelled = False

	def cancel(self):
		self.isCancelled = True

# producer: kafka obj
# graph: lines
# index: index of thread (every thread will take lines assigned to it...)
def sendGraphsMessages(producer, graph, index, cancellationFlag):
    while not cancellationFlag.isCancelled: # runs when flag is false

        edgeIndex = min(random.randrange(0, len(graph), CONST_THREAD_COUNT) + index, len(graph) - 1)
        pair = graph[edgeIndex].strip() # remove \n
        producer.send(CONST_TOPIC, value=bytes(pair, 'utf-8')) # send to Kafka port

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
