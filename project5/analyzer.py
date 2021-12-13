import os
import re
from pprint import pprint
import seaborn as sns
import matplotlib.pyplot as plt

annealingTypes=['TYPE1', 'TYPE2', 'TYPE4']
fileNames = ['3elt.graph', 'add20.graph', 'facebook.graph']

print('\n\n')
swaps = {'3elt.graph': {}, 'add20.graph': {}, 'facebook.graph': {}}
file_params = {'3elt.graph': {}, 'add20.graph': {}, 'facebook.graph': {}}
edgecut = {'3elt.graph': {}, 'add20.graph': {}, 'facebook.graph': {}}

results = {}
for fileName in fileNames:
    for annealingType in annealingTypes:

        if not annealingType in swaps[fileName]:
            swaps[fileName][annealingType] = {}
            edgecut[fileName][annealingType] = {}

        filePattern = f'.*{fileName}.*{annealingType}.*'
        regex = re.compile(filePattern)

        bestValue = -1
        fileForBestValue = ''
        for root, dirs, files in os.walk('output'):
            for file in files:
                if regex.match(file):
                    with open(f'output/{file}') as f:
                        file_D = re.findall("D_\d+\.?\d*", file)[0]
                        # print(file_D)

                        if not file_D in swaps[fileName][annealingType]:
                            swaps[fileName][annealingType][file_D] = []
                            edgecut[fileName][annealingType][file_D] = []
                        # print('yes')
                        file_lines = f.readlines()[3:]

                        current = min([int(re.sub('\\s+', ' ', x).split(' ')[1]) for x in file_lines])

                        swaps[fileName][annealingType][file_D].extend([
                            int(re.sub('\\s+', ' ', y).split(' ')[2])
                            for y in file_lines
                        ])
                        edgecut[fileName][annealingType][file_D].extend([
                            int(re.sub('\\s+', ' ', y).split(' ')[1])
                            for y in file_lines
                        ])

                        if bestValue == -1 or bestValue > current:
                            bestValue = current
                            fileForBestValue = file

        results[(fileName, annealingType)] = (bestValue, fileForBestValue)

print('\n\n')
pprint(results)
print('\n\n')

edgecutForFile = edgecut['3elt.graph']['TYPE1']
pprint([(key, len(edgecutForFile[key])) for key in edgecutForFile])
