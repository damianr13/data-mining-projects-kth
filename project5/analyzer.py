import os
import re
from pprint import pprint

annealingTypes=['TYPE1', 'TYPE2', 'TYPE4']
fileNames = ['3elt.graph', 'add20.graph', 'facebook.graph']

print('\n\n')
results = {}
for fileName in fileNames:
    for annealingType in annealingTypes:
        filePattern = f'.*{fileName}.*{annealingType}.*'
        regex = re.compile(filePattern)

        bestValue = -1
        fileForBestValue = ''
        for root, dirs, files in os.walk('output'):
            for file in files:
                if regex.match(file):
                    with open(f'output/{file}') as f:
                        current = min([int(re.sub('\\s+', ' ', x).split(' ')[1]) for x in f.readlines()[3:]])
                        if bestValue == -1 or bestValue > current:
                            bestValue = current
                            fileForBestValue = file

        results[(fileName, annealingType)] = (bestValue, fileForBestValue)

pprint(results)
print('\n\n')
