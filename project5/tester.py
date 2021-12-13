import subprocess


params = [
	{
		"annealing": ["type1"],
		"temp": [1, 2, 3, 6, 10],
		"delta": [0.001, 0.01, 0.03, 0.1],
		"alpha": [0.5, 1, 2, 4]
	},
	{
		"annealing": ["type2"],
		"temp": [0.1, 0.5, 0.6, 0.8, 0.9, 1],
		"delta": [0.5, 0.7, 0.9, 0.99],
		"alpha": [0.5, 1, 2, 4]
	},
	{
		"annealing": ["type4"],
		"temp": [20, 10, 8, 5, 2],
		"delta": [1, 0.5, 0.1, 0.05],
		"alpha": [0.5, 1, 2, 4]
	}
]

files = ['3elt.graph', 'add20.graph', 'facebook.graph']

subprocess.run("pwd")
for f in files:
	for paramMap in params:
		keys = [k for k in paramMap]
		paramIndex = {k: 0 for k in paramMap}

		args = []
		finished = False
		while not finished:
			finished = True

			args.append(" ".join([f'-{k} {paramMap[k][paramIndex[k]]}' for k in paramMap]))

			for key in keys[::-1]:
				if paramIndex[key] == len(paramMap[key]) - 1:
					paramIndex[key] = 0
				else:
					finished = False
					paramIndex[key] += 1

		for currentArgs in args:
			subprocess.run(['./run.sh', f'-graph graphs/{f} {currentArgs}'])
