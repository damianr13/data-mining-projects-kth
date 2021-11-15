from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, count, array_union, size, array_except, avg
from pyspark.sql.types import IntegerType, BooleanType, ArrayType

import argparse
import numpy as np


support_size = 2000


def parse_arguments(): 
	global support_size
	parser = argparse.ArgumentParser()
	parser.add_argument('--support-size', help="Set the minimum support size")
	args = parser.parse_args()
	if args.support_size:
		support_size = int(args.support_size)


def find_last_item_id(baskets):
	find_max = udf(lambda x: int(np.max(np.array(x, dtype=np.int32))), IntegerType())
	return baskets.select(find_max('items').alias('last_item')).agg({'last_item': 'max'}).collect()[0][0]


def create_new_groups(one_item_groups, previous_groups, previous_group_size):
	return one_item_groups.alias('one').join(previous_groups.alias('prev'))\
		.withColumn('group_new', array_union('prev.group', 'one.group'))\
		.selectExpr('group_new as group')\
		.filter(size(col('group')) == previous_group_size + 1)


def find_supported_groups(baskets, groups):
	groups_count = groups.count()
	print(f'Checking for {groups_count} groups. Aiming for {support_size} support!')

	crossed = baskets.join(groups).filter(size(array_except(col('group'), col('items'))) == 0)
	return crossed.groupBy('group').agg(count('group').alias('occurrences'))\
		.filter(col('occurrences') >= support_size)\
		.select('group', 'occurrences')


def gather_all_groups(baskets, one_item_groups, previous_groups, previous_group_size):
	next_groups = create_new_groups(one_item_groups, previous_groups, previous_group_size)

	supported_groups = find_supported_groups(baskets, next_groups) 
	supported_group_count = supported_groups.count()
	print(f'Identified {supported_group_count} new supported groups!')
	if supported_group_count > 0:
		recursive_supported_groups = gather_all_groups(one_item_groups, supported_groups, previous_group_size + 1)
		return previous_groups.union(recursive_supported_groups)

	return previous_groups


def main():
	parse_arguments()
	spark_context = SparkContext("local[*]", "Project 2 - Frequent items")
	spark = SparkSession.builder.getOrCreate() # initializes sql related stuff
	spark_context.setLogLevel('ERROR')

	baskets = spark.read.text('data/input.dat')

	to_numeric = udf(lambda x: [int(i) for i in x if i.isnumeric()], ArrayType(IntegerType()))
	baskets = baskets.select(split(col('value'), ' ').alias('items')).select(to_numeric(col('items')).alias('items'))
	baskets.show()

	last_item_id = find_last_item_id(baskets)
	print(f'Number of items: {last_item_id}')

	vectorize = udf(lambda x: [x], ArrayType(IntegerType()))
	items_df = spark.range(0, last_item_id + 1).toDF('id').withColumn('group', vectorize('id')).select('group')
	items_df.show()

	one_item_groups = find_supported_groups(baskets, items_df)

	all_supported_groups = gather_all_groups(baskets, one_item_groups, one_item_groups, 1)
	all_supported_groups.show()


if __name__ == "__main__":
	main()

"""
1. [all the possible items] check if the have support s
2. make combinations between the items in step 1
3. make combinations with items in step 2 and the items in step 1
3. make combinations with items in step 3 and the items in step 1
...
 until "combinations in step n" is an empty set
"""