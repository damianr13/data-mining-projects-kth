from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, count, array_union, size, array_except, avg, monotonically_increasing_id
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


def aggregate_and_filter_groups(crossed):
	print(f'Checking for {crossed.count()} pairs. Aiming for {support_size} support!')
	return crossed.dropDuplicates(['index', 'group']).groupBy('group').agg(count('group').alias('occurrences'))\
			.filter(col('occurrences') >= support_size)\
			.select('group', 'occurrences')


def find_supported_groups_multiple_items(helper, group_size):
	crossed = helper.alias('a').join(helper.alias('b'), on='index', how='inner')\
		.withColumn('group', array_union('a.group_prev', 'b.group_1'))\
		.filter(size(col('group')) == group_size)\
		.select(col('a.index').alias('index'), 
			col('a.items').alias('items'), 
			col('a.group_1').alias('group_1'), 
			col('group'))

	aggregated = aggregate_and_filter_groups(crossed)
	crossed_useful = aggregated.alias('a').join(crossed, on='group', how='left')\
		.select(col('index'), col('items'), col('group_1').alias('group_1'), col('a.group').alias('group_prev'))

	return (aggregated, crossed_useful)


def find_supported_groups_one_item(baskets, groups):
	groups_count = groups.count()

	crossed = baskets.join(groups).filter(size(array_except(col('group'), col('items'))) == 0)
	aggregated = aggregate_and_filter_groups(crossed)

	crossed_useful = aggregated.alias('a').join(crossed, on='group', how='left')\
		.select(col('index'), col('items'), col('a.group').alias('group_1'), col('a.group').alias('group_prev'))

	return (aggregated, crossed_useful)


def gather_all_groups(helper, previous_groups, previous_group_size):
	(supported_groups, helper) = find_supported_groups_multiple_items(helper, previous_group_size + 1) 
	supported_group_count = supported_groups.count()
	print(f'Identified {supported_group_count} new supported groups!')
	if supported_group_count > 0:
		recursive_supported_groups = gather_all_groups(helper, supported_groups, previous_group_size + 1)
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
	baskets = baskets.withColumn('index', monotonically_increasing_id())
	baskets.show()

	last_item_id = find_last_item_id(baskets)
	print(f'Number of items: {last_item_id}')

	vectorize = udf(lambda x: [x], ArrayType(IntegerType()))
	items_df = spark.range(0, last_item_id + 1).toDF('id').withColumn('group', vectorize('id')).select('group')
	items_df.show()

	one_item_groups, helper = find_supported_groups_one_item(baskets, items_df)

	all_supported_groups = gather_all_groups(helper, one_item_groups, 1)
	all_supported_groups.show()
	all_supported_groups.withColumn('group_size', size(col('group'))).groupBy('group_size').agg(count(col('group_size'))).show()
	all_supported_groups.write.mode("overwrite").format("json").save(f'data/output-{support_size}')


if __name__ == "__main__":
	main()

"""
1. [all the possible items] check if the have support s
2. make combinations between the items in step 1
3. make combinations with items in step 2 and the items in step 1
3. make combinations with items in step 3 and the items in step 1
...
 until "combinations in step n" is an empty set




Idea: after we have the table with length one groups, we merge it with itself, so we don't associate every 
possible group with every basket, but we build on the (basket, group) relation already known
"""