from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf, count, array_union, size, array_except, avg, monotonically_increasing_id, explode, array_sort
from pyspark.sql.types import IntegerType, BooleanType, ArrayType

import argparse
import numpy as np
import os
from itertools import permutations

# defaults, can be set by command line arguments
support_size = 1000
cache_enabled = True
confidence_threshold = 0.6

def parse_arguments():
    global support_size, cache_enabled, confidence_threshold
    parser = argparse.ArgumentParser()
    parser.add_argument('--support-size', help="Set the minimum support size")
    parser.add_argument('--disable-cache', help="Do not use cached data", action='store_true')
    parser.add_argument('--confidence-threshold', help="Set the confidence_threshold")
    args = parser.parse_args()
    if args.support_size:
        support_size = int(args.support_size)
    if args.disable_cache:
        cache_enabled = False
    if args.confidence_threshold:
        confidence_threshold = float(args.confidence_threshold)


"""
*****************************************************
**************** Cash disabled **********************
*****************************************************
"""

def perform_operation_no_cache(spark):
    baskets = spark.read.text('data/input.dat')  # read data-set of sales transactions

    to_numeric = udf(lambda x: [int(i) for i in x if i.isnumeric()],
                     ArrayType(IntegerType()))  # for each basket, convert text to array of integers
    baskets = baskets.select(split(col('value'), ' ').alias('items')).select(to_numeric(col('items')).alias('items')) # create Spark DataFrame with column "item": each row is a basket
    baskets = baskets.withColumn('index', monotonically_increasing_id())  # each basket has an associated index (useful in aggregate_and_filter_groups())
    baskets.show()

    last_item_id = find_last_item_id(baskets)
    print(f'Number of items: {last_item_id}') # = 999

    vectorize = udf(lambda x: [x], ArrayType(IntegerType()))
    items_df = spark.range(0, last_item_id + 1).toDF('id').withColumn(
        'group', vectorize('id')).select('group') # each item gets an associated group. The group is stored in a new Spark DataFrame.
    items_df.show()

    one_item_groups, helper = find_supported_groups_one_item(baskets, items_df) # finds frequent singletons

    all_supported_groups = gather_all_groups(helper, one_item_groups, 1) # finds frequent doubletons, tripletons etc...
    all_supported_groups.show()
    all_supported_groups.withColumn(
        'group_size', size(col('group'))).groupBy('group_size').agg(
            count(col('group_size'))).show()
    all_supported_groups.write.mode("overwrite").format("json").save(
        f'data/output-{support_size}')

    compute_rules_with_confidence(all_supported_groups)


"""
*****************************************************
**************** Cache enabled **********************
*****************************************************
"""


def perform_operation_cache(spark):
    if not os.path.isdir(f'data/output-{support_size}'):
        return perform_operation_no_cache(spark)

    results_df = spark.read.json(f'data/output-{support_size}/*.json')
    results_df.show()

    compute_rules_with_confidence(results_df)


"""
*****************************************************
**************** Help functions for Task (1) ********
*****************************************************
"""

def find_last_item_id(baskets): # baskets are sorted --> finds last item for each basket --> return max which corresponds to the number of items
    find_max = udf(lambda x: int(np.max(np.array(x, dtype=np.int32))), IntegerType())
    return baskets.select(find_max('items').alias('last_item')).agg({'last_item': 'max'}).collect()[0][0]


def aggregate_and_filter_groups(crossed):
    print(f'Checking for {crossed.count()} pairs. Aiming for {support_size} support!')
    return crossed.dropDuplicates(['index', 'group']).groupBy('group').agg(count('group').alias('occurrences'))\
      .filter(col('occurrences') >= support_size)\
      .select('group', 'occurrences')

def find_supported_groups_one_item(baskets, groups):
    # for each basket and each group: if item in basket it is removed from the group --> then, if group size == 0, the basket is kept!
    crossed = baskets.join(groups).filter(size(array_except(col('group'), col('items'))) == 0)
    aggregated = aggregate_and_filter_groups(crossed) # counts occurences for each group with support size s ("item 2 is included in X baskets"")

    # help DataFrame which is used later (DataFrame with basket and group but only for frequent groups with support size s)
    crossed_useful = aggregated.alias('a').join(crossed, on='group', how='left')\
    .select(col('index'), col('items'), col('a.group').alias('group_1'), col('a.group').alias('group_prev'))

    return (aggregated, crossed_useful)


def find_supported_groups_multiple_items(helper, group_size):
    # keep group only if it has the right group_size
    crossed = helper.alias('a').join(helper.alias('b'), on='index', how='inner')\
     .withColumn('group', array_sort(array_union('a.group_prev', 'b.group_1')))\
     .dropDuplicates()\
     .filter(size(col('group')) == group_size)\
     .select(col('a.index').alias('index'),
      col('a.items').alias('items'),
      col('a.group_1').alias('group_1'),
      col('group'))

    aggregated = aggregate_and_filter_groups(crossed)  # counts occurences for each group with support size s
    # help DataFrame which is in each iteration
    crossed_useful = aggregated.alias('a').join(crossed, on='group', how='left')\
     .select(col('index'), col('items'), col('group_1').alias('group_1'), col('a.group').alias('group_prev'))

    return (aggregated, crossed_useful)

def gather_all_groups(helper, previous_groups, previous_group_size):
    (supported_groups, helper) = find_supported_groups_multiple_items(helper, previous_group_size + 1)
    supported_group_count = supported_groups.count()
    print(f'Identified {supported_group_count} new supported groups!')
    if supported_group_count > 0: # finds frequent tripletons, etc (recursive call)
        recursive_supported_groups = gather_all_groups(helper, supported_groups, previous_group_size + 1)
        return previous_groups.union(recursive_supported_groups)

    return previous_groups


"""
*****************************************************
********** BONUS TASK (2): Association rules ********
*****************************************************
"""

@udf(returnType=ArrayType(ArrayType(ArrayType(IntegerType()))))
def find_permutations_with_split(group):
    perms = permutations(group)
    return [[x[0:j], x[j:len(x)]] for x in perms for j in range(1, len(x))]


def compute_rules_with_confidence(supports):
    # finds all rules (singletons do not require rules): group, premise, conclusion
    rules = supports.filter(size(col('group')) >= 2)\
     .withColumn('splitted_permutations', find_permutations_with_split(col('group')))\
     .withColumn('perm', explode(col('splitted_permutations')))\
     .withColumn('premise', array_sort(col('perm').getItem(0)))\
     .withColumn('conclusion', array_sort(col('perm').getItem(1)))\
     .select('group', 'premise', 'conclusion')\
     .dropDuplicates()

    rules = rules.alias('a').join(supports.alias('b'), on='group', how='left')\
     .select('group', 'premise', 'conclusion', col('occurrences').alias('group_support'))

    rules = rules.alias('a').join(supports.alias('b'), col('a.premise') == col('b.group'), how='left')\
     .select(col('a.group').alias('group'), 'premise', 'conclusion', 'group_support', col('occurrences').alias('premise_support'))

    # computes confidence
    rules = rules.withColumn('confidence', col('group_support') / col('premise_support'))
    rules.filter(col('confidence') >= confidence_threshold)\
     .sort(col('confidence').desc())\
     .show()


def main():
    parse_arguments()
    spark_context = SparkContext("local[*]", "Project 2 - Frequent items")
    spark = SparkSession.builder.getOrCreate() # initializes sql related stuff
    spark_context.setLogLevel('ERROR')

    if cache_enabled:
        perform_operation_cache(spark)
    else:
        perform_operation_no_cache(spark)

if __name__ == "__main__":
    main()
