from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, udf
from pyspark.sql.types import IntegerType

import numpy as np


def find_last_item_id(baskets):
	find_max = udf(lambda x: int(np.max(np.array(x, dtype=np.int32))), IntegerType())
	return baskets.select(find_max('items').alias('last_item')).agg({'last_item': 'max'}).collect()[0][0]


def main():
    spark_context = SparkContext("local", "Project 2 - Frequent items")
    spark = SparkSession.builder.getOrCreate() # initializes sql related stuff

    baskets = spark.read.text('data/input.dat')

    to_numeric = udf(lambda x: [i for i in x if i.isnumeric()])
    baskets = baskets.select(split(col('value'), ' ').alias('items')).select(to_numeric(col('items')).alias('items'))

    baskets.show()

    last_item_id = find_last_item_id(baskets)
    print(f'Number of items: {last_item_id}')

    items_df = spark.range(0, last_item_id + 1).toDF('id')
    items_df.show()


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