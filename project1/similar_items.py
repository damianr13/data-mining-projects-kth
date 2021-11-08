from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, col, arrays_overlap
from pyspark.sql.types import ArrayType, StringType, DoubleType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import HashingTF, MinHashLSH

import argparse
import re
import random

# defaults, can be set by command line arguments
shingle_size = 10
signature_reducing_factor = 100


@udf(returnType=ArrayType(StringType()))
def shingle_single_document(text):
    curated_text = re.sub('\s+', ' ', text.strip()) # strip each document and remove extra blanks

    shingle_list = [curated_text[i:i+shingle_size] for i in range(0, len(curated_text) - shingle_size)]
    return list(set(shingle_list))

def shingle(documents):
    return documents.withColumn('shingles', shingle_single_document("_2")) # adds a new column "shingles" to the df with shingles from each document

def parse_arguments(): # set arguments in command line (shingle size and signature reducting factor)
    global shingle_size, signature_reducing_factor
    parser = argparse.ArgumentParser()
    parser.add_argument('--shingle-size', help="Set the size of one shingle")
    parser.add_argument('--signature-reducing-factor', help="Set the number by which the shingle count is divided to obtain signature size")
    args = parser.parse_args()
    if args.shingle_size:
        shingle_size = int(args.shingle_size)
    if args.signature_reducing_factor:
        signature_reducing_factor = int(args.signature_reducing_factor)


@udf(returnType=DoubleType()) #jaccard similarity
def compare_sets(shingles1, shingles2):
    shingles_set1 = set(shingles1)
    shingles_set2 = set(shingles2)

    return len(shingles_set1 & shingles_set2) / len(shingles_set1 | shingles_set2)


def shingles_to_signatures(documents):
    shingle_count = documents.select(explode(documents.shingles)).distinct().count() # shingle array --> rows (nice for counting shingles)

    numHashTables = int(shingle_count / signature_reducing_factor) + 10
    return Pipeline(stages=[
      HashingTF(inputCol='shingles', outputCol='hashes'),
      # TODO: Implement MinHash from scratch based on course literature?
      MinHashLSH(inputCol='hashes', outputCol='signature', numHashTables=numHashTables)
     ]).fit(documents).transform(documents)


def path_to_name(file_name):
    return file_name.split('/')[-1]

@udf(returnType=ArrayType(StringType()))
def paths_to_pair(file_name_1, file_name_2):
    if file_name_1 == file_name_2:
        return None
    return sorted([path_to_name(file_name_1), path_to_name(file_name_2)])


def cross_compare(documents, column_name):
    documents_comparison = documents.selectExpr('_1 as name_1', column_name + ' as ' + column_name + '_1')\
     .join(documents.selectExpr('_1 as name_2', column_name + ' as ' + column_name + '_2')) #selectExpr allows for sql expression, selecting column 1 and shingle twice to do cross comparison

    # first the columns name_1 and name_2 are sorted with paths_to_pair so that we can drop duplicates
    # compare_sets returns jaccard similarity
    documents_comparison = documents_comparison.withColumn('pair', paths_to_pair('name_1', 'name_2'))\
     .select('pair', column_name + '_1', column_name + '_2')\
     .where(col('pair').isNotNull())\
     .dropDuplicates(['pair'])\
     .withColumn('similarity', compare_sets(column_name + '_1', column_name + '_2'))\
     .sort(col('similarity').desc())

    return documents_comparison.select('pair', 'similarity')


def generate_hash_parameters(documents):
    random.seed(22223016)

    shingle_count = documents.select(explode(documents.shingles)).distinct().count()  # shingle array --> rows (nice for counting shingles)
    print(shingle_count)
    numHashTables = int(shingle_count / signature_reducing_factor) + 10

    numbersa = random.sample(range(10 ** 6, 10 ** 7), numHashTables)
    numbersb = random.sample(range(10 ** 5, 10 ** 8), numHashTables)
    return [[numbersa[i], numbersb[i]] for i in range(numHashTables)]


def extract_signature(hashes, minhash_functions):
    p = minhash_functions[0]
    return [min([((p[0] * x + p[1]) % 653563) % 262144 for x in hashes.indices.tolist()]) for p in minhash_functions]


def shingles_to_signature_from_scratch(documents):
    documents_hashed = HashingTF(inputCol='shingles', outputCol='hashes').transform(documents)
    minhash_params = generate_hash_parameters(documents)

    signature_udf = udf(lambda hashes: extract_signature(hashes, minhash_params), returnType=ArrayType(IntegerType()))

    return documents_hashed.withColumn('signature', signature_udf('hashes'))


def compute_lsh_single_document(signature_list, r):
    return [hash(tuple(signature_list[i: i + r])) for i in range(0, len(signature_list), r)]


def compute_lsh_buckets_with_band_size(documents, r):
    lsh_udf = udf(lambda hashes: compute_lsh_single_document(hashes, r), returnType=ArrayType(IntegerType()))
    documents_lsh = documents.withColumn('lsh', lsh_udf('signature'))

    return documents_lsh.alias('a').join(documents_lsh.alias('b'), arrays_overlap("a.lsh", "b.lsh"))\
     .withColumn('pair', paths_to_pair('a._1', 'b._1'))\
     .where(col('pair').isNotNull())\
     .dropDuplicates('pair')\
     .withColumn('similarity', compare_sets('a.signature', 'b.signature'))\
     .select('pair', 'similarity')


def compute_lsh_buckets(documents, t):
    r = 200

    return compute_lsh_buckets_with_band_size(documents, r)


def main():
    parse_arguments()  # set arguments in command line
    spark_context = SparkContext("local", "Project 1 - Similar items")
    spark = SparkSession.builder.getOrCreate() # initializes sql related stuff

    documents_rdd = spark_context.wholeTextFiles("documents/*.txt")  # reads the directory of text files (documents)
    documents = documents_rdd.toDF()  # converts documents_rdd to dataframe

    documents_shingled = shingle(documents) # adds shingle column to the dataframe
    documents_shingled.show()

    documents_comparison = cross_compare(documents_shingled, 'shingles') # creates new dataframe for jaccard similarities between documents
    documents_comparison.show(documents_comparison.count(), False) #.count() to show all rows and not truncate df

    documents_hashed = shingles_to_signatures(documents_shingled) # signatures using built in function
    documents_hashed.show()
    documents_comparison = cross_compare(documents_hashed, 'signature')
    documents_comparison.show(documents_comparison.count(), False)

    documents_hashed_scratch = shingles_to_signature_from_scratch(documents_shingled) # signatures from scratch
    documents_hashed_scratch.show()
    documents_comparison = cross_compare(documents_hashed_scratch, 'signature')
    documents_comparison.show(documents_comparison.count(), False)

    documents_lsh_filtered = compute_lsh_buckets_with_bucket_size(documents_hashed_scratch, 10)
    documents_lsh_filtered.show(documents_lsh_filtered.count(), False)


if __name__ == "__main__":
    main()
