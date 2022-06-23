#!/usr/bin/env python3

"""
Assignment 5

This script takes as input an InterPROscan output file;
This file contains ~4,200,000 protein annotations.
With the PySpark dataframe functions the following questions are answered:

    1. How many distinct protein annotations are found in the dataset?
       I.e. how many distinct InterPRO numbers are there?
    2. How many annotations does a protein have on average?
    3. What is the most common GO Term found?\
    4. What is the average size of an InterPRO feature found in the dataset?
    5. What is the top 10 most common InterPRO features?
    6. If you select InterPRO features that are almost the same size (within 90-100%)
       as the protein itself, what is the top10 then?
    7. If you look at those features which also have textual annotation,
       what is the top 10 most common word found in that annotation?
    8. And the top 10 least common?
    9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the
       largest InterPRO features?
    10. What is the coefficient of correlation ($R^2$) between the size of the protein and
        the number of features found?
"""

__author__ = "Pascal Visser"
__version__ = 1.3

import csv
import sys

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def get_result(file):
    """get answers for the questions"""
    question_results = []
    spark = SparkSession.builder.master('local[16]').appName('sparkdf').getOrCreate()
    dataframe = spark.read.csv(file, sep=r'\t', header=False, inferSchema=True)

    # Q1
    answer1 = dataframe.select('_c11') \
        .distinct()
    question_results.append([1, answer1.count(), answer1._jdf.queryExecution().simpleString()])

    # Q2
    answer2 = dataframe.groupby('_c0') \
        .count() \
        .agg({'count': 'mean'})
    question_results.append([2, answer2.first()[0], answer2._jdf.queryExecution().simpleString()])

    # Q3
    answer3 = dataframe.where(dataframe._c13 != '-') \
        .groupBy("_c13") \
        .count() \
        .sort("count", ascending=False)
    question_results.append([3, answer3.first()[0], answer3._jdf.queryExecution().simpleString()])

    # Q4
    answer4 = dataframe.groupby('_c11') \
        .agg({'_c2': 'mean'})
    question_results.append([4, answer4.first()[1], answer4._jdf.queryExecution().simpleString()])

    # Q5
    answer5 = dataframe.where(dataframe._c11 != '-') \
        .groupby('_c11') \
        .count() \
        .sort('count', ascending=False)
    question_results.append([5, [i[0] for i in answer5.take(10)],
                             answer5._jdf.queryExecution().simpleString()])

    # Q6
    answer6 = dataframe.where(dataframe._c11 != '-') \
        .withColumn('same_size', f.when((dataframe['_c7'] - dataframe['_c6']) >
                                        (dataframe['_c2'] * 0.9), 1)) \
        .filter(f.col('same_size').between(0, 2)) \
        .groupBy('_c11') \
        .count() \
        .sort('count', ascending=False)
    question_results.append([6, [i[0] for i in answer6.take(10)],
                             answer5._jdf.queryExecution().simpleString()])

    # Q7
    answer7 = dataframe.where(dataframe._c12 != '-') \
        .withColumn('word', f.explode(f.split(f.col('_c12'), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)
    question_results.append([7, [i[0] for i in answer7.take(10)],
                             answer7._jdf.queryExecution().simpleString()])

    # Q8
    answer8 = dataframe.where(dataframe._c12 != '-') \
        .withColumn('word', f.explode(f.split(f.col('_c12'), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=True)
    question_results.append([8, [i[0] for i in answer8.take(10)],
                             answer8._jdf.queryExecution().simpleString()])

    # Q9
    answer9 = dataframe.where(dataframe._c11 != '-') \
        .withColumn('same_size', f.when((dataframe['_c7'] - dataframe['_c6']) >
                                        (dataframe['_c2'] * 0.9), 1)) \
        .filter(f.col('same_size').between(0, 2)) \
        .withColumn('word', f.explode(f.split(f.col('_c12'), ' '))) \
        .groupBy('word') \
        .count() \
        .sort('count', ascending=False)
    question_results.append([9, [i[0] for i in answer9.take(10)],
                             answer9._jdf.queryExecution().simpleString()])

    # Q10
    answer10 = dataframe.where(dataframe._c11 != '-') \
        .select('_c0', '_c2', '_c11') \
        .withColumn('counts', f.count('_c11').over(Window.partitionBy('_c0'))) \
        .dropDuplicates(['_c0'])
    question_results.append([10, answer10.stat.corr('_c2', 'counts'),
                             answer10._jdf.queryExecution().simpleString()])

    spark.stop()

    return question_results


def write_output(result, output=sys.stdout):
    """write output to csv format"""
    csv_writer = csv.writer(output, dialect='excel')
    for row in result:
        csv_writer.writerow(row)


if __name__ == '__main__':
    results = get_result(sys.argv[1])
    write_output(results)
