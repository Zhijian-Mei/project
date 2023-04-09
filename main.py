import argparse
import numpy as np
import pyspark
from pyspark import SparkContext as sc
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time


def get_graph(args):
    filename = args.path
    try:
        graph_text = open(filename, 'r')
    except OSError:
        print('Graph file does not exist')
    line_count = 0
    number_node = None
    number_edge = None
    graph_data = {}
    for line in graph_text.readlines():
        line = line.split()
        if line_count == 0:
            number_node = int(line[0])
            number_edge = int(line[1])
            print(f'graph has {number_node} nodes ,{number_edge} edges')
        else:
            out_node = line[0]
            in_node = line[1]
            distance = float(line[2])
            key = f'{out_node}to{in_node}'
            if key not in graph_data:
                graph_data[f'{out_node}to{in_node}'] = distance
            else:
                print('graph has error')
                quit()
        line_count+=1
    matrix_data = []
    for i in range(number_node):
        for j in range(number_node):
            if i == j:
                continue
            key = f'{i}to{j}'
            if key not in graph_data:
                matrix_data.append((i,j,np.inf))
            else:
                matrix_data.append((i,j,graph_data[key]))
    return matrix_data,number_node

def get_graph_sequential(args):
    filename = args.path
    try:
        graph_text = open(filename, 'r')
    except OSError:
        print('Graph file does not exist')
    line_count = 0
    number_node = None
    number_edge = None
    graph_data = {}
    for line in graph_text.readlines():
        line = line.split()
        if line_count == 0:
            number_node = int(line[0])
            number_edge = int(line[1])
            print(f'graph has {number_node} nodes ,{number_edge} edges')
        else:
            out_node = line[0]
            in_node = line[1]
            distance = float(line[2])
            key = f'{out_node}to{in_node}'
            if key not in graph_data:
                graph_data[f'{out_node}to{in_node}'] = distance
            else:
                print('graph has error')
                quit()
        line_count += 1
    matrix_data = [[0 for _ in range(number_node)] for _ in range(number_node)]
    for i in range(number_node):
        for j in range(number_node):
            if i == j:
                continue
            key = f'{i}to{j}'
            if key not in graph_data:
                matrix_data[i][j] = np.inf
            else:
                matrix_data[i][j] = graph_data[key]
    return matrix_data,number_node

def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', type=str, required=True)
    # parser.add_argument('--mode', type=str, default='sequential')
    args = parser.parse_args()
    return args

def solve_spark_df(graph_data,number_node):
    matrix = spark.createDataFrame(data=graph_data,schema=['out_node','in_node','distance'])
    print(matrix.show())
    for pivot_index in range(number_node):
        left = matrix.filter(matrix.in_node == pivot_index)\
            .withColumnRenamed('in_node', 'left_pivot') \
            .withColumnRenamed('distance', 'left_distance')

        right = matrix.filter(matrix.out_node == pivot_index)\
            .withColumnRenamed('out_node', 'right_pivot') \
            .withColumnRenamed('distance', 'right_distance')

        df = matrix.join(broadcast(right),'in_node').join(broadcast(left),'out_node')

        df = df.select('out_node','in_node','distance',(df.left_distance+df.right_distance).alias('candidate_distance'))
        df = df.withColumn('new_distance', least('distance', 'candidate_distance'))\
            .withColumnRenamed('out_node', 'out_') \
            .withColumnRenamed('in_node', 'in_')\
            .select('out_','in_','new_distance')
        cond = [matrix.out_node == df.out_, matrix.in_node == df.in_]
        matrix = matrix.join(broadcast(df),cond,'left').select('out_node','in_node','distance','new_distance')
        matrix = matrix.rdd.map(lambda x:(x[0],x[1],x[2]) if x[3] is None else (x[0],x[1],x[3])).toDF(['out_node','in_node','distance'])
        matrix.cache()
    return matrix

def solve_spark_rdd(rdd,num_node):
    for k in range(num_node):
        start = time.time()
        left = rdd.filter(lambda x:x[1]==k)
        print(left.collect())
        end = time.time()
        print(end-start)
        quit()


def solve_sequential(matrix,number_node):
    for k in range(number_node):
        for i in range(number_node):
            for j in range(number_node):
                if i == j:
                    continue
                if i == k or j == k:
                    continue
                matrix[i][j] = min(matrix[i][j],matrix[i][k]+matrix[k][j])
    return matrix


if __name__ == '__main__':
    spark = SparkSession.builder.appName("All pairs shortest path").getOrCreate()
    args = get_args()

    # sequential version
    # print('solved by 3 loop')
    # graph_matrix,number_node = get_graph_sequential(args)
    # start = time.time()
    # result_matrix = solve_sequential(graph_matrix,number_node)
    # end = time.time()
    # print(f'sequential algorithm takes {end-start}')
    # print('the result is: ')
    # for g in graph_matrix:
    #     print(g)

    print('solving by spark')
    graph_data,number_node = get_graph(args)

    start = time.time()
    result_matrix = solve_spark_df(graph_data,number_node)
    end = time.time()
    print(f'spark algorithm takes {end - start}')
    print('the result is: ')
    print(result_matrix.show())




