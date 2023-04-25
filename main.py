import argparse
import numpy as np
import pyspark
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import least, broadcast, col, when, isnan, udf
from tqdm import trange
import time
import util
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
        line_count += 1
    matrix_data = np.ones((number_node, number_node)) * np.inf
    for i in range(number_node):
        for j in range(number_node):
            if i == j:
                matrix_data[i][j] = 0
                continue
            key = f'{i}to{j}'
            if key in graph_data:
                matrix_data[i][j] = graph_data[key]
                # matrix_data.append((i, j, graph_data[key]))
            else:
                matrix_data[i][j] = np.inf
    return matrix_data, number_node


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
    matrix_data = [[0.0 for _ in range(number_node)] for _ in range(number_node)]
    for i in range(number_node):
        for j in range(number_node):
            if i == j:
                continue
            key = f'{i}to{j}'
            if key not in graph_data:
                matrix_data[i][j] = np.inf
            else:
                matrix_data[i][j] = graph_data[key]
    return matrix_data, number_node


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', type=str, required=True)
    parser.add_argument('--nb', type=int, default=2)
    parser.add_argument('--np',type=int,default=8)
    args = parser.parse_args()
    return args


def solve_spark_df(graph_matrix, number_node):
    matrix = graph_matrix
    print('Number of partition: ', matrix.rdd.getNumPartitions())
    print('Number of entries: ', matrix.count())
    def f(partition):
        for x in partition:
            if x[3] is None or x[5] is None:
                yield x[0],x[1],x[2]
            elif x[0] == x[3]:
                yield x[0],x[1],x[2]
            else:
                yield x[0],x[1],min(x[2],x[4]+x[6])


    for pivot_index in trange(number_node):
        left = matrix.filter(matrix.in_node == pivot_index) \
            .withColumnRenamed('in_node', 'left_pivot') \
            .withColumnRenamed('distance', 'left_distance')

        right = matrix.filter(matrix.out_node == pivot_index) \
            .withColumnRenamed('out_node', 'right_pivot') \
            .withColumnRenamed('distance', 'right_distance')

        matrix = matrix \
            .join(right, 'in_node', 'left') \
            .join(left, 'out_node', 'left') \
            .rdd.mapPartitions(f).toDF(['out_node','in_node','distance'])

        matrix.cache()
    return matrix


def solve_spark_rdd(graph_data, num_node):   
    rdd = sc.parallelize(graph_data).map(lambda x:((x[0],x[1]),x[2]))
    
    def f1(it):
        for x in it:
            yield ((x[0][0][0],x[1][0][1]),x[0][1]+x[1][1]) 

    def f2(it):
        for x in it:
            if x[1][1] is None:
                yield ((x[0][0],x[0][1]),x[1][0])
            else:
                yield ((x[0][0],x[0][1]),min(x[1]))
                
    for k in trange(num_node):
        left = rdd.filter(lambda x: x[0][1] == k).filter(lambda x:x[1] != np.inf)
        right = rdd.filter(lambda x: x[0][0] == k).filter(lambda x:x[1] != np.inf)
        candidate_result = left.cartesian(right).mapPartitions(f1)
        rdd = rdd.leftOuterJoin(candidate_result).mapPartitions(f2)
        rdd.cache()
        
    
    return rdd


def solve_sequential(matrix, number_node):
    for k in trange(number_node):
        for i in range(number_node):
            for j in range(number_node):
                matrix[i][j] = min(matrix[i][j], matrix[i][k] + matrix[k][j])
    return matrix


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("APSP") \
        .config("spark.memory.fraction", 0.8) \
        .config("spark.executor.memory", "14g") \
        .config("spark.driver.memory", "12g")\
        .config("spark.sql.shuffle.partitions" , "800") \
        .getOrCreate()
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 3*104857600)
    sc = spark.sparkContext
    args = get_args()
    # sequential version
    # print('solved by Sequential')
    # graph_matrix, number_node = get_graph_sequential(args)
    # start = time.time()
    # result_matrix = solve_sequential(graph_matrix, number_node)
    # end = time.time()
    # print(f'sequential algorithm takes {end - start}')
    for node_number in [2000,1000,500,200,100]:
        args.path = f'{node_number}.txt'
        p = args.nb
        num_partition = args.np
        print('number of block: ',p**2)
        print('solving by spark')
        graph_data, number_node = get_graph(args)
        start = time.time()
        r = util.partitioner(graph_data,p)
        rdd = sc.parallelize(r,num_partition)
        a = util.solve_apsp_block(rdd,p,sc,num_partition)
        result = a.collect()
        end = time.time()
        print(f'{number_node} node on spark algorithm takes {end - start}')
    # start = time.time()
    # b = util.solve_sequential(graph_data)
    # end = time.time()
    # print(f'sequential algorithm takes {end - start}')