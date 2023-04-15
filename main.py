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
from pyspark.mllib.linalg.distributed import CoordinateMatrix

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
    matrix_data = []
    for i in range(number_node):
        for j in range(number_node):
            if i == j:
                continue
            key = f'{i}to{j}'
            if key not in graph_data:
                matrix_data.append((i, j, np.inf))
            else:
                matrix_data.append((i, j, graph_data[key]))
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
    # parser.add_argument('--mode', type=str, default='sequential')
    args = parser.parse_args()
    return args


def solve_spark_df(graph_data, number_node):
    matrix = spark.createDataFrame(data=graph_data, schema=['out_node', 'in_node', 'distance'])
    print('Number of partition: ', matrix.rdd.getNumPartitions())
    print('Number of entries: ', matrix.count())
    def f(partition):
        for x in partition:
            yield (x[0],x[1],x[2]) if x[3] is None else (x[0],x[1],x[3])
    for pivot_index in trange(number_node):
        left = matrix.filter(matrix.in_node == pivot_index) \
            .withColumnRenamed('in_node', 'left_pivot') \
            .withColumnRenamed('distance', 'left_distance')

        right = matrix.filter(matrix.out_node == pivot_index) \
            .withColumnRenamed('out_node', 'right_pivot') \
            .withColumnRenamed('distance', 'right_distance')

        sub_matrix = matrix \
            .filter(matrix.in_node != pivot_index)\
            .filter(matrix.out_node != pivot_index)\
            .join(right, 'in_node', 'left') \
            .join(left, 'out_node', 'left') \
            # .select('out_node', 'in_node', 'distance',
            #         (col('left_distance') + col('right_distance')).alias('candidate_distance'))
        print(sub_matrix.show())
        quit()
        sub_matrix = sub_matrix.withColumn('new_distance', least('distance', 'candidate_distance')) \
            .withColumnRenamed('out_node', 'out_') \
            .withColumnRenamed('in_node', 'in_') \
            .select('out_', 'in_', 'new_distance')
            
        cond = [matrix.out_node == sub_matrix.out_, matrix.in_node == sub_matrix.in_]
        matrix = matrix\
            .join(sub_matrix, cond, 'left')\
            .select('out_node','in_node','distance','new_distance')
            # .withColumn('result',when(col('new_distance').isNull(),col('distance')).otherwise(col('new_distance')))
        matrix = matrix.rdd.mapPartitions(f).toDF(['out_node','in_node','distance'])


        
        # matrix = matrix .join(sub_matrix, cond, 'left').select('out_node', 'in_node', 'distance')
        #     # .na.fill(value=np.inf, subset=['new_distance']) \
        #     # .withColumn('result', least('distance', 'new_distance')) \
        #     # .select('out_node', 'in_node', col('result').alias('distance'))
        # # matrix = matrix \
        # #     .select('out_node', 'in_node', 'distance')

        

     
        # left = matrix.filter(matrix.in_node == pivot_index)\
        #     .withColumnRenamed('in_node', 'left_pivot') \
        #     .withColumnRenamed('distance', 'left_distance')

        # right = matrix.filter(matrix.out_node == pivot_index)\
        #     .withColumnRenamed('out_node', 'right_pivot') \
        #     .withColumnRenamed('distance', 'right_distance')

        # df = matrix.join(right,'in_node').join(left,'out_node')



        # df = df.select('out_node','in_node','distance',(df.left_distance+df.right_distance).alias('candidate_distance'))
        # df = df.withColumn('new_distance', least('distance', 'candidate_distance'))\
        #     .withColumnRenamed('out_node', 'out_') \
        #     .withColumnRenamed('in_node', 'in_')\
        #     .select('out_','in_','new_distance')


        # cond = [matrix.out_node == df.out_, matrix.in_node == df.in_]
        # matrix = matrix.join(df,cond,'left').select('out_node','in_node','distance','new_distance')
        
        # # matrix = matrix.select('out_node', 'in_node', 'distance')
        # matrix = matrix.rdd.map(lambda x:(x[0],x[1],x[2]) if x[3] is None else (x[0],x[1],x[3])).toDF(['out_node','in_node','distance'])

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
                if i == j:
                    continue
                if i == k or j == k:
                    continue
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

    print('solving by spark')
    graph_data, number_node = get_graph(args)
    start = time.time()
    result = solve_spark_df(graph_data, number_node)
    end = time.time()
    print('the result is: ')
    print(result.show())
    print(f'spark algorithm takes {end - start}')
