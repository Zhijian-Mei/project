import argparse
import numpy as np
import pyspark
from pyspark import SparkContext as sc
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import least,udf

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

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
    matrix_df = spark.createDataFrame(data=matrix_data,schema=['out_node','in_node','distance'])
    return matrix_df,number_node



def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--path', type=str, required=True)
    args = parser.parse_args()
    return args

def solve(matrix,number_node):
    print(matrix.show())
    for pivot_index in range(number_node):


        left = matrix.filter(matrix.in_node == pivot_index)\
            .withColumnRenamed('in_node', 'left_pivot') \
            .withColumnRenamed('distance', 'left_distance')
        right = matrix.filter(matrix.out_node == pivot_index)\
            .withColumnRenamed('out_node', 'right_pivot') \
            .withColumnRenamed('distance', 'right_distance')
        df = matrix.join(right,'in_node').join(left,'out_node')
        df = df.select('out_node','in_node','distance',(df.left_distance+df.right_distance).alias('candidate_distance'))
        df = df.withColumn('new_distance', least('distance', 'candidate_distance'))\
            .withColumnRenamed('out_node', 'out_') \
            .withColumnRenamed('in_node', 'in_')\
            .select('out_','in_','new_distance')
        print(df.show())
        cond = [matrix.out_node == df.out_, matrix.in_node == df.in_]
        matrix = matrix.join(df,cond,'left').select('out_node','in_node','distance','new_distance')

        print(matrix.show())
        quit()
    return matrix

if __name__ == '__main__':
    args = get_args()
    graph_matrix,number_node = get_graph(args)
    result_matrix = solve(graph_matrix,number_node)
