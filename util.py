import numpy as np
from pyspark.rdd import portable_hash 
from tqdm import trange
from pyspark import SparkContext as sc
def minminplus(A, B, D):
    assert A.shape[1] == B.shape[0], 'Matrix dimension mismatch in minminplus()'

    for i in range(A.shape[0]):
        for j in range(B.shape[1]):
            localsum = np.inf
            for k in range(A.shape[1]):
                localsum = min(localsum, A[i, k] + B[k, j])
            D[i, j] = min(D[i, j], localsum)
    return D


def minplus(A, B):
    assert A.shape[1] == B.shape[0], 'Matrix dimension mismatch in minplus()'

    C = np.zeros((A.shape[0], B.shape[1]))
    for i in range(A.shape[0]):
        for j in range(B.shape[1]):
            localsum = np.inf
            for k in range(A.shape[1]):
                localsum = min(localsum, A[i, k] + B[k, j])
            C[i, j] = localsum
    return C


def solve_sequential(matrix):
    number_node = len(matrix)
    result = np.zeros((number_node, number_node))
    for k in range(number_node):
        for i in range(number_node):
            for j in range(number_node):
                result[i][j] = min(matrix[i][j], matrix[i][k] + matrix[k][j])
    return result


def partitioner(A, p):
    assert A.shape[0] % p == 0, 'non-perfect partition number in partitioner'
    result = []
    s = A.shape[0]//p
    for i in range(p):
        for j in range(p):
            result.append(((i, j), A[i*s:i*s + s, j*s:j*s + s]))
    return result


def cust_partitioner(key):
    return (key[0] + key[1])


def solve_apsp_block(a, p, sc, num_partition):
    def phrase_1(x):
        result = solve_sequential(x[1])
        return (x[0], result)

    def phrase_2(x, diagonal):
        if (x[0][1] == diagonal[0][0][0]):
            result = minminplus(x[1], diagonal[0][1], x[1])
        else:
            result = minminplus(diagonal[0][1], x[1], x[1])
        return (x[0], result)

    def phrase_3(x, RowAndColumn):
        (i, j) = x[0]
        result = x[1]
        for blk in RowAndColumn:
            if blk[0][0] == i:
                left = blk[1]
            if blk[0][1] == j:
                right = blk[1]

        result = minminplus(left, right, x[1])
        return ((i, j), result)

    for k in trange(p):
        diagonal = a.filter(lambda x: x[0] == (k, k)).map(phrase_1)

        b = diagonal.collect()
        diagonal.cache()

        RowAndColumn = a \
            .filter(lambda x: (x[0][0] == k or x[0][1] == k) and not (x[0][0] == k and x[0][1] == k)) \
            .map(lambda x: phrase_2(x, b))

        c = RowAndColumn.collect()
        RowAndColumn.cache()

        rest = a \
            .filter(lambda x: x[0][0] != k and x[0][1] != k) \
            .map(lambda x: phrase_3(x, c))

        # a = sc.union([diagonal, RowAndColumn, rest]).coalesce(num_partition)
        a = sc.union([diagonal, RowAndColumn, rest]).partitionBy(num_partition, cust_partitioner)
        a.cache()
    return a


def solve_apsp_block_checkpoint(a, p, sc,num_partition):
    def phrase_1(x):
        result = solve_sequential(x[1])
        return (x[0], result)

    def phrase_2(x, diagonal):
        if (x[0][1] == diagonal[0][0][0]):
            result = minminplus(x[1], diagonal[0][1], x[1])
        else:
            result = minminplus(diagonal[0][1], x[1], x[1])
        return (x[0], result)

    def phrase_3(x, RowAndColumn):
        (i, j) = x[0]
        result = x[1]
        for blk in RowAndColumn:
            if blk[0][0] == i:
                left = blk[1]
            if blk[0][1] == j:
                right = blk[1]

        result = minminplus(left, right, x[1])
        return ((i, j), result)

    for k in range(p):
        if k >= 1:
            a = sc.pickleFile("/Users/jasony/Desktop/ust/MSBD5003/project/project/data/test" + str(k - 1))

        diagonal = a.filter(lambda x: x[0] == (k, k)).map(phrase_1)

        b = diagonal.collect()
        diagonal.cache()

        RowAndColumn = a.filter(lambda x: (x[0][0] == k or x[0][1] == k) and not (x[0][0] == k and x[0][1] == k)).map(
            lambda x: phrase_2(x, b))
        c = RowAndColumn.collect()
        RowAndColumn.cache()

        rest = a.filter(lambda x: x[0][0] != k and x[0][1] != k).map(lambda x: phrase_3(x, c))

        rest.cache()
        a = sc.union([diagonal, RowAndColumn, rest]).coalesce(num_partition)#.partitionBy(64, cust_partitioner)
        a.cache()

        a.saveAsPickleFile("/Users/jasony/Desktop/ust/MSBD5003/project/project/data/test" + str(k))
    return a

file_path = "/Users/jasony/Desktop/ust/MSBD5003/project/project/data/"
def solve_apsp_block_file(a, p,sc):
    def phrase_1(x):
        result = solve_sequential(x[1])
        return (x[0], result)

    def phrase_2(x, k):
        diagonal = np.load(file_path + "blk" + str(k) + str(k) + str(k) + ".npy")
        # print(diagonal, k)
        if (x[0][1] == k):
            result = minminplus(x[1], diagonal, x[1])
        else:
            result = minminplus(diagonal, x[1], x[1])
        return (x[0], result)

    def phrase_3(x, k):
        (i, j) = x[0]
        result = x[1]
        left = np.load(file_path + "blk" + str(k) + str(i) + str(k) + ".npy")
        right = np.load(file_path + "blk" + str(k) + str(k) + str(j) + ".npy")
        result = minminplus(left, right, x[1])
        return (x[0], result)

    for k in range(p):

        diagonal = a.filter(lambda x: x[0] == (k, k)).map(phrase_1)
        b = diagonal.collect()
        np.save(file_path + "blk" + str(k) + str(k) + str(k), b[0][1])
        diagonal.cache()

        RowAndColumn = a.filter(lambda x: (x[0][0] == k or x[0][1] == k) and not (x[0][0] == k and x[0][1] == k)).map(
            lambda x: phrase_2(x, k))
        c = RowAndColumn.collect()
        for blk in c:
            (i, j) = blk[0]
            np.save(file_path + "blk" + str(k) + str(i) + str(j), blk[1])
        RowAndColumn.cache()

        rest = a.filter(lambda x: x[0][0] != k and x[0][1] != k).map(lambda x: phrase_3(x, k))
        rest.cache()
        a = sc.union([diagonal, RowAndColumn, rest]).coalesce(num_partition)# .partitionBy(64, cust_partitioner)
        a.cache()
    return a.collect()
