import numpy as np


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
    for k in range(number_node):
        for i in range(number_node):
            for j in range(number_node):
                matrix[i][j] = min(matrix[i][j], matrix[i][k] + matrix[k][j])
    return matrix


def partitioner(A, p):
    assert A.shape[0] % p == 0, 'non-perfect partition number in partitioner'
    result = []
    s = A.shape[0]//p
    for i in range(p):
        for j in range(p):
            result.append([(i, j), A[i:i + s, j:j + s]])
    return result


def solve_apsp_block(a):
    def getDiagonal(x, k):
        for ele in x:
            if ele[0][0] == ele[0][1] and ele[0][0] == k:
                return True
        return False

    def prase_1():
        pass

    def prase_2():
        pass

    def prase_3():
        pass

    a.filter(lambda x: getDiagonal(x, 1))
    return a


def solve_apsp_block(a, p):
    def phrase_1(x):
        x[1] = solve_sequential(x[1])
        return x

    def phrase_2(x, diagonal):
        result = x[1]
        if (x[0][1] == diagonal[0][0]):
            x[1] = minminplus(x[1], diagonal[1], x[1])
        else:
            x[1] = minminplus(diagonal[1], x[1], x[1])
        return x

    def phrase_3(x, RowAndColumn):
        (i, j) = x[0]
        result = x[1]
        for blk in RowAndColumn:
            if blk[0][0] == i:
                left = blk[1]
            if blk[0][1] == j:
                right = blk[1]

        x[1] = minminplus(left, right, x[1])
        return x

    for k in range(p):
        diagonal = a.filter(lambda x: x[0] == (k, k)).map(phrase_1)

        b = diagonal.collect()
        diagonal.cache()

        RowAndColumn = a.filter(lambda x: (x[0][0] == k or x[0][1] == k) and not (x[0][0] == k and x[0][1] == k)).map(
            lambda x: phrase_2(x, b[0]))

        c = RowAndColumn.collect()
        RowAndColumn.cache()

        rest = a.filter(lambda x: x[0][0] != k and x[0][1] != k).map(lambda x: phrase_3(x, c))

        rest.cache()
        a = sc.union([diagonal, RowAndColumn, rest])
    return a
