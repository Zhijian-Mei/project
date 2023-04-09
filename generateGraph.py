import os
import random

from tqdm import trange

for i in trange(1000):
    number_node = random.randint(5,15)
    number_edge = random.randint(10,20)
    random_seed = random.randint(0,100)
    output_path = f'data/graph{i}.txt'
    command = f'python3 graphGenerator.py -grnm -n {number_node} -m {number_edge} -w int --seed {random_seed} --output {output_path}'
    os.system(command)