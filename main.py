import os
import sys

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json
from flexible_jobshop_simulator import FJSS_SIMULATOR

pd.set_option('display.max_rows', sys.maxsize)
pd.set_option('display.max_columns', sys.maxsize)

args = json.load(open('arguments.json'))

data_args = args["data_args"]
sim_args = args["sim_args"]

data_path = data_args["data_dir"]
instance = 1
data_path = os.path.join(data_path, f"Ins.#{instance}.csv")
data = pd.read_csv(data_path, header=None, delimiter=";")
env = FJSS_SIMULATOR(data, instance)
