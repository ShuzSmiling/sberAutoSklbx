import numpy as np
import pandas as pd
import json


with open('../data/test_json/ga_sessions_new_2022-01-02.json') as f:
    data = json.load(f)

key = ''
for i in data.keys():
    key = i

print(pd.DataFrame.from_dict(data[key]).columns)
# data = data[data.keys()[0]]

# df = pd.DataFrame.from_dict(data)

# print(data)








