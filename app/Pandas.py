import pandas as pd

json_data = '''
[
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30},
    {"name": "Charlie", "age": 35}
]
'''

df = pd.read_json(json_data, orient='records', dtype={'age': 'float64'})
print(df)