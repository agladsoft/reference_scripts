import json
import os
import sys
import numpy as np
import pandas as pd

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]


df = pd.read_csv(input_file_path, dtype=str)
df = df.replace({np.nan: None})
parsed_data = df.to_dict('records')
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)