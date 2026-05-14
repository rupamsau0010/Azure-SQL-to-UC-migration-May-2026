# from pathlib import Path

# # Load .env from the current working directory
# env_path = Path(__file__).resolve().parent / '.env'

# if not env_path.exists():
#     raise FileNotFoundError(f'.env file not found at {env_path}')

# with env_path.open('r', encoding='utf-8') as f:
#     env_vars = {}
#     for line in f:
#         line = line.strip()
#         if not line or line.startswith('#'):
#             continue
#         if '=' not in line:
#             continue
#         key, value = line.split('=', 1)
#         env_vars[key.strip()] = value.strip()

# sql_password = env_vars.get('SQL_PASSWORD')
# print(sql_password)

import pandas as pd
from pathlib import Path

root = Path(__file__).resolve().parent
for parquet_path in sorted(root.glob('*.parquet')):
    df = pd.read_parquet(parquet_path)
    print(f"{parquet_path.name}: {len(df)} rows")


"last_watermark": "2018-10-17T17:30:18Z",
"new_watermark": "2018-10-17 17:30:18.0000000"