# to_parquet
import sys
import pandas as pd
from csv import QUOTE_NONE

PARQ_PATH=sys.argv[1]
CSV_PATH=sys.argv[2]

def main():
#    print(PARQ_PATH)
    columns = ['dt', 'cmd', 'cnt']
    df = pd.read_csv(CSV_PATH, on_bad_lines='skip', names=columns, 
        keep_default_na=False, quoting=QUOTE_NONE)
 
    df.to_parquet(PARQ_PATH, partition_cols=['dt'])
    
if __name__ == "__main__":
    main()
