# to_parquet
import sys
import pandas as pd
from csv import QUOTE_NONE

def main():
    date = sys.argv[1]
    parq_path = "/home/nishtala/db/parquet/" + date + ".parquet"

    print(parq_path)

    columns = ['dt', 'cmd', 'cnt']
    df = pd.read_csv('~/db/csv/'+date+".csv", on_bad_lines='skip', names=columns, keep_default_na=False, quoting=QUOTE_NONE)
 
    df.to_parquet(parq_path, partition_cols=['dt'])
    
if __name__ == "__main__":
    main()
