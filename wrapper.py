import sys
from importlib import reload
import tables_as_lists
from tables_as_lists import table_names, attributes, output_dir, number_of_samples
import BernoulliSample

conn_cache = None
if __name__ == "__main__":
    while True:
        if not conn_cache:
            conn_cache = tables_as_lists.test()
        try:
            BernoulliSample.join(output_dir, table_names, attributes, number_of_samples)
            print("Press enter to re-run the script, CTRL-C to exit")
            sys.stdin.readline()
            reload(BernoulliSample)
        except:

            print("Press enter to re-run the script, CTRL-C to exit")
            sys.stdin.readline()
            reload(BernoulliSample)
