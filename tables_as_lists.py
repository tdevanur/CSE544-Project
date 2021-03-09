from numpy import random
from math import floor, sqrt
import csv
import time
import psycopg2
import os
# import errno
from psycopg2.extensions import AsIs
import re
from os import listdir
from os.path import isfile, join as pathjoin, abspath
# import glob
import json
import matplotlib.pyplot as plt

LARGE_PRIME = 1000000007

"""
table_names: tables involved in the join
attribute_names: join attributes for the corresponding tables
attribute_indices: column indices of join attributes, needed to hash values for correlated sampling
sampling_method: bernoulli or correlated
sampling_probability: (0,1)
number_of_samples
"""
# table_names = ['title', 'cast_info']
# attributes = ['id', 'movie_id']
# attribute_indices = [0, 2]
# sampling_method = 'correlated'
# sampling_probability = .001
# number_of_samples = 5

table_names = ['title', 'movie_keyword', 'cast_info']
attributes = ['id', 'movie_id', 'movie_id']
number_of_samples = 2
attribute_indices = [0, 1, 2]
sampling_method = 'correlated'
sampling_probability = .001
plan_rows = 102405662
actual_rows = 358045425

"""
Following is used for testing
FIX - remove instances of testing_mode when finished
"""

testing_mode = 0
small_table_probability = 2

if testing_mode == 1:
    original_tables_dir = '/home/tejas/Desktop/dbms/Project/smalltables/' + str(
        int(small_table_probability)) + '/'

elif testing_mode == 0:
    original_tables_dir = '/home/tejas/Desktop/dbms/Project/tables/'

"""
To test validity of parameters
"""
if len(table_names) != len(attributes):
    print("# of attributes doesn't match # of tables!")

if len(table_names) <= 1:
    print('you need at least two tables!')
    exit()

# directory where results will be written
output_dir = '/home/tejas/Desktop/dbms/Project/results/Bernoulli'

# directory where samples will be written
destination = '/home/tejas/Desktop/dbms/Project/samples/'

# needed to connect to postgres
param_dic = {
    "host": "localhost",
    "database": "imdbload",
    "user": "postgres",
    "password": "tejas"
}


# for sorting files in a directory correctly
def natural_key(string_):
    """See https://blog.codinghorror.com/sorting-for-humans-natural-sort-order/"""
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', string_)]


"""
hash function which maps domain uniformly into [0,1]
"""


def uniform_hash(a, b, value):
    return ((a * value + b) % LARGE_PRIME) / LARGE_PRIME


"""
This function writes a number of samples of the tables that occur in the given eq-join
to destination, using the given method and sampling probability.
"""


def sampler(table_names, attribute_indices, number_of_samples, this_method, this_probability):
    hash_parameters = []
    source_files = []
    target_files = [[] for j in range(number_of_samples)]
    sample_writers = [[] for j in range(number_of_samples)]
    # join_values_dics = [{} for j in range(number_of_samples)]
    # key_counters = [0 for j in range(number_of_samples)]
    for table_name in table_names:
        source_files.append(original_tables_dir + table_name + '.csv')
    for sample_index in range(number_of_samples):
        a = random.randint(low=0, high=LARGE_PRIME)
        b = random.randint(low=0, high=LARGE_PRIME)
        hash_parameters.append([a, b])
        for i in range(len(table_names)):
            table_name = table_names[i]
            target_file = destination + table_name + '/' + table_name + str(sample_index + 1) + '.csv'
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            target_files[sample_index].append(target_file)
            sample = open(target_files[sample_index][i], 'w')
            sample_writers[sample_index].append(csv.writer(sample))

    for i in range(len(table_names)):
        print(table_names[i])
        attribute_index = attribute_indices[i]
        table = open(source_files[i], 'r')
        table_reader = csv.reader(table)
        temp_counter = 0

        for row in table_reader:
            temp_counter += 1
            if temp_counter % 10000 == 0:
                print(str(temp_counter) + "...", end="\r")
            for j in range(number_of_samples):
                a = hash_parameters[j][0]
                b = hash_parameters[j][1]

                if this_method == 'bernoulli':
                    hash_value = uniform_hash(a, b, temp_counter)
                elif this_method == 'correlated':
                    join_value = row[attribute_index]
                    # FIX - when attribute values are strings
                    #
                    # if join_value in join_values_dics[j]:
                    #
                    #     join_value_key = join_values_dics[j][join_value]
                    # else:
                    #     join_value_key = key_counters[j]
                    #     join_values_dics[j][join_value] = join_value_key
                    #     key_counters[j] += 1
                    join_value_key = int(join_value)

                    hash_value = uniform_hash(a, b, join_value_key)
                else:
                    hash_value = 1
                    print('sampling method invalid')
                    exit()
                if hash_value < this_probability:
                    sample_writers[j][i].writerow(row)
        table.close()


"""
Connect to Postgres using psycopg2 using these parameters
"""


def connect(params_dic):
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn


"""
Loads all samples into Postgres
"""


def copy_samples(conn, table_names, number_of_files):
    for i in table_names:

        table_dir = '/home/tejas/Desktop/dbms/Project/samples/' + i

        sec_files = sorted(os.listdir(table_dir), key=natural_key)
        for i in range(number_of_files):
            file = sec_files[i]
            if os.path.basename(file)[0] == '.':
                del sec_files[i]
        drop_query = """
                DROP TABLE IF EXISTS %(table)s
                """

        query = """
                CREATE TABLE %(new_table)s ( like %(base_table)s including all)
                """

        with conn.cursor() as cursor:
            for i in range(number_of_files):
                file = sec_files[i]
                name = os.path.basename(file).split('.')[0]
                cursor.execute(drop_query, {'table': AsIs(name)})
                conn.commit()
                cursor.execute(query,
                               {'new_table': AsIs(name),
                                'base_table': AsIs(''.join(i for i in name if not i.isdigit()))})
                conn.commit()
                sqlstr = "COPY " + name + " FROM STDIN DELIMITER ',' CSV"
                with open(table_dir + '/' + name + '.csv', 'r') as fin:
                    try:
                        cursor.copy_expert(sqlstr, fin)
                        conn.commit()
                        # print('Data inserted into ', name, ' complete')
                    except (Exception, psycopg2.DatabaseError) as error:
                        print("Error: %s" % error)
                        conn.rollback()
                        cursor.close()
                        return 1
                if i % 10 == 0:
                    print('Data inserted into ', i, ' samples')


"""
Executes the join query for each set of samples, as well as an explain query
to obtain the true join size and Postgres' estimate. Writes the result in json
format to output_dir.
"""


def join(conn, output_dir, table_names, attributes, number_of_samples, p, method, no_explanation):
    sampling_probability = p
    sampling_method = method

    cursor = conn.cursor()
    outputs = []

    join_query_test = "SELECT COUNT(*) FROM "
    for i in range(len(table_names) - 1):
        join_query_test += table_names[i] + "%(index)s" + " t" + str(i) + ", "
    join_query_test += table_names[len(table_names) - 1] + "%(index)s" + " t" + str(len(table_names) - 1) + " WHERE "
    for i in range(len(table_names) - 2):
        join_query_test += "t" + str(i) + "." + attributes[i] + " = " + "t" + str(i + 1) + "." + \
                           attributes[i + 1] + " and "
    join_query_test += "t" + str(len(table_names) - 2) + "." + attributes[len(table_names) - 2] + " = " + \
                       "t" + str(len(table_names) - 1) + "." + attributes[len(table_names) - 1]

    drop_query = """
                DROP TABLE IF EXISTS %(table)s
                """

    print(join_query_test)
    for j in range(number_of_samples):
        try:
            cursor.execute(join_query_test, {"index": j + 1})
            output = (cursor.fetchall()[0])[0]
            for k in range(len(table_names)):
                name = table_names[k] + str(j + 1)
                cursor.execute(drop_query, {'table': AsIs(name)})
                conn.commit()
            if sampling_method == 'bernoulli':
                outputs.append(round(output / sampling_probability ** len(table_names)))
            elif sampling_method == 'correlated':
                outputs.append(round(output / sampling_probability))
        except:
            print("error in execution of query")
            print(j)
            return outputs
        if j % 1 == 0:
            print(str(j) + ' sampled joined')

    sum = 0
    for i in range(number_of_samples):
        sum += outputs[i]

    average = sum / number_of_samples

    sum = 0
    for i in range(number_of_samples):
        sum += (outputs[i] - average) ** 2

    # FIX - for correct variance formula
    variance = sum / (number_of_samples)
    if testing_mode == 1:
        filename = output_dir + '/result ' + sampling_method + ' p=' + str(
            round(sampling_probability * small_table_probability, 5))
    elif testing_mode == 0:
        filename = output_dir + '/result ' + sampling_method + str(number_of_samples) + ' p=' + str(
            round(sampling_probability, 5))

    error = sqrt(variance) / actual_rows
    output_dic = {'method': str(sampling_method), 'table_names': str(table_names), 'attributes': str(attributes),
                  'sampling_probability': str(round(sampling_probability, 5)),
                  'number_of_samples': str(number_of_samples),
                  'estimates': str(outputs), 'average': str(average), 'variance': variance, 'error': error,
                  'plan_rows': plan_rows,
                  'actual_rows': actual_rows}
    if testing_mode == 1:
        output_dic['sampling_probability'] = str(round(sampling_probability * small_table_probability, 5))
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as outfile:
        json.dump(output_dic, outfile, indent=4)
        print("wrote the results to file")
    return output_dic['error']





"""
Gathering data for frequency histograms by sampling many times,
as well as relative error line graphs by varying sampling probability.
"""


def milestone_report():
    start = time.time()
    outputs_dic = {'bernoulli': {}, 'correlated': {}}
    errors_dic = {'bernoulli': [], 'correlated': []}
    conn = connect(param_dic)
    for method in ['bernoulli', 'correlated']:
        # print(method + ' sampling with p='+ str(sampling_probability) + ' for ' + str(number_of_samples) + ' samples')
        # sampler(table_names, attribute_indices, number_of_samples, method, sampling_probability)
        # copy_samples(conn, table_names, number_of_samples)
        # outputs_dic[method] = join(conn, output_dir, table_names, attributes, number_of_samples, sampling_probability, method)
        for probability in [0.0001, 0.0003, 0.0005, 0.001, 0.005, 0.01]:
            print(method + ' sampling with p=' + str(probability))
            sampler(table_names, attribute_indices, number_of_samples, method, probability)
            end = time.time()
            print('sampling took ' + str(end - start) + ' sec')
            copy_samples(conn, table_names, number_of_samples)
            errors_dic[method].append(join(conn, output_dir, table_names, attributes, number_of_samples,
                                           probability, method, 0))
    return errors_dic


outputs_dic = milestone_report()
# conn = connect(param_dic)
# sampler(table_names, attribute_indices, number_of_samples, 'correlated', sampling_probability)
# copy_samples(conn, table_names, number_of_samples)
# outputs_dic[method] = join(conn, output_dir, table_names, attributes, number_of_samples, sampling_probability, 'correlated')
