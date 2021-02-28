from numpy import random
from math import floor
import csv
import time
import psycopg2
import os
import errno
from psycopg2.extensions import AsIs
import re

LARGE_PRIME = 1000000007
table_names = ['title', 'movie_keyword', 'cast_info']
attributes = ['id', 'movie_id', 'movie_id']
number_of_samples = 10

attribute_indices = [0, 1, 1]

sampling_method = 'correlated'

if len(table_names) != len(attributes):
    print("The attributes don't match the tables!")

# location of csv files
location = '/home/tejas/Desktop/dbms/Project/tables/'
output_dir = '/home/tejas/Desktop/dbms/Project/results/Bernoulli'

# sampling probability
sampling_probability = 0.01

# destination folder for samples
destination = '/home/tejas/Desktop/dbms/Project/samples/'

param_dic = {
    "host": "localhost",
    "database": "imdbload",
    "user": "postgres",
    "password": "tejas"
}


# for sorting

def natural_key(string_):
    """See https://blog.codinghorror.com/sorting-for-humans-natural-sort-order/"""
    return [int(s) if s.isdigit() else s for s in re.split(r'(\d+)', string_)]


def import_tables():
    tables = {}
    for i in table_names:
        # defines path to file
        file_path = location + i + '.csv'

        # read full table into a list
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            print('file has been read')
            tables[i] = list(reader)
        print('table has been filled')
    return tables


def import_table(file_path):
    # read full table into a list
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        print('file has been read')
        table = list(reader)
    print('table has been filled')
    return table


def sample_table(source_path, table_name, sample_index):
    target_path = destination + table_name + '/' + table_name + str(sample_index) + '.csv'
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(source_path, mode='r') as source_file:
        reader = csv.reader(source_file)
        with open(target_path, "w", newline="") as target_file:
            writer = csv.writer(target_file)
            for i in reader:
                random_number = random.uniform(low=0.0, high=1.0)
                if random_number < sampling_probability:
                    writer.writerow(i)
    return 0


def bernoulli_sampler(table):
    # this array will be populated with the sample
    bernoulli_sample = []

    table_length = len(table)

    # define an array of samples from Bernoulli distribution
    random_numbers = random.uniform(low=0.0, high=1.0, size=table_length)

    for i in range(table_length):
        if random_numbers[i] < sampling_probability:
            bernoulli_sample.append(table[i])

    return bernoulli_sample


def bernoulli_writer(table, table_name, sample_index):
    table_length = len(table)
    random_numbers = random.uniform(low=0.0, high=1.0, size=table_length)

    filename = destination + table_name + '/' + table_name + str(sample_index) + '.csv'

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(table_length):
            if random_numbers[i] < sampling_probability:
                writer.writerow(table[i])
    return 0


# def correlated_writer(table, table_name, attribute_index, sample_index, values_dic):
#     table_length = len(table)
#
#     filename = destination + table_name + '/' + table_name + str(sample_index) + '.csv'
#     os.makedirs(os.path.dirname(filename), exist_ok=True)
#     with open(filename, "w", newline="") as f:
#         writer = csv.writer(f)
#         for i in range(table_length):
#             this_value = table[i][attribute_index]
#             if this_value not in values_dic:
#                 values_dic[this_value] = random.uniform(low=0.0, high=1.0)
#             if values_dic[this_value] < sampling_probability:
#                 writer.writerow(table[i])
#     return values_dic

def uniform_hash(a,b,value):
    return ((a*value+b) % LARGE_PRIME)/LARGE_PRIME


def correlated_writer(table_names, attribute_indices, number_of_samples):

    hash_parameters = []
    source_files = []
    target_files = [[] for j in range(number_of_samples)]
    sample_writers = [[] for j in range(number_of_samples)]
    for table_name in table_names:
        source_files.append(location + table_name + '.csv')
    for sample_index in range(number_of_samples):
        a = random.randint(low=0, high=LARGE_PRIME)
        b = random.randint(low=0, high=LARGE_PRIME)
        hash_parameters.append([a,b])
        for i in range(len(table_names)):
            table_name=table_names[i]
            target_file = destination + table_name + '/' + table_name + str(sample_index) + '.csv'
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            target_files[sample_index].append(target_file)
            sample = open(target_files[sample_index][i], 'w')
            sample_writers[sample_index].append(csv.writer(sample))
    print("files and directories created")

    for i in range(len(table_names)):
        print(i)
        attribute_index = attribute_indices[i]
        table = open(source_files[i], 'r')
        table_reader = csv.reader(table)
        temp_counter = 0
        for row in table_reader:
            temp_counter += 1
            if temp_counter % 100000 == 0:
                print(temp_counter)
            join_value = int(row[attribute_index])
            for j in range(number_of_samples):
                a = hash_parameters[j][0]
                b = hash_parameters[j][1]
                hash_of_join_value = uniform_hash(a, b, join_value)
                if hash_of_join_value < sampling_probability:
                    sample_writers[j][i].writerow(row)






param_dic = {
    "host": "localhost",
    "database": "imdbload",
    "user": "postgres",
    "password": "tejas"
}


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


def copy_samples(conn, dir, number_of_files):
    """
    Here we are going save the dataframe on disk as
    a csv file, load the csv file
    and use copy_from() to copy it to the table
    """
    sec_files = sorted(os.listdir(dir), key=natural_key)
    print(sec_files)
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

            cursor.execute(query,
                           {'new_table': AsIs(name), 'base_table': AsIs(''.join(i for i in name if not i.isdigit()))})

            sqlstr = "COPY " + name + " FROM STDIN DELIMITER ',' CSV"
            with open(dir + '/' + name + '.csv', 'r') as fin:
                try:
                    cursor.copy_expert(sqlstr, fin)
                    conn.commit()
                    print('Data inserted into ', name, ' complete')
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
                    conn.rollback()
                    cursor.close()
                    return 1


# this loads entire tables
def test():
    start = time.time()
    if sampling_method == 'bernoulli':
        for i in table_names:
            file_path = location + i + '.csv'
            cache = import_table(file_path)
            for j in range(number_of_samples):
                print(j + 1)
                bernoulli_writer(cache, i, j + 1)
            cache = []

    elif sampling_method == 'correlated':
        correlated_writer(table_names,attribute_indices,number_of_samples)
    conn = connect(param_dic)

    for i in table_names:
        print(i)
        copy_samples(conn, '/home/tejas/Desktop/dbms/Project/samples/' + i, number_of_samples)

    end = time.time()
    print('this program took ' + str(end - start) + ' seconds')
    # print(average_sizes)
    return conn


# this is to sample without loading entire tables
def test2():
    start = time.time()

    samples = {}
    average_sizes = {}

    for i in table_names:
        file_path = location + i + '.csv'
        for j in range(number_of_samples):
            print(j + 1)
            sample_table(file_path, i, j + 1)

    conn = connect(param_dic)

    for i in table_names:
        print(i)
        copy_samples(conn, '/home/tejas/Desktop/dbms/Project/samples/' + i, number_of_samples)

    end = time.time()
    print('this program took ' + str(end - start) + ' seconds')
    # print(average_sizes)
    return conn
