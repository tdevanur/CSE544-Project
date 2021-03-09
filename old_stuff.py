from numpy import random
from math import floor
import csv
import time
import psycopg2
import os
import errno
from psycopg2.extensions import AsIs
import re
from os import listdir
from os.path import isfile, join, abspath
import glob
import json
import matplotlib.pyplot as plt
import old_stuff

table_names = ['title', 'movie_keyword', 'cast_info']
attributes = ['id', 'movie_id', 'movie_id']
number_of_samples = 250
attribute_indices = [0, 1, 1]


def import_tables():
    tables = {}
    for i in table_names:
        # defines path to file
        file_path = original_tables_dir + i + '.csv'

        # read full table into a list
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            print('file has been read')
            tables[i] = list(reader)
        print('table has been filled')
    return tables


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


# this is to sample without loading entire tables
def test2():
    start = time.time()


    conn = connect(param_dic)

    for i in table_names:
        print(i)
        copy_samples(conn, '/home/tejas/Desktop/dbms/Project/samples/' + i, number_of_samples)

    end = time.time()
    print('this program took ' + str(end - start) + ' seconds')
    # print(average_sizes)
    return conn



def import_table(file_path):
    # read full table into a list
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        print('file has been read')
        table = list(reader)
    print('table has been filled')
    return table


def sample_table(source_path, target_path, p):
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(source_path, mode='r') as source_file:
        reader = csv.reader(source_file)
        with open(target_path, "w", newline="") as target_file:
            writer = csv.writer(target_file)
            for i in reader:
                random_number = random.uniform(low=0.0, high=1.0)
                if random_number < p:
                    writer.writerow(i)
    return 0

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




def bernoulli2(table_names, attribute_indices, number_of_samples):
    hash_parameters = []
    source_files = []
    target_files = [[] for j in range(number_of_samples)]
    sample_writers = [[] for j in range(number_of_samples)]
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
    print("files and directories created")

    for i in range(len(table_names)):
        print(table_names[i])
        attribute_index = attribute_indices[i]
        table = open(source_files[i], 'r')
        table_reader = csv.reader(table)
        temp_counter = 0
        for row in table_reader:
            temp_counter += 1
            if temp_counter % 100000 == 0:
                print(temp_counter)
            for j in range(number_of_samples):
                a = hash_parameters[j][0]
                b = hash_parameters[j][1]
                hash_of_row = uniform_hash(a,b,temp_counter)
                # random_number = random.uniform(0, 1)
                if hash_of_row < sampling_probability:
                    sample_writers[j][i].writerow(row)
        table.close()



def get_small_tables():
    # delete old small tables to make room for new ones
    old_small_tables = glob.glob(small_tables_dir + '*')
    for old_small_table in old_small_tables:
        os.remove(old_small_table)

    # get list of csv names of original tables
    all_tables = ['title.csv', 'movie_keyword.csv', 'cast_info.csv']

    # seperate out the type tables to copy whole without sampling
    type_tables = [f for f in all_tables if 'type' in f]

    for csv_name in all_tables:
        print(csv_name)
        source_path = original_tables_dir + csv_name
        target_path = small_tables_dir + csv_name
        if csv_name in type_tables:
            sample_table(source_path, target_path, 1)
        else:
            sample_table(source_path, target_path, small_table_probability)


def copy_small_tables():
    small_tables = os.listdir(small_tables_dir)
    for i in range(len(small_tables)):
        file = small_tables[i]
        if os.path.basename(file)[0] == '.':
            del small_tables[i]

    drop_query = """
                DROP TABLE IF EXISTS %(table)s
                """

    query = """
                CREATE TABLE %(new_table)s ( like %(base_table)s including all)
                """
    conn = connect(param_dic)
    with conn.cursor() as cursor:
        for small_table in small_tables:
            print(small_table)
            name = os.path.basename(small_table).split('.')[0]

            cursor.execute(drop_query, {'table': AsIs('small' + str(int(small_table_probability * 100)) + '_' + name)})
            conn.commit()
            cursor.execute(query,
                           {'new_table': AsIs('small' + str(int(small_table_probability * 100)) + '_' + name),
                            'base_table': AsIs(''.join(i for i in name if not i.isdigit()))})
            conn.commit()
            print('test')
            sqlstr = "COPY small" + str(
                int(small_table_probability * 100)) + "_" + name + " FROM STDIN DELIMITER ',' CSV"
            with open(small_tables_dir + name + '.csv', 'r') as fin:
                try:
                    cursor.copy_expert(sqlstr, fin)
                    conn.commit()
                    print('Data inserted into ', name, ' complete')
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
                    conn.rollback()
                    cursor.close()
                    return 1

def test(method, p):
    sampling_method = method
    sampling_probability = p
    start = time.time()
    if sampling_method == 'bernoulli':
        if testing_mode == 0:
            writer(table_names,attribute_indices,number_of_samples)
            # for i in table_names:
            #     file_path = original_tables_dir + i + '.csv'
            #     cache = import_table(file_path)
            #     for j in range(number_of_samples):
            #         print(j + 1)
            #         bernoulli_writer(cache, i, j + 1)
        elif testing_mode == 1:
            for table_name in table_names:
                source_path = original_tables_dir + table_name + '.csv'
                for sample_index in range(number_of_samples):
                    print(sample_index + 1)
                    target_path = destination + table_name + '/' + table_name + str(sample_index + 1) + '.csv'
                    sample_table(source_path, target_path, sampling_probability)
    elif sampling_method == 'correlated':
        writer(table_names, attribute_indices, number_of_samples)
    conn = connect(param_dic)

    for i in table_names:
        print(i)
        copy_samples(conn, '/home/tejas/Desktop/dbms/Project/samples/' + i, number_of_samples)

    end = time.time()
    print('this program took ' + str(end - start) + ' seconds')
    # print(average_sizes)

if testing_mode == 2:
    original_tables_dir = '/home/tejas/Desktop/dbms/Project/tables/'
    small_tables_dir = '/home/tejas/Desktop/dbms/Project/smalltables/' + str(int(small_table_probability * 100)) + '/'
    confirmation = input("Are you sure you want to resample small tables?")
    if confirmation == 'y':
        get_small_tables()
        copy_small_tables()
    else:
        testing_mode = 1

def clear_tables(table_names, number_of_files):
    conn = connect(param_dic)
    drop_query = """
            DROP TABLE IF EXISTS %(table)s
            """
    for table_name in table_names:
        with conn.cursor() as cursor:
            for i in range(number_of_files):
                name = table_name + str(i)
                print(name)
                cursor.execute(drop_query, {'table': AsIs(name)})
                conn.commit()
    return 1


"""
    explain_query = "EXPLAIN (FORMAT JSON, ANALYZE) SELECT * FROM "
    for i in range(len(table_names) - 1):
        if testing_mode == 1:
            explain_query += 'small' + str(int(small_table_probability)) + '_' + table_names[i] + " t" + str(
                i) + ", "
        elif testing_mode == 0:
            explain_query += table_names[i] + " t" + str(i) + ", "
    if testing_mode == 1:
        explain_query += 'small' + str(int(small_table_probability)) + '_' + table_names[
            len(table_names) - 1] + " t" + str(len(table_names) - 1) + " WHERE "
    elif testing_mode == 0:
        explain_query += table_names[len(table_names) - 1] + " t" + str(len(table_names) - 1) + " WHERE "
    for i in range(len(table_names) - 2):
        explain_query += "t" + str(i) + "." + attributes[i] + " = " + "t" + str(i + 1) + "." + \
                         attributes[i + 1] + " and "
    explain_query += "t" + str(len(table_names) - 2) + "." + attributes[len(table_names) - 2] + " = " + \
                     "t" + str(len(table_names) - 1) + "." + attributes[len(table_names) - 1]
    
    # if you need to run the explain query, e.g. if it's a completely new query
    if no_explanation == 1:
        cursor.execute(explain_query)
        explanation = ((((cursor.fetchall())[0])[0])[0]['Plan'])
        plan_rows = explanation['Plan Rows']
        actual_rows = explanation['Actual Rows']
"""