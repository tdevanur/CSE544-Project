from numpy import random
from math import floor
import csv
import time
from tables_as_lists import table_names, attributes, param_dic, sampling_probability, connect, sampling_method
import psycopg2
import os
import errno
from psycopg2.extensions import AsIs
import json


def join(output_dir, table_names, attributes, number_of_samples):
    conn = connect(param_dic)

    if len(table_names) <= 1:
        print('you need at least two tables!')
        return 1
    # elif len(table_names) == 2:
    #     join_query = """
    #             SELECT COUNT(*) FROM %(table1)s, %(table2)s WHERE %(table1)s.%(attribute1)s = %(table2)s.%(attribute2)s
    #             """
    #     # NOTE: remove this when consolidating program, use earlier connection
    #     cursor = conn.cursor()
    #     outputs = []
    #
    #     for i in range(number_of_samples):
    #         join_data = {'table1': AsIs(table_names[0] + str(i + 1)), 'table2': AsIs(table_names[1] + str(i + 1)),
    #                      'attribute1': AsIs(attributes[0]), 'attribute2': AsIs(attributes[1])}
    #         cursor.execute(join_query, join_data)
    #         output = (cursor.fetchall()[0])[0]
    #         outputs.append(output)
    #     print(join_query)

    explain_query = "EXPLAIN (FORMAT JSON, ANALYZE) SELECT * FROM "
    join_query_test = "SELECT COUNT(*) FROM "
    for i in range(len(table_names) - 1):
        join_query_test += table_names[i] + "%(index)s" + " t" + str(i) + ", "
        explain_query += table_names[i] + " t" + str(i) + ", "
    join_query_test += table_names[len(table_names) - 1] + "%(index)s" + " t" + str(len(table_names) - 1) + " WHERE "
    explain_query += table_names[len(table_names) - 1] + " t" + str(len(table_names) - 1) + " WHERE "
    for i in range(len(table_names) - 2):
        join_query_test += "t" + str(i) + "." + attributes[i] + " = " + "t" + str(i + 1) + "." + \
                           attributes[i + 1] + " and "
        explain_query += "t" + str(i) + "." + attributes[i] + " = " + "t" + str(i + 1) + "." + \
                         attributes[i + 1] + " and "
    join_query_test += "t" + str(len(table_names) - 2) + "." + attributes[len(table_names) - 2] + " = " + \
                       "t" + str(len(table_names) - 1) + "." + attributes[len(table_names) - 1]
    explain_query += "t" + str(len(table_names) - 2) + "." + attributes[len(table_names) - 2] + " = " + \
                       "t" + str(len(table_names) - 1) + "." + attributes[len(table_names) - 1]

    cursor = conn.cursor()
    outputs = []
    cursor.execute(explain_query)
    explanation = ((((cursor.fetchall())[0])[0])[0]['Plan'])
    plan_rows = explanation['Plan Rows']
    actual_rows = explanation['Actual Rows']



    for j in range(number_of_samples):
        try:
            cursor.execute(join_query_test, {"index": j + 1})
            output = (cursor.fetchall()[0])[0]
            outputs.append(output / (sampling_probability))
        except:
            print("error in execution of query")
            return 1


    sum = 0
    for i in range(number_of_samples):
        sum += outputs[i]
    average = sum / number_of_samples


    sum = 0
    for i in range(number_of_samples):
        sum += (outputs[i] - average)**2
    variance = sum / (number_of_samples - 1)

    filename = output_dir+'/result'
    output_dic = {'method': str(sampling_method), 'table_names': str(table_names), 'attributes': str(attributes),
                  'sampling_probability': str(sampling_probability), 'number_of_samples': str(number_of_samples),
                  'estimates': str(outputs), 'average': str(average), 'variance': variance, 'plan_rows': plan_rows,
                  'actual_rows': actual_rows}
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as outfile:
        json.dump(output_dic, outfile, indent = 4)
        print("wrote the results to file")
    return 0
