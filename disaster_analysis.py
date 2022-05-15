##########################################################################
##
## Original Code written for SBU's Big Data Analytics Project 
##
## Student Name: Sai Bhargav Varanasi
## Student ID: 114707860
## Student Name: Aniket Panda
## Student ID: 114356301
## Student Name: Akash Sateesh
## Student ID: 113221752
## Student Name: Priyanka Amol Dighe
## Student ID: 113264191

import time
import json
import numpy as np
import sys
from pyspark import SparkContext, SparkConf
from pprint import pprint
import math
import csv

DISASTERS_HEADER_DICT = {}

def processLinesUtil(lines_to_be_processed):
    """
      Processing the input lines across partitions using mappartitions.  
    """
    ans = []
    for line in lines_to_be_processed:
        line = list(csv.reader([line], delimiter=','))
        ans.append(line[0])
    return ans

def process_disaster_file(row, headers, required_rows):
    key_dict = dict()

    row_split = row.split(',')

    # print(type(headers.value))

    for value, key in zip(row_split, headers.value):
        # print(key,value)
        if key not in required_rows.value:
            continue

        if key == 'designated_area':
            value =value.replace(' (County)', '')

        if key == 'place_code' and str(value) == '0':
            return
        key_dict[key] = value

    # print(key_dict)
    map_key = (key_dict['state'], key_dict['fy_declared'])

    return (map_key, [key_dict])

if __name__ == '__main__':
    
    file = sys.argv[1]
    output_file = sys.argv[2]
    con = SparkConf().setAppName("Disasters_Analysis")
    sc = SparkContext(conf=con)
    sc.setLogLevel("WARN")

    """
        https://www.washingtonpost.com/climate-environment/2022/01/05/climate-disasters-2021-fires/

        Used for filtering states
    """
    states = ["CA", "FL", "GA", "NC", "TX", "WA", "NY"]

    required_rows = sc.broadcast(['state', 'declaration_date', 'incident_type', 'fy_declared', \
         'incident_begin_date', 'incident_end_date', 'place_code', 'designated_area'])

    disastersRDD = sc.textFile(file, 32)
    # fipsRDD = sc.textFile(fips_file, 32)
    
    header_1 = disastersRDD.first()
    # header_2 = fipsRDD.first()
    header_list = header_1.split(",")
    index = 0
    for col in header_list:
        DISASTERS_HEADER_DICT[col]=index
        index+=1
    
    disastersRDD = disastersRDD.filter(lambda x : x!=header_1)
    # fipsRDD = fipsRDD.filter(lambda x : x!=header_2)

    disastersRDD = disastersRDD.mapPartitions(lambda x : processLinesUtil(x))
    # fipsRDD = fipsRDD.mapPartitions(lambda x : processLinesUtil(x))

    with open(output_file, "w+") as OUTPUT_FILE:

        disastersRDD = disastersRDD.map(lambda x : (x[DISASTERS_HEADER_DICT['state']], x[DISASTERS_HEADER_DICT['fips']],
                                            x[DISASTERS_HEADER_DICT['fy_declared']],  x[DISASTERS_HEADER_DICT['incident_type']],
                                            x[DISASTERS_HEADER_DICT['incident_begin_date']], x[DISASTERS_HEADER_DICT['incident_end_date']], 
                                            x[DISASTERS_HEADER_DICT['place_code']], x[DISASTERS_HEADER_DICT['declaration_type']],
                                            x[DISASTERS_HEADER_DICT['designated_area']]))\
                                    .filter(lambda x : x[7] != "EM" and x[8]!= "Statewide")

        pprint("*********** DISASTERS **************", OUTPUT_FILE)
        disastersRDD = disastersRDD.filter(lambda x : x[0] in states)
        pprint(disastersRDD.take(20), OUTPUT_FILE)

        pprint(disastersRDD.filter(lambda x : x[2] == "2018")\
                        .map(lambda x : ((x[8], x[0]), [x]))\
                        .reduceByKey(lambda x, y : x + y)\
                        .mapValues(lambda x : len(x))\
                        .sortBy(lambda x : x[1], ascending=False)
                        .take(100), OUTPUT_FILE)
                            


    sc.stop()
