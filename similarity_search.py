from doctest import COMPARISON_FLAGS
import random
import numpy as np
import math
import hashlib
import csv
import sys
import datetime
from pprint import pprint
from pyspark import SparkContext, SparkConf

BANDS = 5
ROWS_PER_BAND = 100//BANDS

def hashBands(hospital_set):
    """
        Hashing all the signatures of a hospital_id within the band using LHS_HASH_FUNCTIONS.
    """
    l = []
    band = 0
    for i in range(0, 100, ROWS_PER_BAND):
        running_sum = 0
        for j in range(i, i+ROWS_PER_BAND):
            running_sum += hospital_set[1][j][1] 
        
        l.append(((LHS_HASH_FUNCTIONS[band](running_sum), band), hospital_set[0]))
        band+=1
    
    return l

def generateHashFunctions(num, mod):
    
    """
        Reference : https://stackoverflow.com/questions/2255604/hash-functions-family-generator-in-python
    """
    hashFunctions = []

    def getHashFunction(n):
        random.seed(n)
        
        val = random.getrandbits(32)

        def customHashFunction(x):
            return (hash(x) ^ val) % (mod)
        
        return customHashFunction
    
    for i in range(num):
        hashFunctions.append(getHashFunction(i))

    # return sc.broadcast(hashFunctions)
    return hashFunctions

def generateSimilarityRDD(x):
    """
        Converting sparse characteristic matrix to singature matrix using flatmap.
        Initial approach was to use flatmap and use reduceKey(min) operation, but that's spending a lot of time in shuffles
        and I avoided that by computing the minimum value across shingles within the transform function and populating the RDD.
    """
    l = []
    # signature_matrix = sparse_hospitals_rdd.flatMap(lambda x : transform(x))
    # .reduceByKey(min)
    for i in range(100):
        minVal = float('inf')
        attributeDict = x[1][0]
        for key, value in attributeDict.items():
            v = HASH_FUNCTIONS[i](key+":"+str(value))
            minVal = min(minVal, v)
            # l.append(((i, x[0]), HASH_FUNCTIONS[i](shingle)))
        l.append(((i, x[0]), minVal))
    return l

def cosine_similarity(list1, list2):
    l = min(len(list1), len(list2))
    l1 = list1 if len(list1) != l else list2
    l2 = list1 if len(list1) == l else list2
    running_sum = 0
    x1sqr, x2sqr = 0, 0
    for item1 in l1:
        for item2 in l2:
            if(item2[0] == item1[0]):
                running_sum += item1[1]*item2[1]
                x1sqr += item1[1]*item1[1]
                x2sqr += item2[1]*item2[1]
                break
    
    den1 = math.sqrt(x1sqr)
    den2 = math.sqrt(x2sqr)

    if den1 == 0 or den2 == 0:
        return 0

    return running_sum/(den1*den2)

def jacardSimilarity(similar_hosp_set, hos_id):
    """
        Using transformed Signature matrix of the form [(Hosp_id), [(0, Hash_0_Val), (1, Hash_1_Val), .....]]
    """
    d={}
    similar_hosp_set = set(similar_hosp_set)
    hos_id_list = transformed_signature_matrix1.filter(lambda x : x[0] == hos_id).map(lambda x : x[1]).collect()[0]
    similar_hosp_set_rdd = transformed_signature_matrix2.filter(lambda x : x[0] in similar_hosp_set)
  
    for target_hosp in similar_hosp_set:
        if target_hosp == hos_id:
            continue
        
        l = similar_hosp_set_rdd.filter(lambda x : x[0] == target_hosp).map(lambda x : x[1]).collect()[0]

        # hos_list_values = [tup[1] for tup in hos_id_list]
        # l_values = [tup[1] for tup in l]
        d[target_hosp] = cosine_similarity(hos_id_list, l)
    
    return d

if __name__ == '__main__':
    con = SparkConf().setAppName("Similar_Hospital_Search")
    sc = SparkContext(conf=con)
    sc.setLogLevel("WARN")

    HASH_FUNCTIONS = generateHashFunctions(100, 2**20 - 1)
    characteristicRDD = sc.pickleFile('characteristicRDD.pkl')

    state1 = "FL"
    state2 = "CA"
    county1 = "bradford"
    county2 = "san mateo"
    disaster1 = "hurricane"
    disaster2 = "flood"

    with open('similarity.txt', 'w') as f:
        
        characteristicRDDSub1 = characteristicRDD.filter(lambda x : x[0][0].lower() == county1 \
                                                and x[0][1] == state1 and x[0][4].lower() == disaster1)
        characteristicRDDSub2 = characteristicRDD.filter(lambda x : x[0][0].lower() == county2 \
                                                and x[0][1] == state2 and x[0][4].lower() == disaster2)


        # pprint(characteristicRDDSub2.collect()[:10])
        # exit(0)
        LHS_HASH_FUNCTIONS = generateHashFunctions(BANDS, 128)
        signatureMatrixRDD1 = characteristicRDDSub1.flatMap(generateSimilarityRDD)
        signatureMatrixRDD2 = characteristicRDDSub2.flatMap(generateSimilarityRDD)

        
        transformed_signature_matrix1 = signatureMatrixRDD1.map(lambda x : (x[0][0], (x[0][1], x[1])))\
                                            .sortByKey().map(lambda x : (x[1][0], (x[0], x[1][1])))\
                                            .groupByKey().mapValues(list)
        
        transformed_signature_matrix2 = signatureMatrixRDD2.map(lambda x : (x[0][0], (x[0][1], x[1])))\
                                            .sortByKey().map(lambda x : (x[1][0], (x[0], x[1][1])))\
                                            .groupByKey().mapValues(list)

        grouped_across_bands1 = transformed_signature_matrix1\
                                        .map(lambda x : hashBands(x))\
                                        .flatMap(lambda x : x)\
                                        .groupByKey().mapValues(list)
        grouped_across_bands2 = transformed_signature_matrix2\
                                        .map(lambda x : hashBands(x))\
                                        .flatMap(lambda x : x)\
                                        .groupByKey().mapValues(list)

        results = {}
        COMPARISON_LIST = transformed_signature_matrix1.keys().collect()

        for id in COMPARISON_LIST:
            
            similar_hopitals_list = transformed_signature_matrix2.keys().collect()            
            results[id] = jacardSimilarity(similar_hopitals_list, id)
        
        for key, value in results.items():
            pprint(key, f)
            pprint(value, f)
