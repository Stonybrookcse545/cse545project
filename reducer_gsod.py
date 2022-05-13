from pyspark import SparkContext
from pprint import pprint
import csv
from collections import defaultdict

sc = SparkContext(appName="GSOD")
climateRDD = sc.textFile('gsod-county-cleaned-2019.csv', 32)
climateGhcndRDD = sc.textFile('ghcnd-county-2019.csv', 32)
sc.setLogLevel("WARN")

required_keys = ['state', 'county', 'yearday', 'temp', 'dewp', 'slp', 'stp' , 'visib', 'wdsp', 'mxspd', 'gust', 'max', 'min', 'prcp' ,'sndp', 'frshtt']
filter_keys = ['state', 'county', 'yearday', 'prcp']
star = "*"

def process_row(row, headers):
    
    key_dict = dict()
    row_split = row.split(',')

    for value, key in zip(row_split, headers.value):
        if key not in required_keys:
            continue

        if star in value:
            value = value.replace(star, '') 

        key_dict[key] = value

    map_key = (key_dict['county'], key_dict['state'], key_dict['yearday'])

    return (map_key, (1, key_dict))



def combineAttributeAtCounty(row1, row2):
    """
    TODO : Filter prcp units and add an in clause for it.
    """
    dict1 = row1[1]
    dict2 = row2[1]

    ans_dict = dict()

    for key in dict1.keys():
        if key in filter_keys:
            ans_dict[key] = dict1[key]

        elif key == "max":
            ans_dict[key] = max(dict1[key], dict2[key])
        
        elif key == "min":
            ans_dict[key] = min(dict1[key], dict2[key])

        else:
            ans_dict[key] = float(dict1[key]) + float(dict2[key])

    return (row1[0] + row2[0], ans_dict)

def reduceAtCountyDaily(row):
    dict = row[1][1]

    ans_dict = defaultdict(float)

    for key in dict.keys():
        if key in filter_keys:
            ans_dict[key] = dict[key]
        elif key == "max" or key == "min":
            ans_dict[key] = dict[key]
        else:
            ans_dict[key] = float(dict[key])/row[1][0]
    
    return (row[0], ans_dict)

def meanCenterAtCounty(row):
    dict_list = row[1]
    dict_size = len(row[1])
    agg_dict = defaultdict(float)
    for dict in dict_list:
        for key in dict.keys():
            if key in filter_keys:
                continue
            else:
                agg_dict[key] += float(dict[key])
    
    agg_dict = {k : v/dict_size for k, v in agg_dict.items()}
    meancentered_dict = {}
    for dict in dict_list:
        d = defaultdict(float)
        date = dict['yearday']
        for key in dict.keys():
            if key in filter_keys:
                d[key] = dict[key]
            else:
                d["gsod_"+key] = float(dict[key]) - agg_dict[key]
        meancentered_dict[date] = d
    
    return (row[0], meancentered_dict)

def emitCountyKeys(row):
    date, attribute, county, state = row[0]
    count, s = row[1]

    avgForDay = round(s / count, 1)

    keyTuple = (county, state, attribute)
    valueTuple = ( [(date, avgForDay)] , (count, s) )

    return (keyTuple, valueTuple)
    
def emitMeanCenteredValues(row):
    county, state, attribute = row[0]
    listDatesValues = row[1][0]
    count,s = row[1][1]

    avgForAttribute = round(s/count , 1)

    finalEmitList = []

    for dateValuePair in listDatesValues:
        date, value = dateValuePair
        keyTuple = (county, state, date)
        valueTuple = (attribute, value-avgForAttribute)
        emitTuple = (keyTuple, [valueTuple])

        finalEmitList.append(emitTuple)
    
    return finalEmitList

def emitCountyState(row):

    county, state, date = row[0]
    attributeMeanCenteredValues = row[1]

    res = {}
    for amv in attributeMeanCenteredValues:
        attribute, value = amv[0], amv[1]
        
        if date not in res:
            res[date] = {}
        
        res[date][attribute] = value

    return ((county,state), res)


def mergeDictionaries(x,y):

    if not x:
        return y 
    
    if not y:
        return x 
    
    x.update(y)

    return x

def processLine(line, keys, values):
    
    res = []

    columns = list(csv.reader([line], delimiter=','))[0]

    size = len(columns)

    key = tuple()

    for i in range(len(columns)):

        if i in keys:
            key += tuple([columns[i]])
        
        if i in values:
            value = int(columns[i])
            
    
    return ( key, (1,value) )

def mergeDictionaries_1(dict1, dict2):
    ans_dict = {}

    for key in dict1.keys():
        v1 = dict1[key]
        v2 = (dict2[key] if key in dict2 else None)

        ans_dict[key] = mergeDictionaries(v1, v2)
    
    return ans_dict


"""
    key, value: key = (county, state) value = dict(mean_centered_attribute values for that county)
"""

with open('result.txt', 'w') as f:
    
    headers = climateRDD.first()
    headerList = headers.split(",")
    headerList = sc.broadcast(headerList)

    climateRDD = climateRDD.filter(lambda x : x!=headers)

    climateRDD = climateRDD.map(lambda x : process_row(x, headerList))\
                            .reduceByKey(lambda x, y : combineAttributeAtCounty(x, y))\
                            .map(reduceAtCountyDaily)\
                            .map(lambda x : ((x[0][0], x[0][1]), [x[1]]))\
                            .reduceByKey(lambda x, y : x + y)\
                            .map(meanCenterAtCounty)

    # l = climateRDD.take(1)
    # pprint(l, f)

    headers = climateGhcndRDD.first()
    headerList = headers.split(",")
    headerList = sc.broadcast(headerList)

    keys = ['state', 'county', 'yearday','attribute']
    values = ['value']
    keyOrdinals = []
    valueOrdinals = []

    for i in range(len(headerList.value)):
    
        if headerList.value[i] in keys:
            keyOrdinals.append(i)
        
        if headerList.value[i] in values:
            valueOrdinals.append(i)
    
    climateGhcndRDD = climateGhcndRDD.filter(lambda line: line != headers)\
                            .filter(lambda line: len(line.split(',')[keyOrdinals[1]]) > 0)
    
    climateGhcndRDD = climateGhcndRDD.map(lambda line: processLine(line, keyOrdinals, valueOrdinals))

    climateGhcndRDD = climateGhcndRDD.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                                    .map(emitCountyKeys)\
                                    .reduceByKey(lambda x,y: ( x[0]+y[0] ,  ( x[1][0]+y[1][0], x[1][1]+y[1][1]  ) ) )\
                                    .flatMap(emitMeanCenteredValues)\
                                    .reduceByKey(lambda x,y: x+y)\
                                    .map(emitCountyState)\
                                    .reduceByKey(lambda x,y: mergeDictionaries(x,y))
    
    # pprint(climateGhcndRDD.take(1), f)
    
    finalRdd = climateRDD.union(climateGhcndRDD).reduceByKey(lambda x, y : mergeDictionaries_1(x, y))

    pprint(finalRdd.take(1), f)