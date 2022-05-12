from pyspark import SparkContext

from pprint import pprint

import csv

from collections import defaultdict

sc = SparkContext(appName="PythonStreamingQueueStream")

climateRDD = sc.textFile('ghcnd-county-2019.csv', 32)

climateGSODRDD = sc.textFile("gsod-county-cleaned-2019.csv",32)

sc.setLogLevel("WARN")

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

with open('result.txt', 'w') as f:

    headers = climateRDD.first()

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

    climateRDD = climateRDD.filter(lambda line: line != headers)\
                            .filter(lambda line: len(line.split(',')[keyOrdinals[1]]) > 0)
    
    climateRDD = climateRDD.map(lambda line: processLine(line, keyOrdinals, valueOrdinals))

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


    def mergeCountys(x,y):

        if not x:
            return y 
        
        if not y:
            return x 
        
        x.update(y)

        return x


# at end of this
# key, value: key = (county, state, date) value = list((attribute, mean-centered-value))
    climateRDD = climateRDD.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                    .map(emitCountyKeys)\
                    .reduceByKey(lambda x,y: ( x[0]+y[0] ,  ( x[1][0]+y[1][0], x[1][1]+y[1][1]  ) ) )\
                    .flatMap(emitMeanCenteredValues)\
                    .reduceByKey(lambda x,y: x+y)\
                    .map(emitCountyState)\
                    .reduceByKey(lambda x,y: mergeCountys(x,y))
    
    pprint(climateRDD.take(1), f)

    pprint(climateRDD.count())
    
