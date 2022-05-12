from typing import OrderedDict
from pyspark import SparkContext
from pprint import pprint
import csv
from collections import defaultdict
from dateutil import parser

sc = SparkContext(appName="PythonStreamingQueueStream")
sc.setLogLevel("WARN")


#CLIMATE RDD methods
climateRDD = sc.textFile('ghcnd-county-2019.csv', 32)

def processClimateLine(line, keys, values):
    columns = list(csv.reader([line], delimiter=','))[0]
    key = tuple()

    for i in range(len(columns)):
        if i in keys:
            key += tuple([columns[i]])
        
        if i in values:
            value = int(columns[i])
            
    return ( key, (1,value) )

#for Climate data
def emitCountyKeys(row):
    date, attribute, county, state = row[0]
    county = county.lower().replace(' county', '')
    count, s = row[1]

    avgForDay = round((s / count)*2)/2

    keyTuple = (county, state, attribute)
    valueTuple = ( [(date, avgForDay)] , (count, s) )

    return (keyTuple, valueTuple)

#for Climate data
def emitMeanCenteredValues(row):
    county, state, attribute = row[0]
    listDatesValues = row[1][0]
    count,s = row[1][1]

    avgForAttribute = round((s / count)*2)/2

    finalEmitList = []

    for dateValuePair in listDatesValues:
        date, value = dateValuePair
        keyTuple = (county, state, date)
        valueTuple = (attribute, value-avgForAttribute)
        emitTuple = (keyTuple, [valueTuple])

        finalEmitList.append(emitTuple)
    
    return finalEmitList

#for Climate data - make dictionary of key,value pairs of attributes
def emitDictKeys(row):
    county, state, date = row[0]
    attributeDict = dict(row[1])
    
    keyTuple = (county, state)
    
    date = parser.parse(date)
    valueTuple = (date, attributeDict)

    return (keyTuple, [valueTuple])



#CLIMATE RDD manipulations
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

climateRDD = climateRDD.map(lambda line: processClimateLine(line, keyOrdinals, valueOrdinals))

# at end of this
# key, value: key = (county, state, date) value = list((attribute, mean-centered-value))
climateRDD = climateRDD.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                .map(emitCountyKeys)\
                .reduceByKey(lambda x,y: ( x[0]+y[0] ,  ( x[1][0]+y[1][0], x[1][1]+y[1][1]  ) ) )\
                .flatMap(emitMeanCenteredValues)\
                .reduceByKey(lambda x,y: x+y)\
                .map(emitDictKeys)\
                .reduceByKey(lambda x,y: x+y)

# pprint(climateRDD.take(1))






#DISASTER RDD methods
#For disaster data - process each row of disaster and 
def process_disaster_file(row, headers, required_cols):
    
    key_dict = dict()
    row_split = list(csv.reader([row], delimiter=','))[0]

    for value, key in zip(row_split, headers.value):
        # print(key,value)
        if key not in required_cols.value:
            continue

        if key == 'designated_area':
            value =value.replace(' (County)', '').lower()
        
        if 'date' in key:
            value = value.split('T')[0]
            value = value.replace('-','')

        # if key == 'place_code' and str(value) == '0':
        #     return
        key_dict[key] = value

    # print(key_dict)
    map_key = (key_dict['designated_area'], key_dict['state'], key_dict['fy_declared'])
    
    return (map_key, key_dict)

#helper method for emitCountyDisasterRange
def notValidDate(dateString):

    #date string should be 6 characters
    if len(dateString) != 6 or not dateString.isdigit():
        return True

    return False

#disaster RDD method - emit only county and distater + date range
def emitCountyDisasterRange(row):
    county, state, _ = row[0]
    detailsDict = row[1]

    startDate = detailsDict['incident_begin_date']
    endDate = detailsDict['incident_end_date']
    reportedDate = detailsDict['declaration_date']

    #if either start or end date is not reported
    if notValidDate(startDate) and notValidDate(endDate):
        startDate = endDate = reportedDate
    elif notValidDate(startDate) and notValidDate(endDate):
        startDate = reportedDate
    elif notValidDate(startDate) and notValidDate(endDate):
        endDate = startDate
    
    detailsDict['incident_begin_date'] = parser.parse(startDate)
    detailsDict['incident_end_date'] = parser.parse(endDate)
    detailsDict['declaration_date'] = parser.parse(reportedDate)

    # try:
    #     detailsDict['incident_begin_date'] = parser.parse(startDate)
    #     detailsDict['incident_end_date'] = parser.parse(endDate)
    #     detailsDict['declaration_date'] = parser.parse(reportedDate)
    # except:
    #     print("ERROR IN DATE PARSING OF DISASTER" + str(detailsDict))

    disasterType = detailsDict['incident_type']

    keyTuple = (county, state)
    valueTuple = (disasterType, detailsDict)

    return (keyTuple, [valueTuple])


#DISASTER RDD manipulation
disaterRDD = sc.textFile('us_disaster_declarations.csv', 32)

headers = disaterRDD.first()
disaterRDD = disaterRDD.zipWithIndex().filter(lambda row_index: row_index[1] > 0).keys()

headers = headers.split(',')
headers = sc.broadcast(headers)

required_cols = sc.broadcast(['state', 'declaration_date', 'incident_type', 'fy_declared', \
        'incident_begin_date', 'incident_end_date', 'place_code', 'designated_area'])

disaterRDD = disaterRDD.map(lambda row :process_disaster_file(row, headers, required_cols))\
    .filter(lambda row: row[0][2] == '2019')\
    .map(emitCountyDisasterRange)\
    .reduceByKey(lambda x,y: x+y)

# print(disaterRDD.collect()[0])



#climatedisasterRDD method - emit key-value pairs with 
def emitCountyAttrDisasterPairs(row):
    
    finalEmitList = []

    county, state = row[0]
    climateList = row[1][0]
    disasterList = row[1][1]

    # (date, attributeDict) - for climate attributeDict = value for temp etc
    # (disasterType, detailsDict) - for disaster detailsDict = details like startdate,etc
    for date, attributeDict in climateList:
        
        # flag to check if any disaster was matched
        disasterFlag = 0

        for disasterType, detailsDict in disasterList:
            
            startDate = detailsDict['incident_begin_date']
            endDate = detailsDict['incident_end_date']

            if startDate <= date <= endDate:
                disasterFlag = 1
                keyTuple = (county, state , date)
                valueTuple = (attributeDict, True, disasterType)
                emitPair = (keyTuple, valueTuple)

                finalEmitList.append(emitPair)
        
        #meaning not mapped to any disaster - good right ? :)
        if disasterFlag == 0:
            keyTuple = (county, state , date)
            valueTuple = (attributeDict, False, '')
            emitPair = (keyTuple, valueTuple)

            finalEmitList.append(emitPair)
    
    return finalEmitList


#CLIMATE-DISASTER-RDD manipulations
#merging climate and disaster RDD

climateDisasterRDD = climateRDD.join(disaterRDD)

climateDisasterRDD = climateDisasterRDD.flatMap(emitCountyAttrDisasterPairs)

# climateDisasterRDD.saveAsPickleFile('climateDisasterRDD')

item1 = climateDisasterRDD.collect()[0:1]

# FINAL OUTPUT - key(county, state, date) value(attributeDict, disasterY/N, disasterType)
print(item1)