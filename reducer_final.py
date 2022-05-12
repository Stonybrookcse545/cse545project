from pyspark import SparkContext
from pprint import pprint
import csv
from collections import defaultdict
from dateutil import parser

sc = SparkContext(appName="GSOD")
climateRDD = sc.textFile('gsod-county-cleaned-2017.csv', 32)
climateGhcndRDD = sc.textFile('ghcnd-county-2017.csv', 32)
sc.setLogLevel("WARN")

required_keys = ['state', 'county', 'yearday', 'temp', 'dewp', 'slp', 'stp' , 'visib', 'wdsp', 'mxspd', 'gust', 'max', 'min', 'sndp']
filter_keys = ['state', 'county', 'yearday']
star = "*"

def process_row(row, headers):
    
    key_dict = dict()
    row_split = row.split(',')

    for value, key in zip(row_split, headers.value):
        if key not in required_keys:
            continue

        if star in value:
            value = value.replace(star, '') 

        if key == 'county':
            value = value.lower().replace(' county', '')

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
    
    agg_dict = {k : v / dict_size for k, v in agg_dict.items()}
    meancentered_dict = {}
    for dict in dict_list:
        d = defaultdict(float)
        date = dict['yearday']
        for key in dict.keys():
            if key in filter_keys:
                d[key] = dict[key]
            else:
                d["gsod_"+key] = round((float(dict[key]) - agg_dict[key])*2)/2
        meancentered_dict[date] = d
    
    return (row[0], meancentered_dict)

def emitCountyKeys(row):
    date, attribute, county, state = row[0]
    count, s = row[1]

    avgForDay = round((s / count)*2)/2

    keyTuple = (county, state, attribute)
    valueTuple = ( [(date, avgForDay)] , (count, s) )

    return (keyTuple, valueTuple)
    
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

def processLine(line, headerList):
    
    columns = list(csv.reader([line], delimiter=','))[0]

    keyTuple = tuple()
    valueTuple = tuple()

    for value, key in zip(columns, headerList.value):

        if key in ['state', 'county', 'yearday','attribute']:
            
            if key == 'county':
                value = value.lower().replace(' county', '')
            
            keyTuple += tuple([value])
        
        elif key == 'value':
            valueTuple = int(value)

    if len(keyTuple) < 4:
        print(keyTuple)

    return ( keyTuple, (1,valueTuple) )

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
countyOrdinal = 0

filterKeys = ['PRCP','SNOW','SNWD','TMAX','TMIN']

for i in range(len(headerList.value)):

    if headerList.value[i] in keys:
        keyOrdinals.append(i)
    
    if headerList.value[i] == 'county':
        countyOrdinal = i

    if headerList.value[i] in values:
        valueOrdinals.append(i)

climateGhcndRDD = climateGhcndRDD.filter(lambda line: line != headers)\
                                 .filter(lambda line: len ( list(csv.reader([line], delimiter=','))[0][keyOrdinals[1]] ) > 0)                                   

climateGhcndRDD = climateGhcndRDD.map(lambda line: processLine(line, headerList))\
                                 .filter(lambda x: x[0][1] in ['PRCP','SNOW','SNWD','TMAX','TMIN'] and len(x[0][2]) > 0)                                  

climateGhcndRDD = climateGhcndRDD.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                                .map(emitCountyKeys)\
                                .reduceByKey(lambda x,y: ( x[0]+y[0] ,  ( x[1][0]+y[1][0], x[1][1]+y[1][1]  ) ) )\
                                .flatMap(emitMeanCenteredValues)\
                                .reduceByKey(lambda x,y: x+y)\
                                .map(emitCountyState)\
                                .reduceByKey(lambda x,y: mergeDictionaries(x,y))

# pprint(climateGhcndRDD.take(1), f)

finalRdd = climateRDD.union(climateGhcndRDD).reduceByKey(lambda x, y : mergeDictionaries_1(x, y))



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
    .filter(lambda row: row[0][2] == '2017')\
    .map(emitCountyDisasterRange)\
    .reduceByKey(lambda x,y: x+y)

# print(disaterRDD.collect()[0])

# key, value: key = (county, state) value = dict(mean_centered_attribute values for that county)
def emitMap(row):

    county, state = row[0]
    dateAttributeDict = row[1]

    #list(dateAttributeDict.items())
    resultDict = {}

    for k,v in dateAttributeDict.items():

        # k -> date
        # v -> attribute dict
        parsedDate = parser.parse(k)
        resultDict[parsedDate] = v

    
    return ( ( county, state ),  list(resultDict.items()))


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
                keyTuple = (county, state)
                valueTuple = (date, attributeDict, True, disasterType)
                emitPair = (keyTuple, [valueTuple])

                finalEmitList.append(emitPair)
        
        #meaning not mapped to any disaster - good right ? :)
        if disasterFlag == 0:
            keyTuple = (county, state)
            valueTuple = (date, attributeDict, False, '')
            emitPair = (keyTuple, [valueTuple])

            finalEmitList.append(emitPair)
    
    return finalEmitList


#CLIMATE-DISASTER-RDD manipulations
#merging climate and disaster RDD

#pprint(disaterRDD.take(1))

finalRdd = finalRdd.map(emitMap)

climateDisasterRDD = finalRdd.join(disaterRDD)

# pprint(climateDisasterRDD.take(1))

climateDisasterRDD = climateDisasterRDD.flatMap(emitCountyAttrDisasterPairs)\
    .reduceByKey(lambda a,b: a+b)

# climateDisasterRDD.saveAsPickleFile('climateDisasterRDD')

climateDisasterRDD = sc.pickleFile('climateDisasterRDD')

# FINAL OUTPUT - key(county, state) value(date, attributeDict, disasterY/N, disasterType)
# with open('result_final.txt', 'w') as rf:
#     pprint(climateDisasterRDD.collect()[0:10], rf)


## SIMILARITY SEARCH
#function to create list of attributes of 7 days
# value(date, attributeDict, disasterY/N, disasterType)
def helperForShingles(dateRange):
    weeklyAttributeDict = dict()

    startDate = endDate = None
    startFlag = 0
    disasterBool, disasterType = False, ''

    for idx in range(0, len(dateRange)):
        if dateRange[idx] is None:
            continue

        date, attributeDict, idxDisasterBool, idxDisasterType = dateRange[idx]
        disasterBool = idxDisasterBool or disasterBool
        disasterType = idxDisasterType

        if not startFlag:
            startDate = date
            startFlag = 1

        endDate = date

        for k,v in attributeDict.items():
            #1_Tmax, 2_Tmax, etc
            newKey = str(idx+1)+'_'+k
            weeklyAttributeDict[newKey] = v
    
    return (startDate, endDate, weeklyAttributeDict, disasterBool, disasterType)


#function takes list of date
def emitWeeklyShingles(row):
    finalEmitList = []

    keyTuple = row[0]
    listOfDates = row[1]
    listOfDates.sort(key= lambda x: x[0])

    # making list of 366 days and filling entries based on dayofYear found
    dateRange = [None]*366
    first = float('inf')
    last = float('-inf')
    for dateItem in listOfDates:
        date = dateItem[0]
        dayOfYear = date.timetuple().tm_yday
        dateRange[dayOfYear] = dateItem

        if dayOfYear < first:
            first = dayOfYear
        if dayOfYear > last:
            last = dayOfYear
    
    # making weekly shingle strings
    if last - first <= 7:
        valueTuple = helperForShingles(dateRange[first:last])
        finalEmitList.append((keyTuple, valueTuple))

    else:
        for idx in range(first, last, 7):
            weekData = dateRange[idx: idx + 7]
            valueTuple = helperForShingles(weekData)
            finalEmitList.append((keyTuple, valueTuple))
    
    return finalEmitList



shingleRDD = climateDisasterRDD.flatMap(emitWeeklyShingles)

for item in shingleRDD.collect():
    pprint(item)
