from attr import attributes
from pyspark import SparkContext
from pprint import pprint
import csv
from collections import defaultdict
from dateutil import parser
from pprint import pprint as pp


sc = SparkContext(appName="PythonStreamingQueueStream")
sc.setLogLevel("WARN")

climateDisasterRDD = sc.pickleFile('climateDisasterRDD')

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
    pp(item)