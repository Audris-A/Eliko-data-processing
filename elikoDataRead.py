import csv
import pymongo
import copy
import time
import os
import sys
from datetime import datetime

databaseClient = pymongo.MongoClient(os.environ["connectionString"])
database = databaseClient["conTraDB"]
locationCollection = database["ElikoLocations"]
distanceCollection = database["ElikoDistances"]

locationRecordArray = []
distanceRecordArray = []

locationRecordTemplate = {"zone_id" : "", "counter" : "", "tag_id" : "", "x_coordinate" : "", "y_coordinate" : "", "z_coordinate" : "", "error_msg" : "", "timestamp" : ""}
distanceRecordTemplate = {"zone_id" : "", "counter" : "", "tag_id" : "", "anchor_1" : "", "distance_1" : "", "anchor_2" : "", "distance_2" : "", "anchor_3" : "", 
                          "distance_3" : "", "anchor_4" : "", "distance_4" : "", "anchor_5" : "", "distance_5" : "", "anchor_6" : "", "distance_6" : "", 
                          "relative_timestamp" : "", "timestamp_from_coordinate_msg": ""}

with open(str(sys.argv[1]) + ".csv") as csvDataFile:
    csvReader = csv.reader(csvDataFile)

    distanceRecordArrayIt = 0

    for row in csvReader:
        row.insert(2, "LNO_stage") # Jāpiekoriģē katrai vietai ar roku

        if row[1] == "COORD":
            locationRecord = copy.deepcopy(locationRecordTemplate)

            counter = 2
            for key in locationRecord:
                if key == "timestamp":
                    row[counter] = datetime.fromtimestamp(float(row[counter])).strftime("%d.%m.%Y %H:%M:%S.%f")
                    distanceRecordArray[distanceRecordArrayIt]["timestamp_from_coordinate_msg"] = row[counter]
                    distanceRecordArrayIt += 1


                locationRecord[key] = row[counter]
                counter += 1

            locationRecordArray.append(locationRecord)

        if row[1] == "RR_L":
            distanceRecord = copy.deepcopy(distanceRecordTemplate)

            counter = 2
            for key in distanceRecord:
                if key != "timestamp_from_coordinate_msg":
                    distanceRecord[key] = row[counter]
                    counter += 1

            distanceRecordArray.append(distanceRecord)

locationInsert = locationCollection.insert_many(locationRecordArray)
distanceInsert = distanceCollection.insert_many(distanceRecordArray)
