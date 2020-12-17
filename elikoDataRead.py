"""
    Ziņu piemēri:
        1) $PEKIO,COORD,148 (8 bit counter),0x000810 (Birka),18.78 (x),4.10 (y),0.91 (z), (error string),1607780734.22 (unix timestamp, kad mērījums tika izdarīts)
        2) $PEKIO,RR_L,227 (8 bit counter),0x000644 (Birka),0x000454 (Enkurs),559 (Distance),0x000488,1901,0x000452,2452,0x000496,2087,0x000475,1482,0x000449,298,
             15578410 (Birkas relatīvs laiks milisekundēs),0x00,0x00,0x00,0x00,0x00,0x00,0x00

    Ziņas pieprasījums: $PEKIO,GET_HISTORY_BY_UNIX_TIME, ALL (var likt arī specifiskus birku id), unix time from, unix time to
        Piemērs - $PEKIO,GET_HISTORY_BY_UNIX_TIME,ALL,1608098400,1608159600

"""

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
        row.insert(2, "LNO_PARTER") # Jāpiekoriģē katrai vietai ar roku

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
