import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

from password import *

import statistics
from datetime import datetime
import time
from os import walk
from pyspark.sql.functions import datediff, when, to_date, lit, unix_timestamp,col, date_format, monotonically_increasing_id, row_number
from pyspark.sql.functions import rand,when
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

from password import *

"""
Finally, once the model has been created, we must create a pipeline that, given a new record
(<aircraft,day, FH, FC, DM, AVG(sensor)>), predicts if this aircraft is going to go for unscheduled maintenance.
"""

def process(sc, date_query, aircraft_query, model, save_model):
    sess = SparkSession(sc)

    #Carrega de DW
    DW = (sess.read
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
        .option("dbtable", "public.aircraftutilization")
        .option("user", AIMSusername)
        .option("password", AIMSpassword)
        .load())


    # Llista de tots els .csv a llegir
    path = "resources/trainingData"
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
        break


    #Funció que converteix Strings a format "Date"
    toDate = lambda date: datetime.strptime(date[:10], "%Y-%m-%d").date()

    #Canviem el format de la data especificada a la query
    x = date_query.split('-')
    compressedDate = x[2] + x[1] + x[0][2] + x[0][3]

    #Objectiu: aircraftregistration(string), day(datetime), sensor_avg
    vals = []
    for filename in f:
        # Extreure identificador de l'avio i data del titol de csv
        division = filename.split("-")
        date_csv = division[0]
        aircraft_csv = (division[4] + "-" + division[5]).split(".")[0]

        # En cas de coincidencia entre la query i el nom del csv, calcular la mitjana del sensor:
        if((aircraft_csv == aircraft_query) and (date_csv == compressedDate)):
            vals.append(sc.textFile("./" + path + "/" + filename)
            	.filter(lambda t: "date" not in t)
                .map(lambda t: t.split(";"))
                .map(lambda t: ((aircraft_query, toDate(t[0])), (float(t[2]), 1)))
                #Ara tenim una llista de les observacions del mateix vol, toca fer un ReduceByKey
                .reduceByKey(lambda f, f2: (f[0] + f2[0], f[1] + f2[1]))
                #I ara tenim per al vol que ens interessa, la suma total dels sensors i el nombre d'instàncies
            )

    #Ara data és una sola fila, que correspon al vol que coincideix amb la query
    data = (sc.union(vals).
            reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
            .mapValues(lambda d: d[0] / d[1])) #calculem l'average del sensor, igual que a management.py
    #Ara "data" és un RDD en que hi ha guardats per a tots els vols el seu avg_sensor.


    columns = ["aircraftid", "date", "sensor_avg", "flighthours", "flightcycles", "delayedminutes"]


    #Carreguem els KPIs de DW
    KPIS = (DW.select("aircraftid", "timeid", "flighthours", "flightcycles", "delayedminutes").
            rdd.map(lambda f: ( (f[0], f[1]) , ( float(f[2]), int(f[3]), int(f[4]), 0) ) ) )


    #Join entre "data" amb els KPIs corresponents al vol
    joined = (data.join(KPIS).mapValues(lambda d: (d[0], *d[1])))
    joinedDF = joined.map(lambda t: (t[0][0], t[0][1], t[1][0], t[1][1], t[1][2], t[1][3])).toDF(columns)
    joinedDF = joinedDF.drop("aircraftid", "date")
    #Ara tinc joinedDF que és un DataFrame que relaciona el vol de la query amb els seus KPIs


    #Defineixo les columnes com a features que utilitzaré per predir
    vector_assembler = VectorAssembler(inputCols=["sensor_avg", "flighthours", "flightcycles", "delayedminutes"],outputCol="features")
    to_predict = vector_assembler.transform(joinedDF)

    #Si he decidit guardar el model en local, el carrego. Si no, utilitzaré el model que he passat com a paràmetre
    if save_model:
        model = DecisionTreeClassificationModel.load("./dataOutput/myDecisionTreeClassificationModel")

    #Calculo la predicció per a la meva query
    prediction = model.transform(to_predict)
    pred = prediction.select("prediction").collect()[0]['prediction']

    if (pred == 1.0):
        print("Maintenance IS required")
    else: print("Maintenance IS NOT required")
