import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from datetime import datetime
import time
from os import walk
from pyspark.sql.functions import datediff, when, to_date, lit, unix_timestamp,col, date_format, date_add, monotonically_increasing_id, row_number, to_timestamp
from pyspark.sql.functions import rand,when


from password import *

"""
Data management pipeline:
This pipeline generates a matrix where the rows denote the information of an
aircraft per day, and the columns refer to the FH, FC and DM KPIs, and the
average measurement of the 3453 sensors.

FH: Flight Hours (nombre d'hores de vol per dia)
FC: Flighr Cycles (nombre de vegades que l'avió ha despegat-aterrat)
DM: Delayed Minutes (total de minuts de retard acumulat en tots els vols)

"""



def process(sc, save_csv):
    sess = SparkSession(sc)

    DW = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/DW?sslmode=require")
		.option("dbtable", "public.aircraftutilization")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())

    AMOS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
		.option("dbtable", "oldinstance.operationinterruption")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())


    # Llista de tots els .csv a llegir
    path = "resources/trainingData"
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
        break

    toDate = lambda date: datetime.strptime(date[:10], "%Y-%m-%d").date()
    #toDate2 = lambda date: datetime.strptime(date, "%Y-%m-%d").date()
    # Extreure dades de tots els .csv en un dataframe
    # Sortida: day(datetime), aircraftregistration(string), sensor_avg
    # FUNCIONA!
    vals = []
    for filename in f:
        # Model de l'avio
        division = filename.split("-")
        aircraftid = (division[4] + "-" + division[5]).split(".")[0]

        vals.append(sc.textFile("./" + path + "/" + filename)
        	.filter(lambda t: "date" not in t)
            .map(lambda t: t.split(";"))
            .map(lambda t: ((aircraftid, toDate(t[0])), (float(t[2]), 1))) #guardem les dades i una columna de 1s que ens
            .reduceByKey(lambda f, f2: (f[0] + f2[0], f[1] + f2[1]))       #servirà per sumar i trobar l'average en el reduce
        )


    data = (sc.union(vals).
            reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
            .mapValues(lambda d: d[0] / d[1])) #calculem l'average

    columns = ["aircraftid", "date", "sensor_avg", "flighthours", "flightcycles", "delayedminutes"]
    KPIS = (DW.select("aircraftid", "timeid", "flighthours", "flightcycles", "delayedminutes").
            rdd.map(lambda f: ( (f[0], f[1]) , ( float(f[2]), int(f[3]), int(f[4]), 0) ) ) )
    joined = (data.join(KPIS).mapValues(lambda d: (d[0], *d[1])))
    joinedDF = joined.map(lambda t: (t[0][0], t[0][1], t[1][0], t[1][1], t[1][2], t[1][3])).toDF(columns)

    MaintenanceEvents = (AMOS.select("aircraftregistration", "departure", "kind", "subsystem").rdd\
                        .filter(lambda t: str(t[3]) == "3453"))

    columns2 = ["aircraftid1", "date1", "kind"]
    LabTrueDF = (MaintenanceEvents.filter(lambda t: t[2] not in ['Revision','Maintenance'])
            .map(lambda t: ((t[0],t[1]), 1))\
            .map(lambda t: (t[0][0], t[0][1], t[1])))\
            .toDF(columns2)


    cond = [((joinedDF.aircraftid == LabTrueDF.aircraftid1) &\
            (datediff(LabTrueDF.date1, joinedDF.date) >= 0)) &\
            (datediff(LabTrueDF.date1, joinedDF.date) <= 6) ]

    joinLabel = (joinedDF.join(LabTrueDF, cond, 'left'))
    joinLabel = joinLabel.drop("aircraftid1", "date1")
    joinLabel = joinLabel.distinct()
    joinLabel = joinLabel.withColumn("kind", \
              when(joinLabel["kind"].isNull(), 0).otherwise(joinLabel["kind"]))
    #print(joinLabel.collect(), len(joinLabel.collect()))

##################################################
    # Eliminar data i avió per retornar la matriu de KPI
    data_train =  joinLabel.drop("date").drop("aircraftid")

    # Guardar en csv en cas de que es demani
    if save_csv:
        data_train.toPandas().to_csv('dataOutput/KPI_matrix.csv')
    return data_train
