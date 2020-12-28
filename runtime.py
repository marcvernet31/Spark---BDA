import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

from password import *

import statistics
import datetime
import time
from os import walk
from pyspark.sql.functions import datediff, when, to_date, lit, unix_timestamp,col, date_format, monotonically_increasing_id, row_number
from pyspark.sql.functions import rand,when

from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

"""
Suposcicions:
    - Suposem que el format de les dades d'entrada sera sempre (2019-05-10)
    per les dates i un string per aircraftregistration.
Cal fer:
    - Falta saber quin es el format d'entrada de la query
    - Afegir algun avis d'error quan el dia o avio de la query no aparegui a AIMS
    - Comprovar be que vol dir 1 o 0, comparar amb quan es posa el label a management.py
    - A la versió definitiva, el model no es pot guardar de forma local, per tant la forma
    com es crida i executa cambiarà
    - Mucho import
"""

def process(sc):
    sess = SparkSession(sc)

    # Format temporal, encara no es sap com es llegeix la query
    date_query = "2012-03-07"
    aircraft_query = "XY-LOL"

    AIMS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AIMS?sslmode=require")
		.option("dbtable", "oldinstance.flights")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())

    AIMS = (AIMS.withColumn('day', AIMS['actualarrival'].cast('date'))
        .filter(AIMS['cancelled'] == "False")
        .withColumn('day', date_format(col("day"), "y-MM-dd"))
        )

    AIMS = (AIMS.filter(AIMS['day'] == date_query)
        .filter(AIMS['aircraftregistration'] == aircraft_query)
    )

    FH = (AIMS.withColumn('duration_hours',
            (unix_timestamp(AIMS['actualarrival'], "yyyy-MM-dd'T'hh:mm:ss")
            - unix_timestamp(AIMS['actualdeparture'], "yyyy-MM-dd'T'hh:mm:ss"))/(60*60))
        .select("duration_hours").groupBy().sum().collect()[0][0]
        )

    FC = AIMS.count()

    DM = (AIMS.withColumn('delay_hours',
            (unix_timestamp(AIMS['actualarrival'], "yyyy-MM-dd'T'hh:mm:ss")
            - unix_timestamp(AIMS['scheduledarrival'], "yyyy-MM-dd'T'hh:mm:ss"))/(60*60))
        .select("delay_hours").groupBy().sum().collect()[0][0]
    )


    # Llista de tots els .csv a llegir
    path = "resources/trainingData"
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
        break

    # Filtrar els csv que ens interessen i extreure el valor dels sensors:
    x = date_query.split('-')
    compressedDate = x[2] + x[1] + x[0][2] + x[0][3]

    vals = list()
    for filename in f:
        # Extreure identificador de l'avio i data del titol de csv
        division = filename.split("-")
        date_csv = division[0]
        aircraft_csv = (division[4] + "-" + division[5]).split(".")[0]

        # En cas de coincidencia calcular la mitjana del sensor
        if((aircraft_csv == aircraft_query) and (date_csv == compressedDate)):
            input = (sc.textFile("./" + path + "/" + filename)
            	.filter(lambda t: "date" not in t))

            sensors = input.map(lambda t: t.split(";")[2]).collect()
            sensors = list(map(float, sensors))
            sensor_avg = sum(sensors) / len(sensors)
            vals.append(sensor_avg)
    sensor_avg = statistics.mean(vals)

    # Exportar el model guardat i fer la predicció
    KPI = list([FH, FC, DM, sensor_avg])
    model = DecisionTreeModel.load(sc, "dataTemporal/myDecisionTreeClassificationModel")
    prediction = model.predict(KPI)

    if prediction == 1:
        print("Maintenance is not required")
    elif prediction == 0:
        print("Maintenance is required")
