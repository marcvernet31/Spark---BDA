import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

from os import walk


from password import *
"""
Cal tenir el fitxer password.py and:
    AIMSusername = "nom.cognom"
    AIMSpassword = "DBddmmyy"
"""


"""
Data management pipeline:
This pipeline generates a matrix where the rows denote the information of an
aircraft per day, and the columns refer to the FH, FC and DM KPIs, and the
average measurement of the 3453 sensors.

FH: Flight Hours (nombre d'hores de vol per dia)
    Cal:
        AIMS(flights): aircraftregistration(id de l'avió), actualdeparture,
            actualarrival, canceled(format?)
FC: Flighr Cycles (nombre de vegades que l'avió ha despegat-aterrat)
    Cal:
        AIMS(flights): aircraftregistration, flightid(identificador del vol, unic?),
            canceled
DM: Delayed Minutes (total de minuts de retard acumulat en tots els vols)
    Cal:
        AIMS(flights): aircraftregistration, scheduledarrival, actualarrival,
            canceled(suposem que un vol cancelat no és un retard)

Cal:    AIMS(fligths): aircraftregistration, flightid, actualdeparture, actualarrival,
        scheduledarrival, canceled,

        AMOS(maintenanceevent): aircraftregistration, starttime

        csv: llegir les dades de sensors

-----------------
Suposicions generals:
    - Suposem que totes les dades de AIMS i AMOS ja venen netejades i no hi haura
    problemes raros de vols que s'intercalen i les merdes de sempre
    - Suposem que flightid de AMOS(flights) és un identificador únic per cada vol
    (es pot comprovar facil)
    - Per calcular DM, suposem que un vol canelat no conta com a retard
    - Considerem actualdeparture el dia en que es fa el vol (per a l'hora d'agregar en dies)
"""
def process(sc):
    sess = SparkSession(sc)

    AIMS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AIMS?sslmode=require")
		.option("dbtable", "public.flights")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())

    AMOS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AMOS?sslmode=require")
		.option("dbtable", "oldinstance.maintenanceevents")
		.option("user", AIMSusername)
		.option("password", AIMSpassword)
		.load())

    # Afegir la columna day (2012-7-23), per poder agregar per dia
    # Eliminar vols cancelats
    AIMS = (AIMS.withColumn('day', AIMS['actualarrival'].cast('date'))
        .filter(AIMS['cancelled'] == "False"))

    # Seleccionar: aircraftid, day, (actualarrival-actualdeparture)
    # FH = AIMS.rdd.map(lambda t: (t[0],t[14], t[8]-t[7]))
    # FC =
    # DM =

    # Lectura dels csv
    path = "resources/trainingData"
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
        break

    # Crear un dataFrame on posar les dades del csv (quin format ?)
    # ...

    for filename in f:
        # Mitjana del valor del sensor
        input = (sc.textFile("./" + path + "/" + filename)
        		.filter(lambda t: "date" not in t)
                .map(lambda t: t.split(";")[2]).collect())
        input = list(map(float, input))
        sensor_avg = sum(input) / len(input)

        # Trobar el aircraftid i dia (separar del titol, ddmmyy to dateTime,...)
        # ...
        # Posar tot al DataFrame
        # ...


    # Join de KPI's derivats de AIMS amb dataFrame dels csv
    # ...

    # Extreure led dades de manteniment de AMOS i creuar amb la taula anterior
    # ...

    # Retornar dataFrame unificat (quin format ? )
    # ...
