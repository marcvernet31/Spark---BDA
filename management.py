import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

import datetime
import time
from os import walk
from pyspark.sql.functions import datediff, when, to_date, lit, unix_timestamp,col, date_format, monotonically_increasing_id, row_number


from password import *
"""
Cal tenir el fitxer password.py and:
    AIMSusername = "nom.cognom"
    AIMSpassword = "DBddmmyy"

(linux) Cal posar:
     f5fpc -s -x -t https://upclink.upc.edu
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
    - Suposem que tots els vols arriben despres de la sortida (actualarrival > actualdeparture)
    - Per FC assumim que totes les avions que despeguen aterren en algun moment
    - Un FC conta pel dia en que despega, encara que aterri en un altre dia
    - Assumim flightid un identificador únic
    - Si un vol arriba abans de la scheduledarrival té retard negatiu que resta al retard total
        (esta be ?)
    - Assumim que un mateix avió pot tenir diferents manteniments programats (?)

Cal fer:
    - Comprovar que els valors de DM estan bé.
    - Veure si es pot implementar .cache() per anar més ràpid
"""



def process(sc):
    sess = SparkSession(sc)

    AIMS = (sess.read
		.format("jdbc")
		.option("driver","org.postgresql.Driver")
		.option("url", "jdbc:postgresql://postgresfib.fib.upc.edu:6433/AIMS?sslmode=require")
		.option("dbtable", "oldinstance.flights")
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
        #.filter(AIMS['cancelled'] == "False").withColumn("duration", datediff(AIMS['actualdeparture'], AIMS['actualarrival']))
        .withColumn('day', date_format(col("day"), "y-MM-dd"))
        #.orderBy(["day", "aircraftregistration"],ascending=False)
        )


    # Sortirda: day(datetime), aircraftregistration(string), sum(duration_hours)
    #   duration_hours = (actualarrival-actualdeparture) en hores (per dia)
    # FUNCIONA!
    FH = (AIMS.withColumn('duration_hours',
            (unix_timestamp(AIMS['actualarrival'], "yyyy-MM-dd'T'hh:mm:ss")
            - unix_timestamp(AIMS['actualdeparture'], "yyyy-MM-dd'T'hh:mm:ss"))/(60*60)
        )
        .select("day", "aircraftregistration", "duration_hours")
        .groupBy("day","aircraftregistration").sum("duration_hours")
        .withColumnRenamed("sum(duration_hours)", "FH")
        )

    # Sortirda: day(datetime), aircraftregistration(string), count()
    #  FUNCIONA!
    FC = (AIMS.select("day", "aircraftregistration", "flightid")
        .groupBy("day","aircraftregistration").count()
         .withColumnRenamed("count", "FC")
    )

    # Sortirda: day(datetime), aircraftregistration(string), sum(delay_hours)
    #   delay_hours = (actualarrival-scheduledarrival) en hores (per dia)
    # FUNCIONA!
    DM = (AIMS.withColumn('delay_hours',
            (unix_timestamp(AIMS['actualarrival'], "yyyy-MM-dd'T'hh:mm:ss")
            - unix_timestamp(AIMS['scheduledarrival'], "yyyy-MM-dd'T'hh:mm:ss"))/(60*60)
        )
        .select("day", "aircraftregistration", "delay_hours")
        .groupBy("day","aircraftregistration").sum("delay_hours")
        .withColumnRenamed("sum(delay_hours)", "DM")
    )

    # Llista de tots els .csv a llegir
    path = "resources/trainingData"
    f = []
    for (dirpath, dirnames, filenames) in walk(path):
        f.extend(filenames)
        break

    # Extreure dades de tots els .csv en un dataframe
    # Sortida: day(datetime), aircraftregistration(string), sensor_avg
    # FUNCIONA!
    vals = list()
    for filename in f:
        # Model de l'avio
        division = filename.split("-")
        aircraftid = (division[4] + "-" + division[5]).split(".")[0]

        input = (sc.textFile("./" + path + "/" + filename)
        	.filter(lambda t: "date" not in t))

        # Mitjana del valor del sensor
        sensors = input.map(lambda t: t.split(";")[2]).collect()
        sensors = list(map(float, sensors))
        sensor_avg = sum(sensors) / len(sensors)

        # Data
        date = list(input.map(lambda t: t.split(";")[0]).collect())
        date = date[1].split(" ")[0] # Seleccionar una data i eliminar h:m:s

        # Posar tot al DataFrame
        vals.append([date, aircraftid, sensor_avg])

    columns = ['day', 'aircraftregistration', 'sensor_avg']
    SensorLectures = sess.createDataFrame(vals, columns)



    # Join de les dades d'AIMS i les dels SensorLectures
    # FUNCIONA!
    cond = ['day', 'aircraftregistration']
    KPI = FH.join(FC, cond).join(DM, cond).join(SensorLectures, cond)



    # Crear labels de manteniment
    # Sortida: aircraftregistration(string), startime(datetime)
    MaintenanceEvents = (AMOS.select("aircraftregistration", "starttime")
        .withColumn('starttime', date_format(col("starttime"), "y-MM-dd"))
    )

    # Crear labels del manteniment i vincular amb la resta de dades
    #   ....
