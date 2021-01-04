import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from management import *

import argparse
import time
from pathlib import Path

import management
import analysis
import runtime

"""
Default:
python3 main.py -csv False -model False -reuse False
python3 main.py --help

Cal tenir el fitxer password.py and:
    AIMSusername = "nom.cognom"
    AIMSpassword = "DBddmmyy"

(linux) Cal conectar-se amb:
     f5fpc -s -x -t https://upclink.upc.edu

Cal:

main.py:
    - la variable save_model no funciona, sempre guarda el model. S'ha pasar el model
    com a parametre de runtime.py
runtime.py:
    - el model ha de pasar com a parametre, no s'ha de carregar sempre de local
    (ara no funciona)
    - posar una explicacio a la capçalera de que fa (com la que hi ha a management.py)
analysis.py:
    - posar una explicacio a la capçalera de que fa (com la que hi ha a management.py)


Comentar tot el codi i posar-lo bonic
eliminar els imports que no facin falta
"""



# Flags per al terminal
parser = argparse.ArgumentParser()
parser.add_argument("-csv", "--save_csv", help="True: save KPI matrix as csv locally (False: default)")
parser.add_argument("-model", "--save_model", help="True: save the trained model locally (False: default)")
parser.add_argument("-reuse", "--reuse_model", help="True: when possible reuse precalculated model for faster predictions (False: default)")
args = parser.parse_args()




HADOOP_HOME = "./resources/hadoop_home"
JDBC_JAR = "./resources/postgresql-42.2.8.jar"

# (canviar segons requeriments)
#PYSPARK_PYTHON = "python3.6"
#PYSPARK_DRIVER_PYTHON = "python3.6"
PYSPARK_PYTHON = "python3.8"
PYSPARK_DRIVER_PYTHON = "python3.8"

if(__name__== "__main__"):
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    sys.path.append(HADOOP_HOME + "\\bin")
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON

    conf = SparkConf()  # create the configuration
    conf.set("spark.jars", JDBC_JAR)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .master("local") \
        .appName("Training") \
        .getOrCreate()

    sc = pyspark.SparkContext.getOrCreate()


    # Entrada de l'input de l'usuari
    print("------------------------------------------")
    aircraft_query = input("Enter Aircraft ID: ")
    date_query = input("Enter date (yyyy-mm-dd): ")
    print("------------------------------------------")

    # Parametres dels flags
    save_csv = args.save_csv
    if save_csv == "True":
        save_csv = 1
    else:
        save_csv = 0

    save_model = args.save_model
    if save_model == "True":
        save_model = 1
    else:
        save_model = 0

    reuse_model = args.reuse_model
    if reuse_model == "True":
        reuse_model = 1
    else:
        reuse_model = 0


    # Cas en que es reutilitza un model ja calculat (comprovant que existeix)
    my_dir = Path("dataOutput/myDecisionTreeClassificationModel")
    if reuse_model and my_dir.is_dir():
        start = time.time()
        runtime.process(sc, str(date_query), str(aircraft_query))
        end = time.time()

        print("------------------------------------------")
        print("Execution time:")
        print("runtime(s): ", end - start)

    else:
        start = time.time()

        # KPI matrix
        start_m = time.time()
        db = management.process(sc, save_csv)
        end_m = time.time()

        # Creació del model
        start_a = time.time()
        model = analysis.process(sc, db, save_model)
        end_a = time.time()

        # resultat final
        start_r = time.time()
        runtime.process(sc, str(date_query), str(aircraft_query))
        end_r = time.time()

        end = time.time()

        print("------------------------------------------")
        print("Execution time:")
        print("management(s):", end_m - start_m)
        print("analysis(s):", end_a - start_a)
        print("runtime(s):", end_r - start_r)
        print("total(s):", end - start)

    print("Query:")
    print(aircraft_query + " // " + date_query)
