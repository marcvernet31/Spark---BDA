import os
import sys
import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

"""
Modificar el flow de l'execució
"""

# Afegir fitxers a vincular
import management
import analysis
import runtime

HADOOP_HOME = "./resources/hadoop_home"
JDBC_JAR = "./resources/postgresql-42.2.8.jar"

# A mi no em funciona per python3.6 (canviar segons requeriments)
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

    #Create and point to your pipelines here
    if(len(sys.argv) < 2):
        print("Funcions actuals: (management)")
        exit()
    elif(sys.argv[1] == "management"):
        management.process(sc)
    elif(sys.argv[1] == "analysis"):
        analysis.process(sc)
    elif(sys.argv[1] == "runtime"):
        runtime.process(sc)
    else:
       print("Funcio no existent")
