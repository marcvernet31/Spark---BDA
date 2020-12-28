import pyspark
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint

from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

"""
Decision Tree:
    https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier

Cal canviar:
    - Ara el fitxer d'entrada és un csv, i a l'entrega ha de ser un dataframe o hadoop.
    (Per tant tot l'input de dades s'ha de canviar)
"""

def process(sc):
    sess = SparkSession(sc)

    input = (sc.textFile("management_temporal.csv")
		.filter(lambda t: "FH" not in t)
		.map(lambda t: (t.split(",")))
        )

    data = input.map(lambda t: LabeledPoint(t[5],[t[:4]]))

    columns = ['features', 'label']
    data = sess.createDataFrame(list(data.collect()), columns)

#    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
#    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures").fit(data)

    (trainingData, testData) = data.randomSplit([0.7, 0.3])

#    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
