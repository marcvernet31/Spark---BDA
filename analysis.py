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
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

from pathlib import Path
import shutil


"""
Decision Tree:
    https://spark.apache.org/docs/latest/mllib-decision-tree.html
"""

def process(sc, db, save_model):

    input = db
    vector_assembler = VectorAssembler(inputCols=["sensor_avg", "flighthours", "flightcycles", "delayedminutes"],outputCol="features")
    df_temp = vector_assembler.transform(input)
    data = df_temp.drop("sensor_avg", "flighthours", "flightcycles", "delayedminutes")
    l_indexer = StringIndexer(inputCol="kind", outputCol="label")
    data = l_indexer.fit(data).transform(data)


    #data = input.map(lambda t: LabeledPoint(t[5],[t[:4]]))

    # Divisió en training i test (0.7:0.3)
    trainingData = data.sampleBy('label', fractions = {0: .95, 1: .8}, seed = 78927)
    testData = data.subtract(trainingData)
    # Decision Tree
    model = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth = 30)
    fited_model = model.fit(trainingData)

    # Validació amb les dades de test
    predictions = fited_model.transform(testData)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="accuracy")
    evaluator2 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="recallByLabel")
    accuracy = evaluator.evaluate(predictions)
    recall = evaluator2.evaluate(predictions)
    print("Test Error = %g " % (1.0 - accuracy))
    print("Accuracy = %g " % accuracy)
    print("Recall = %g " % recall)

    # Guardar el model si l'usuari ho demena
    if save_model:

        # Comprovar si el directori ja existeix per evitar sobreescriure
        my_file = Path("dataOutput/myDecisionTreeClassificationModel")
        if my_file.is_dir():
            shutil.rmtree("dataOutput/myDecisionTreeClassificationModel")

        # guardar
        fited_model.save("dataOutput/myDecisionTreeClassificationModel")

    return fited_model
