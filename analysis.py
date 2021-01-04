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
By using the data matrix created in the previous pipeline, we want to train a classifier. In the literature,
we can find many papers reporting that decision trees perform well for predicting transport delays.
Thus, we will use the MLlib library to train a decision tree.
"""

def process(sc, db, save_model):

    input = db
    #Especifiquem les columnes que volem que siguin "features"
    vector_assembler = VectorAssembler(inputCols=["sensor_avg", "flighthours", "flightcycles", "delayedminutes"],outputCol="features")
    df_temp = vector_assembler.transform(input)
    data = df_temp.drop("sensor_avg", "flighthours", "flightcycles", "delayedminutes")

    #Especifiquem que la label és la columna "kind"
    l_indexer = StringIndexer(inputCol="kind", outputCol="label")
    data = l_indexer.fit(data).transform(data)


    # Divisió en train data i test data balancejada per a una quantitat semblant de labels True i False
    trainingData = data.sampleBy('label', fractions = {0: .95, 1: .8}, seed = 78927)
    testData = data.subtract(trainingData)
    # Definim el model de Decision Tree
    model = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth = 5)
    fited_model = model.fit(trainingData)

    # Validem el model amb les dades de test
    predictions = fited_model.transform(testData)

    #Calculem l'accuracy i el recall
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
