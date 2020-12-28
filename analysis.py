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

from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils


"""
Decision Tree:
    https://spark.apache.org/docs/latest/mllib-decision-tree.html


Comentaris // Cal canviar:
    - Ara el fitxer d'entrada és un csv, i a l'entrega ha de ser un dataframe o hadoop.
    (Per tant tot l'input de dades s'ha de canviar). EL format de l'input final pot fer canviar
    bastant el preprocés que cal abans de poder declarar el model.

    - S'ha de tenir en conte que ara l'error de test és molt gran perque els labels són aleatoris

    - Igual que el csv, el decision tree de moment es guarda de forma local, a la versió final
    ha d'anar tot d'una tirada o guardarse en hadoop

    - Els parametres del DecisionTree son els que venien per defecte, caldria entendre que fa cadascun
    per saber quin valor posar.

    - L'execcució és molt rapida, per tant potser es podrien fer proves amb diferents parametres del
    DecisionTree per veure quins son millors.

"""

def process(sc):
    sess = SparkSession(sc)

    input = (sc.textFile("dataTemporal/management_temporal.csv")
		.filter(lambda t: "FH" not in t)
		.map(lambda t: (t.split(",")))
        )

    # Canvi de dades al format LabeledPoint: [label, [features]]
    data = input.map(lambda t: LabeledPoint(t[5],[t[:4]]))

    # Divisió en training i test (0.7:0.3)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Decision Tree
    model = DecisionTree.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
            impurity='gini', maxDepth=5, maxBins=32)

    # Validació amb les dades de test
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

    # Test error (correctes / total)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())

    # confusionMatrix = [[TP, FP], [FN, TN]]
    metrics = MulticlassMetrics(labelsAndPredictions.map(tuple))
    confusionMatrix = metrics.confusionMatrix().toArray()

    # Recall = TruePositives / (TruePositives + FalseNegatives)
    recall = confusionMatrix[0][0] / (confusionMatrix[0][0] + confusionMatrix[1][0])

    # Precision = TruePositives / (TruePositives + FalsePositives)
    precision = confusionMatrix[0][0] / (confusionMatrix[0][0] + confusionMatrix[0][1])

    print('Test Error = ' + str(testErr))
    print('Recall = ' + str(recall))
    print('Precision = ' + str(precision))


    # Guardar el model
    # cuidado que es queixa si ja existeix
    model.save(sc, "dataTemporal/myDecisionTreeClassificationModel")

    # Recuperar un model guardat:
    # sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
