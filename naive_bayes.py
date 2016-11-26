
from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
import sys

def parseLine(line):
    parts = line.split(',')
    label = float(parts[len(parts)-1])
    features = Vectors.dense([float(parts[x]) for x in range(0,len(parts)-1)])
    return LabeledPoint(label, features)
# $example off$

if __name__ == "__main__":

    SparkContext.setSystemProperty('spark.executor.memory', '4g')
    SparkContext.setSystemProperty('spark.driver.memory', '4g')
    SparkContext.setSystemProperty('spark.driver.maxResultSize', '4g')
    sc = SparkContext(appName="PythonNaiveBayes")
    #print(sc._conf.getAll())
    data = sc.textFile(sys.argv[1]).map(parseLine)
    
    # Split data aproximately into training (80%) and test (40%)
    training, test = data.randomSplit([0.8, 0.2], seed=0)

    # Train a naive Bayes model.
    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    #to interate through rdd
    #predictionAndLabel.map(lambda x: print(str(x[0])+" "+str(x[1]))).collect()
    predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))
    truepos=predictionAndLabel.filter(lambda x: (x[0]==1)and (x[1]==1)).count()
    trueneg=predictionAndLabel.filter(lambda x: (x[0]==0)and (x[1]==0)).count()
    falseneg=predictionAndLabel.filter(lambda x: (x[0]==0)and (x[1]==1)).count()
    falsepos=predictionAndLabel.filter(lambda x: (x[0]==1)and (x[1]==0)).count()
    print("true positive "+str(truepos))
    print("true negative "+str(trueneg))
    print("false negative "+str(falseneg))
    print("false pasitive "+str(falsepos))
    accuracy = 1.0 * predictionAndLabel.filter(lambda x: x[0] == x[1]).count() / test.count()
    print(accuracy)
    print("recall"+ str(float(truepos*100)/(truepos+falseneg)))
    print("precision"+ str(float(truepos*100)/(truepos+falsepos)))
    
