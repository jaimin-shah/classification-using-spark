# $example on$
from pyspark import SparkContext
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
from pyspark.sql import SparkSession
import sys

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '4g')
spark = SparkSession.builder.appName("neural").config('spark.sql.warehouse.dir', 'file:///C:/Users/hp/Downloads/spark-2.0.0-bin-hadoop2.7/spark-2.0.0-bin-hadoop2.7/bin/').getOrCreate()

# $example on$
# Load training data
data = spark.read.format("libsvm").load(sys.argv[1])
# Split the data into train and test
splits = data.randomSplit([0.8, 0.2], 0)
train = splits[0]
test = splits[1]
# specify layers for the neural network:
# input layer of size 4 (features), two intermediate of size 5 and 4
# and output of size 3 (classes)
layers = [8,5, 2]
# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=500, layers=layers, blockSize=64, seed=0)
# train the model
model = trainer.fit(train)
# compute accuracy on the test set
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
listobject=predictionAndLabels.rdd.collect()
#to print type of object
print(type(listobject))
print(type(predictionAndLabels))

truepos=len(list(filter(lambda x: (x[0]==1)and (x[1]==1),listobject)))
trueneg=len(list(filter(lambda x: (x[0]==0)and (x[1]==0),listobject)))
falseneg=len(list(filter(lambda x: (x[0]==0)and (x[1]==1),listobject)))
falsepos=len(list(filter(lambda x: (x[0]==1)and (x[1]==0),listobject)))
print("true positive "+str(truepos))
print("true negative "+str(trueneg))
print("false negative "+str(falseneg))
print("false pasitive "+str(falsepos))
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Accuracy: " + str(evaluator.evaluate(predictionAndLabels)*100))
print("recall"+ str(float(truepos*100)/(truepos+falseneg)))
print("precision"+ str(float(truepos*100)/(truepos+falsepos)))
# $example off$
spark.stop()
