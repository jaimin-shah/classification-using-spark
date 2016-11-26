from pyspark.ml import Pipeline
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import sys

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '4g')
spark = SparkSession.builder.appName("decision tree").config('spark.sql.warehouse.dir', 'file:///C:/Users/hp/Downloads/spark-2.0.0-bin-hadoop2.7/spark-2.0.0-bin-hadoop2.7/bin/').getOrCreate()

# Load the data stored in LIBSVM format as a DataFrame.
data = spark.read.format("libsvm").load(sys.argv[1])

# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.8, 0.2])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictionAndLabels=predictions.select("prediction", "indexedLabel", "features")

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
print("accuracy"+ str(float((truepos+trueneg)*100)/(truepos+falseneg+trueneg+falseneg)))
print("recall"+ str(float(truepos*100)/(truepos+falseneg)))
print("precision"+ str(float(truepos*100)/(truepos+falsepos)))
treeModel = model.stages[2]
# summary only
print(treeModel)