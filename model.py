import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import tree
import pydotplus
from IPython.display import Image 
import os

data1 = pd.read_csv('rejectedLoans.csv',converters={'st': str,'credit':str})
data2 = pd.read_csv('acceptedLoans.csv',converters={'st': str,'credit':str})

frames = [data1, data2]
data = pd.concat(frames)

data['credit'] = data['credit'].astype('category')
data['st'] = data['st'].astype('category')

cat_columns = data.select_dtypes(['category']).columns
data[cat_columns] = data[cat_columns].apply(lambda x: x.cat.codes)

randObj = np.random
randObj.seed(25)

rnd = pd.DataFrame(randObj.randn(779501, 2))
msk = randObj.rand(len(rnd)) < 0.8

train = data[msk]
test = data[~msk]

def getTrainFeature(colums):
	return train[colums]

def getTestFeature(columns):
	return test[columns]

trainValue = train['loanAccept']
testValues = test['loanAccept']
trainValuesList = np.array(trainValue.tolist())
testValuesList = np.array(testValues.tolist())

def findError(pred):
	corrId = 0 
	truePos = 0
	trueNeg = 0
	falsePos = 0
	falseNeg = 0
	for i in range(0,len(pred)):
		if pred[i]==0 and pred[i]==testValuesList[i]:
			trueNeg = falsePos+1
			corrId = corrId+1
		elif pred[i]==1 and pred[i]==testValuesList[i]:
			truePos = truePos+1
			corrId = corrId+1
		elif pred[i]==0 and pred[i]!=testValuesList[i]:
			falsePos = falseNeg + 1
		else:
			falseNeg = trueNeg+1
	accuracy = float(corrId)/len(testValuesList)
	precision = float(truePos)/(truePos+falsePos)
	fScore = 2*float(truePos)/(2*truePos+falsePos+falseNeg)
	errorList = [accuracy,precision,fScore]
	return errorList


featureList = ['amount','credit','dti','zipc','st','empLen']

trainFeatureArray = getTrainFeature(featureList)
testFeatures = getTestFeature(featureList)

clf = tree.DecisionTreeClassifier()
clf.fit(trainFeatureArray,trainValue)
prediction =  clf.predict(testFeatures)
print "All Features: ",findError(prediction)

for feature in featureList:
	clf1 = tree.DecisionTreeClassifier()
	trainFeatureArray = getTrainFeature([feature])
	testFeatures = getTestFeature([feature])
	clf.fit(trainFeatureArray,trainValue)
	preds =  clf.predict(testFeatures)
	print feature,findError(preds)