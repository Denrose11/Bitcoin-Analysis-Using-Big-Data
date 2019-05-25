# Data-Analytics-
COURSEWORK- Data Analytics 
# Prediction and Classification of countries based on their rate level of Development 

This project tells whether a country is developed, developing or under-developed by looking and calculating it's HDI values.

## Getting Started

These instructions will tell you how exactly the project works


The dataset files contains all the datasets needed for our project. 

### Installing Libraries 

Install all the libraries important for running the code 

```
import pandas as pd
import numpy as np
from pandas import read_excel
import matplotlib as plt
%matplotlib inline
import pylab as plot
import mglearn.plots
import seaborn as sns
from sklearn.manifold import Isomap
from sklearn import linear_model
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn import metrics
from sklearn import svm
from mlxtend.plotting import plot_decision_regions
from sklearn.model_selection import train_test_split
from mlxtend.plotting import plot_decision_regions
import tkinter as tk
from yellowbrick.classifier import ConfusionMatrix
```
Thesse are the python libraries that needed to run the code.py file
Two datasets need to be imported on which further models are built and run
```
df0=pd.read_excel(r"E:\Qmul\Data Analytics\ONLY_HDI.xlsx")
df0_=pd.read_excel(r"E:\Qmul\Data Analytics\All_other_datasets.xlsx")
```
After checking and understanding the dataset, we perform two major things called classification and prediction.
For classification, 4 methods are used which are described in the report file attached.
Classification allows us to classify the countries on the basis of their status.

Prediction allows us to predict the countries rate of development by calculating it's HDI value
.

```
df3.index = pd.to_numeric(df3.index)
X = df3.index.values #features
y = df3[["India"]].values #target variable
#y=df3.iloc[:,:12]
for col in df3.columns: 
    print(col) )
```

A GUI called Tkinter is used so as to have an user interface:
The first entry box is for the HDI value
```
label1 = tk.Label(root, text='  HDI:')
canvas1.create_window(100, 100, window=label1)

entry1 = tk.Entry (root) # create 1st entry box
canvas1.create_window(270, 100, window=entry1)
```

The second entry box is for entering the year

```
label2 = tk.Label(root, text=' Year:         ')
canvas1.create_window(120, 120, window=label2)

entry2 = tk.Entry (root) # create 2nd entry box
canvas1.create_window(270, 120, window=entry2)

```


## Authors

* **Aditi, Divya, Denrose, Pranjal** 


## Acknowledgments

* Thanks for the teaching and support through-out Bhusan and Anthony!
