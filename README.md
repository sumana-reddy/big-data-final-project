# Big-Data-final-project <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQaiPFuUvu60QKdB4N9UC2WFI2Jeq8tT2w0Pw&usqp=CAU" width="50" height="50">

## About Me :woman:
- [Sumana Reddy Reddybathula](https://github.com/sumana-reddy)

## Link for Databricks published notebook :notebook:
- https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7340731884324704/4073657483805730/3316173007748205/latest.html

## Source of text used <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRvqhBisSZIPP_GydSPJD3gr0kkCepL5_VqsA&usqp=CAU" width="31" height="33">
- https://www.gutenberg.org/

## Process text using Databricks Community Edition and PySpark. <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTtD2Xv4Qb0PkUR-537V3BeKE9mHD0asahldoiHB17UECjLVI7V-jYpIUHqWtWxMdtoEn8&usqp=CAU" width="35" height="35">

Using PySpark for Natural language Processing
* Cleaning
* Processing
* Charting

### Cleaning <img src="https://lh3.googleusercontent.com/proxy/nTAD3l0H9XudvAgSZsOCYhcXapp8pKBpTZBbVHTOl6KsTxLiwxa9X5CgOBU7XyTsCY6U6iL-tvI1L0rE3s19xk3vH7HS-3elNL9XvohYoeynfCmjZkz-DlftjBK0LWFhJNfw3EP8GXaXDzOOmlRxTrDL" alt="Flowers in Chania" width="28" height="28">
   * NLP stopword removal
   * Remove all non-leters
   * To get data text from URL
```
import urllib.request
urllib.request.urlretrieve("https://www.gutenberg.org/files/6130/6130-0.txt", "/tmp/gutenberg.txt")

```
   * dbutils.fs.mv to transfer the data and Storing the data in gutenberg.txt

```
dbutils.fs.mv("file:/tmp/gutenberg.txt", "dbfs:/data/gutenberg.txt")

```
```
rawRDD = sc.textFile("dbfs:/data/gutenberg.txt")

```
### Processing <img src="https://d2gg9evh47fn9z.cloudfront.net/800px_COLOURBOX17349311.jpg" width="30" height="30">

   * Flatmap from one to many(one line of text to many words)
   * cast everything to lowercase
   * strip whitespace from beginning and end split by our delimiter (space, commma)
   * map() into intermediate key-value pairs
   * filter() to get just some records
   * reduceByKey() to get the count
   * flatmap each line to words
```
wordsRDD = rawRDD.flatMap(lambda line : line.lower().strip().split(" "))

```
   * map() words to (words,1) intermediate key-value pairs.
   * removing punctutations.
   * prepare to clean stopwords and remove spaces and empty words

```
import re
cleanTokenRDD = wordsRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
from pyspark.ml.feature import StopWordsRemover
removes =StopWordsRemover()
stopWords = removes.getStopWords()
cleanWordsRDD=cleanTokenRDD.filter(lambda w: w not in stopWords)
FinalcleanWordsRDD = cleanWordsRDD.filter(lambda x: x != "")
IKVPairsRDD= FinalcleanWordsRDD.map(lambda word: (word,1))

```
   * reduceByKey() to get (word, count) results
```
wordsCountRDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)

```

```
results = wordsCountRDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
print(results)

```
   * collect() action to get back to Python
```
finalresults = wordsCountRDD.collect()
print(finalresults)

```

### Charting :bar_chart:
   * Plot Options... was used to configure the graph below.
   * The Key is Year and appears on the X-Axis.
   * The Series groupings is Product and there is a different color to denote each of those.
   * The Values is salesAmount and appears on the Y-Axis.
```
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

source = 'The Project Gutenberg eBook of The Iliad, by Homer'
title = 'Top 20 Words in ' + source
xlabel = 'Count (No.of occourences)'
ylabel = 'Words'

# create Pandas dataframe from list of tuples
df = pd.DataFrame.from_records(results, columns =[xlabel, ylabel]) 
print(df)

# create plot (using matplotlib)
plt.figure(figsize=(15,5))
sns.barplot(xlabel, ylabel, data=df, palette="rocket").set_title(title)

```   

## Results <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRvrf9cSoP1Bk3sRsAKjgzO6QRmooS6TLH2mg&usqp=CAU" width="40" height="35">

- ![Results for top 20 words](results.PNG)
- ![Results in bar graph](bargraph.PNG)

## References <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQXXH11khG_d4qz8TLEa1BANAnYX7iu4B8YOg&usqp=CAU" width="40" height="35">
- https://docs.databricks.com/_static/notebooks/higher-order-functions-tutorial-python.html


