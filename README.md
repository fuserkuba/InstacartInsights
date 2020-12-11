# Instacart Insights

Proof of Concept for [market basket analysis](https://towardsdatascience.com/a-gentle-introduction-on-market-basket-analysis-association-rules-fa4b986a40ce) 
and [customer segmentation](https://en.wikipedia.org/wiki/Market_segmentation) using [Apache Spark](https://spark.apache.org/)

## Jobs

* [etl-job](/etl-job/src/main/java/instacart/ETLInputData.java): Data wrangling with Spark 

* [segmentation-job](/segmentation-job/src/main/java/instacart/insights/job/segmentation/SegmentationEngine.java): Customer segmentation based on [RFM](https://en.wikipedia.org/wiki/RFM_(market_research)) 
using [K-Means](https://spark.apache.org/docs/latest/ml-clustering.html#k-means) 
from [Spark ML](https://spark.apache.org/docs/latest/ml-guide.html)

* [market-basket-analysis-job](market-basket-analysis-job/src/main/java/instacart/insights/job/basketanalysis/BasketAnalysisEngine.java): 
[Market basket analysis](https://towardsdatascience.com/a-gentle-introduction-on-market-basket-analysis-association-rules-fa4b986a40ce)
using [Frequent Pattern Mining](https://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html#fp-growth) 
from [Spark ML](https://spark.apache.org/docs/latest/ml-guide.html)


## Data
[Instacart](https://www.instacart.com/) open dataset with [3 million grocery orders](https://tech.instacart.com/3-million-instacart-orders-open-sourced-d40d29ead6f2)

## Acknowledgements

[Eduardo Aranda](https://github.com/earandap)
