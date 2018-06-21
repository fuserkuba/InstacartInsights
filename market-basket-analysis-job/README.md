**Ejecutar el job en spark** (cargando los datos desde los csv)


./bin/spark-submit --class uy.com.geocom.insights.job.basketanalysis.BasketAnalysisSparkRunner  
geo-insights-jobs/market-basket-analysis-job/target/market-basket-analysis-job-1.0-SNAPSHOT-jar-with-dependencies.jar 
instacart4insights/products.csv 
instacart4insights/purchases.csv  
0.01  
0.1  
instacart4insights/itemsets.csv