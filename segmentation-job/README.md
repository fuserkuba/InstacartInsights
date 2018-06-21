**Ejecutar el job en spark** (cargando los datos desde los csv)


./bin/spark-submit --class uy.com.geocom.insights.job.segmentation.SegmentationEngine 
geo-insights-jobs/segmentation-job/target/segmentation-job-1.0-SNAPSHOT-jar-with-dependencies.jar 
/mnt/data/MINING/instacart4insights/baskets.csv 
/mnt/data/MINING/instacart4insights/clients.csv 

