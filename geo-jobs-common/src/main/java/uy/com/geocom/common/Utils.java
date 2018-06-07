package uy.com.geocom.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Utils {

    public static Dataset<Row> readDataSetFromFile(SparkSession spark, String dataPath, StructType structType) {
        if (dataPath.contains(".csv")) {
            return spark.read()
                    .schema(structType)
                    .option("header", true)
                    .option("dateFormat", "yyyy-MM-dd")
                    .csv(dataPath);
        } else if (dataPath.contains(".json")) {
            return spark.read().json(dataPath);
        } else if (dataPath.contains(".parquet")) {
            return spark.read().parquet(dataPath);
        } else
            //Tab Separated Files (TSV)
            return spark.read().option("sep", "t").csv(dataPath);
    }

    public static void describeDataSet(Dataset dataset, String title, int rows) {
        //display schema of data
        System.out.println("------------".concat(title + " (").concat(dataset.count() + " rows) ------------"));
        dataset.printSchema();
        dataset.show(rows);
    }
}
