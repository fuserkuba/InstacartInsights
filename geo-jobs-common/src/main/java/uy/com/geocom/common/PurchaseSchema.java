package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PurchaseSchema {
    public static StructType getSchema(){
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("productId", DataTypes.StringType, false),
                DataTypes.createStructField("basketId", DataTypes.StringType, true),
                DataTypes.createStructField("units", DataTypes.IntegerType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, true),
                DataTypes.createStructField("context", DataTypes.StringType, true)
        });
    }
}
