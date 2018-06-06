package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class BasketSchema {
    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("time", DataTypes.DateType, false),
                DataTypes.createStructField("clientId", DataTypes.LongType, false),
                DataTypes.createStructField("paymentMethod", DataTypes.StringType, true),
                DataTypes.createStructField("posId", DataTypes.LongType, true),
                DataTypes.createStructField("charge", DataTypes.DoubleType, true)
        });
    }
}
