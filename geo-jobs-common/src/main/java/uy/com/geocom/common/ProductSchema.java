package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ProductSchema {
    public static StructType getSchema(){
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("department", DataTypes.StringType, true),
                DataTypes.createStructField("type", DataTypes.StringType, true),
                DataTypes.createStructField("brand", DataTypes.StringType, true),
                DataTypes.createStructField("profit", DataTypes.DoubleType, true),
        });
    }
}
