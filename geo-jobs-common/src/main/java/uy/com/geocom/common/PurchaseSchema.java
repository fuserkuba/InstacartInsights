package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PurchaseSchema {

    public static final StructField basketId = DataTypes.createStructField("basketId", DataTypes.StringType, true);
    public static final StructField productId = DataTypes.createStructField("productId", DataTypes.StringType, false);
    public static final StructField units = DataTypes.createStructField("units", DataTypes.IntegerType, true);
    public static final StructField price = DataTypes.createStructField("price", DataTypes.DoubleType, true);
    public static final StructField context = DataTypes.createStructField("context", DataTypes.StringType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{basketId, productId, units, price, context});
    }
}
