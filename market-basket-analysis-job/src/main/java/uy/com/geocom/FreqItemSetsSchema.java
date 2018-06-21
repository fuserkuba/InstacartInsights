package uy.com.geocom;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FreqItemSetsSchema {
    public static final StructField items = DataTypes.createStructField("items", DataTypes.StringType, true);
    public static final StructField freq = DataTypes.createStructField("freq", DataTypes.StringType, true);
    public static final StructField rank = DataTypes.createStructField("rank", DataTypes.IntegerType, true);


    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{items, freq});
    }
}
