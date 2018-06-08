package uy.com.geocom;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RFMSchema {
    public static final StructField clientId = DataTypes.createStructField("clientId", DataTypes.StringType, false);
    public static final StructField recency = DataTypes.createStructField("recency", DataTypes.LongType, false);
    public static final StructField frequency = DataTypes.createStructField("frequency", DataTypes.LongType, false);
    public static final StructField monetary = DataTypes.createStructField("monetary", DataTypes.DoubleType, false);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{clientId,recency, frequency, monetary});
    }
    public static String[] getDataColumns(){
        return  new String[]{recency.name(),frequency.name(),monetary.name()};
    }
}
