package uy.com.geocom.insights.job.segmentation.rfm;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


public class RFMSchema {
    public static final StructField clientId = DataTypes.createStructField("clientId", DataTypes.StringType, false);
    public static final StructField recency = DataTypes.createStructField("recency", DataTypes.LongType, false);
    public static final StructField frequency = DataTypes.createStructField("frequency", DataTypes.LongType, false);
    public static final StructField monetary = DataTypes.createStructField("monetary", DataTypes.DoubleType, false);


    public static String[] getDataColumns() {
        return new String[]{recency.name(), frequency.name(), monetary.name()};
    }


}
