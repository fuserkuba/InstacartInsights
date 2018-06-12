package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class BasketSchema {

    public static final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
    public static final StructField time = DataTypes.createStructField("time", DataTypes.DateType, false);
    public static final StructField clientId = DataTypes.createStructField("clientId", DataTypes.LongType, false);
    public static final StructField paymentMethod = DataTypes.createStructField("paymentMethod", DataTypes.StringType, true);
    public static final StructField posId = DataTypes.createStructField("posId", DataTypes.LongType, true);
    public static final StructField charge = DataTypes.createStructField("charge", DataTypes.DoubleType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{id, time, clientId, paymentMethod, posId, charge});
    }
}
