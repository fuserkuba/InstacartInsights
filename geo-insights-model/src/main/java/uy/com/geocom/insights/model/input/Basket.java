package uy.com.geocom.insights.model.input;

import lombok.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uy.com.geocom.insights.model.output.SegmentItem;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Cesta de compra
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Basket{
    protected String id;
    protected LocalDateTime time;
    protected Long clientId;
    protected Double charge;
    protected String paymentMethod;
    protected Long posId;

    public static StructType getSchema(){
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

