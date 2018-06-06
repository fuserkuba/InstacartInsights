package uy.com.geocom.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uy.com.geocom.insights.model.output.SegmentItem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO: Incorporar otra información demográfica de interés
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Client  implements SegmentItem {
    protected String id;
    protected String sex;
    protected Integer age;
    protected String locality;
    @Override
    public Map<String, String> getFeatures() {
        Map<String, String> features=new HashMap<String, String>();
        features.put("id",id);
        features.put("sex",sex);
        features.put("age",age.toString());
        features.put("locality",locality);
        return features;
    }

    public static StructType getSchema(){
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("sex", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("locality", DataTypes.StringType, true),
        });
    }
}
