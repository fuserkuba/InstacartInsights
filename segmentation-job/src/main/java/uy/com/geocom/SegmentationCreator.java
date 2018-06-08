package uy.com.geocom;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import uy.com.geocom.insights.model.output.Segmentation;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SegmentationCreator {
    private Segmentation segmentationInsight;



    public Segmentation createSegmentationInsightFromClustering(Map<String,String> params, String[] datasetPaths){
        //TODO: obtener resultados y ltransformarlos al formato de Segmentation
        return segmentationInsight;
    }

    public Segmentation getSegmentationInsight() {
        return segmentationInsight;
    }
}
