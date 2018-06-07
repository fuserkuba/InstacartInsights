package uy.com.geocom;

import uy.com.geocom.insights.model.output.Segmentation;

import java.util.Map;

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
