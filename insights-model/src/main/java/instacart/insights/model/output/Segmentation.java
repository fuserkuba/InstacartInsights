package instacart.insights.model.output;

import lombok.*;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Segmentation extends Insight {

    /**
     * The Silhouette is a measure for the validation of the consistency within clusters. It ranges
     *  * between 1 and -1, where a value close to 1 means that the points in a cluster are close to the
     *  * other points in the same cluster and far from the points of the other clusters.
     *
     * The cosine distance measure is defined as `1 - s` where `s` is the cosine similarity between two points.
     *
     *  The total distance of the point `X` to the points `$C_{i}$` belonging to the cluster `$\Gamma$` is:
     *
     *  <blockquote>
     *     $$
     *     \sum\limits_{i=1}^N d(X, C_{i} ) =
     *     \sum\limits_{i=1}^N \Big( 1 - \frac{\sum\limits_{j=1}^D x_{j}c_{ij} }{ \|X\|\|C_{i}\|} \Big)
     *     = \sum\limits_{i=1}^N 1 - \sum\limits_{i=1}^N \sum\limits_{j=1}^D \frac{x_{j}}{\|X\|}
     *     \frac{c_{ij}}{\|C_{i}\|}
     *     = N - \sum\limits_{j=1}^D \frac{x_{j}}{\|X\|} \Big( \sum\limits_{i=1}^N
     *    \frac{c_{ij}}{\|C_{i}\|} \Big)
     *     $$
     *  </blockquote>
     *
     *  where `$x_{j}$` is the `j`-th dimension of the point `X` and `$c_{ij}$` is the `j`-th dimension
     *  of the `i`-th point in cluster `$\Gamma$`.
     *
     * SquaredEuclideanSilhouette computes the average of the Silhouette over all the data of the dataset, which is
     *      *  a measure of how appropriately the data have been clustered.
     *      *
     *      *  The Silhouette for each point `i` is defined as:
     *      *
     *      *  <blockquote>
     *      *     $$
     *      *     s_{i} = \frac{b_{i}-a_{i}}{max\{a_{i},b_{i}\}}
     *      *     $$
     *      *   </blockquote>
     */
    protected double silhouetteValue;
    //Within Set Sum of Squared Errors
    protected double wssseValue;

    protected Map<String,Segment> segments;

    protected String segmentedDatasetPath;
}
