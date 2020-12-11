package instacart.insights.dataextractor.articles;

import lombok.Data;

@Data
public class ArticleDTO {

    private String id;
    private String description;
    private String category;

    @Override
    public String toString() {
        return id + "," + description + "," + category;
    }
}
