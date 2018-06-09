package uy.com.geocom.insight.dataextractor.articles;

import lombok.Data;

@Data
public class CategoryDTO {

    private String id;
    private String name;
    private String description;

    @Override
    public String toString() {
        return id + "," + name + "," + description;
    }
}
