package uy.com.geocom.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * TODO: Incorporar otra información demográfica de interés
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Client {
    protected String id;
    protected String sex;
    protected Integer age;
    protected String locality;
}
