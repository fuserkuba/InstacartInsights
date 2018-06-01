package uy.com.geocom.insights.model;

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
    protected long id;
    protected String sex;
    protected int age;
    protected String occupation;
}
