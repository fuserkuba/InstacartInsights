package instacart.insights.dataextractor.payments;

import lombok.Data;

@Data
public class PaymentModeDTO {

    private String id;

    private String name;

    private String type;

    @Override
    public String toString() {
        return id + "," + name + "," + type;
    }
}
