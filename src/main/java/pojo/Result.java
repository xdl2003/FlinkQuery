package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import model.DataEvent;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Result {
    private String orderKey;

    private String orderDate;

    private String shipPriority;

    private BigDecimal revenue;

    public Result(DataEvent<Lineitem> lineitemDataEvent) {
        // Step 1: Extract fields from the entity
        this.orderKey = String.valueOf(lineitemDataEvent.getDataObject().getL_orderkey());
        this.orderDate = String.valueOf(lineitemDataEvent.getDataObject().getO_orderdate());
        this.shipPriority = String.valueOf(lineitemDataEvent.getDataObject().getO_shippriority());

        // Step 2: Initialize revenue to zero
        this.revenue = BigDecimal.ZERO;
    }

    public void addRevenue(BigDecimal amount) {
        this.revenue = this.revenue.add(amount);
    }

    public void subtractRevenue(BigDecimal amount) {
        this.revenue = this.revenue.subtract(amount);
    }
}
