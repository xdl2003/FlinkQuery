package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TPC-H Customer Table.
 * Primary Key: c_custkey
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    // 主键
    public long c_custkey;

    // 属性
    public String c_name;
    public String c_address;
    public long c_nationkey;     // 外键，引用 nation.n_nationkey
    public String c_phone;
    public double c_acctbal;
    public String c_mktsegment;
    public String c_comment;
}