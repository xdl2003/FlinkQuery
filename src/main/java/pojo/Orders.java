package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TPC-H Orders Table.
 * Primary Key: o_orderkey
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Orders {
    // 主键
    public long o_orderkey;

    // 属性
    public long o_custkey;        // 外键，引用 customer.c_custkey
    public String o_orderstatus;
    public double o_totalprice;
    public String o_orderdate;
    public String o_orderpriority;
    public String o_clerk;
    public int o_shippriority;
    public String o_comment;

    // Customer属性
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

    public Orders(Orders o, Customer c) {
        o_orderkey = o.o_orderkey;
        o_orderstatus = o.o_orderstatus;
        o_totalprice = o.o_totalprice;
        o_orderdate = o.o_orderdate;
        o_orderpriority = o.o_orderpriority;
        o_clerk = o.o_clerk;
        o_shippriority = o.o_shippriority;
        o_comment = o.o_comment;

        // 主键
        c_custkey = c.c_custkey;

        // 属性
        c_name = c.c_name;
        c_address = c.c_address;
        c_nationkey = c.c_nationkey;     // 外键，引用 nation.n_nationkey
        c_phone = c.c_phone;
        c_acctbal = c.c_acctbal;
        c_mktsegment = c.c_mktsegment;
        c_comment = c.c_comment;
    }
}
