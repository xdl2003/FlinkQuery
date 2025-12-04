package pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * TPC-H Lineitem Table.
 * Primary Key: (l_orderkey, l_linenumber)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Lineitem {
    // 主键的一部分
    public long l_orderkey;       // 外键，引用 orders.o_orderkey

    // 主键
    public int l_linenumber;

    // 属性
    public long l_partkey;        // 外键，引用 part.p_partkey
    public long l_suppkey;        // 外键，引用 supplier.s_suppkey
    public int l_quantity;
    public double l_extendedprice;
    public double l_discount;
    public double l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public String l_shipdate;
    public String l_commitdate;
    public String l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;

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

    public Lineitem(Lineitem l, Orders o) {

        // 主键的一部分
        l_orderkey = l.l_orderkey;       // 外键，引用 orders.o_orderkey

        // 主键
        l_linenumber = l.l_linenumber;

        // 属性
        l_partkey = l.l_partkey;        // 外键，引用 part.p_partkey
        l_suppkey = l.l_suppkey;        // 外键，引用 supplier.s_suppkey
        l_quantity = l.l_quantity;
        l_extendedprice = l.l_extendedprice;
        l_discount = l.l_discount;
        l_tax = l.l_tax;
        l_returnflag = l.l_returnflag;
        l_linestatus = l.l_linestatus;
        l_shipdate = l.l_shipdate;
        l_commitdate = l.l_commitdate;
        l_receiptdate = l.l_receiptdate;
        l_shipinstruct = l.l_shipinstruct;
        l_shipmode = l.l_shipmode;
        l_comment = l.l_comment;

        o_orderkey = o.o_orderkey;
        o_orderstatus = o.o_orderstatus;
        o_totalprice = o.o_totalprice;
        o_orderdate = o.o_orderdate;
        o_orderpriority = o.o_orderpriority;
        o_clerk = o.o_clerk;
        o_shippriority = o.o_shippriority;
        o_comment = o.o_comment;

        // 主键
        c_custkey = o.c_custkey;

        // 属性
        c_name = o.c_name;
        c_address = o.c_address;
        c_nationkey = o.c_nationkey;     // 外键，引用 nation.n_nationkey
        c_phone = o.c_phone;
        c_acctbal = o.c_acctbal;
        c_mktsegment = o.c_mktsegment;
        c_comment = o.c_comment;
    }

    public BigDecimal calculateRevenue() {
        BigDecimal extendedPrice = new BigDecimal(l_extendedprice);
        BigDecimal discount = new BigDecimal(l_discount);
        return extendedPrice.multiply(BigDecimal.ONE.subtract(discount));
    }
}