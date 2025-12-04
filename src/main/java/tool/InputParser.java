package tool;

import model.DataEvent;
import pojo.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * 解析由 DataGenerator.py 生成的 input_data_all.csv 文件。
 * 每次调用 nextEvent() 返回一个 DataEvent 对象。
 */
public class InputParser {

    private final BufferedReader reader;
    private String currentLine;

    public InputParser(String filePath) throws IOException {
        this.reader = new BufferedReader(new FileReader(filePath));
        // 预读取第一行
        this.currentLine = reader.readLine();
    }

    /**
     * 读取下一行数据并解析为 DataEvent。
     * @return 解析后的 DataEvent，如果文件结束则返回 null。
     * @throws IOException 如果读取文件时发生错误。
     * @throws IllegalArgumentException 如果行格式不正确或无法解析数据。
     */
    public DataEvent<?> nextEvent() throws IOException {
        if (currentLine == null) {
            return null;
        }

        try {
            // 1. 检查行长度，至少要有 3 个字符的前缀
            if (currentLine.length() < 3) {
                throw new IllegalArgumentException("Line too short: " + currentLine);
            }

            // 2. 提取前 3 个字符作为 prefix (e.g., "+LI", "-OR")
            String prefix = currentLine.substring(0, 3); // 注意：是 0 到 3 (不包括 3)
            char opSymbol = prefix.charAt(0);
            String tablePrefix = prefix.substring(1, 3); // "LI", "OR"

            DataEvent.Operation operation = DataEvent.Operation.fromSymbol(opSymbol);

            // 3. 提取剩余部分作为原始数据
            // 从第 3 个字符开始截取
            String rawData = currentLine.substring(3); // 跳过 "+LI"

            // 4. 清理 rawData: 移除末尾的 "|\n"
            rawData = rawData.trim(); // 去除首尾空白（包括换行符）
            if (rawData.endsWith("|")) {
                rawData = rawData.substring(0, rawData.length() - 1);
            }

            // 5. 按 "|" 分割
            String[] fields = rawData.split("\\|");
            if (fields.length == 0 || fields[0].isEmpty()) {
                throw new IllegalArgumentException("No data fields found after parsing: " + currentLine);
            }

            // 6. 根据 tablePrefix 解析
            Object parsedObject;
            String tableName;

            switch (tablePrefix) {
                case "LI":
                    tableName = "lineitem";
                    parsedObject = parseLineitem(fields);
                    break;
                case "OR":
                    tableName = "orders";
                    parsedObject = parseOrders(fields);
                    break;
                case "CU":
                    tableName = "customer";
                    parsedObject = parseCustomer(fields);
                    break;
                // ... 其他表
                default:
                    currentLine = reader.readLine();
                    return nextEvent();
            }

            DataEvent<Object> event = new DataEvent<>(operation, tableName, parsedObject);

            // 预读下一行
            currentLine = reader.readLine();

            return event;

        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing line: " + currentLine, e);
        }
    }

    /**
     * 关闭文件读取器。
     */
    public void close() throws IOException {
        reader.close();
    }

    // ==================== 私有解析方法 ====================

    // 以下是解析每个表数据的私有方法。

    private Lineitem parseLineitem(String[] fields) {
        if (fields.length != 16) { // lineitem 有 16 个字段
            throw new IllegalArgumentException("Invalid lineitem line: " + Arrays.toString(fields));
        }

        Lineitem li = new Lineitem();
        li.l_orderkey = Long.parseLong(fields[0]);
        li.l_partkey = Long.parseLong(fields[1]);
        li.l_suppkey = Long.parseLong(fields[2]);
        li.l_linenumber = Integer.parseInt(fields[3]);
        li.l_quantity = Integer.parseInt(fields[4]);
        li.l_extendedprice = Double.parseDouble(fields[5]);
        li.l_discount = Double.parseDouble(fields[6]);
        li.l_tax = Double.parseDouble(fields[7]);
        li.l_returnflag = fields[8];
        li.l_linestatus = fields[9];
        li.l_shipdate = fields[10];
        li.l_commitdate = fields[11];
        li.l_receiptdate = fields[12];
        li.l_shipinstruct = fields[13];
        li.l_shipmode = fields[14];
        li.l_comment = fields[15];
        return li;
    }

    private Orders parseOrders(String[] fields) {
        if (fields.length != 9) {
            throw new IllegalArgumentException("Invalid orders line: " + Arrays.toString(fields));
        }
        Orders o = new Orders();
        o.o_orderkey = Long.parseLong(fields[0]);
        o.o_custkey = Long.parseLong(fields[1]);
        o.o_orderstatus = fields[2];
        o.o_totalprice = Double.parseDouble(fields[3]);
        o.o_orderdate = fields[4];
        o.o_orderpriority = fields[5];
        o.o_clerk = fields[6];
        o.o_shippriority = Integer.parseInt(fields[7]);
        o.o_comment = fields[8];
        return o;
    }

    private Customer parseCustomer(String[] fields) {
        if (fields.length != 8) {
            throw new IllegalArgumentException("Invalid customer line: " + Arrays.toString(fields));
        }
        Customer c = new Customer();
        c.c_custkey = Long.parseLong(fields[0]);
        c.c_name = fields[1];
        c.c_address = fields[2];
        c.c_nationkey = Long.parseLong(fields[3]);
        c.c_phone = fields[4];
        c.c_acctbal = Double.parseDouble(fields[5]);
        c.c_mktsegment = fields[6];
        c.c_comment = fields[7];
        return c;
    }
}
