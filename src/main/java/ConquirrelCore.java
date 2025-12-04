import Processor.*;
import datasource.CustomerSource;
import datasource.LineitemSource;
import datasource.OrderSource;
import pojo.Result;
import model.DataEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.Customer;
import pojo.Lineitem;
import pojo.Orders;

public class ConquirrelCore {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(8);
        
        // 处理数据源
        DataStream<DataEvent<Orders>> ordersStream = env.addSource(new OrderSource("G:/class/IP/FlinkQuery/new_data.csv"));
        DataStream<DataEvent<Customer>> customerStream = env.addSource(new CustomerSource("G:/class/IP/FlinkQuery/new_data.csv"));
        DataStream<DataEvent<Lineitem>> lineitemStream = env.addSource(new LineitemSource("G:/class/IP/FlinkQuery/new_data.csv"));

        // 处理 customer 数据，过滤出automobile market customers
        DataStream<DataEvent<Customer>> processedCustomerStream = customerStream
                .keyBy(c->String.valueOf(c.getDataObject().getC_custkey()))
                .process(new CustomerProcessor())
                .name("Customer Processor");

        // 合并orders
        DataStream<DataEvent<Orders>> processedOrdersStream = processedCustomerStream
                .connect(ordersStream)
                .keyBy(
                        c->String.valueOf(c.getDataObject().getC_custkey()),
                        o->String.valueOf(o.getDataObject().getO_custkey())
                )
                .process(new OrdersProcessor())
                .name("Orders Processor");

        // 合并lineitem和orders
        DataStream<DataEvent<Lineitem>> processedLineItemStream = processedOrdersStream
                .connect(lineitemStream)
                .keyBy(
                        DataEvent::getKeyValue,
                        l->String.valueOf(l.getDataObject().getL_orderkey())
                )
                .process(new LineitemProcessor())
                .name("Lineitem Processor");

        // 计算revenue
        DataStream<Result> RevenueStream = processedLineItemStream
                .keyBy(l->l.getKeyValue())
                .process(new RevenueProcessor())
                .name("Q3 Aggregator");

        // 结果sink
        RevenueStream.addSink(new SinkProcessor("./output.csv")).name("result sink");

        // 执行
        env.execute("TPC-H Q3 Query Processing");

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        System.out.println("====================================");
        System.out.println("✅ TPC-H Q3 Execution completed.");
        System.out.println("⏱️  Total execution time: " + durationMs + " ms");
        System.out.println("⏱️  (" + (durationMs / 1000.0) + " seconds)");
        System.out.println("====================================");
    }


}