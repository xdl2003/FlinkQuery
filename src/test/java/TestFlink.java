import datasource.OrderSource;
import model.DataEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.Orders;

public class TestFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用自定义源
        DataStream<DataEvent<Orders>> dataStream = env.addSource(new OrderSource("G:/class/IP/FlinkQuery/input_data_all.csv"));

        // 过滤出 Orders 表的插入事件
        DataStream<DataEvent<Orders>> insertOrders = dataStream
                .filter(event -> "orders".equals(event.getTableName()) &&
                        event.getOperation() == DataEvent.Operation.INSERT)
                .map(event -> (DataEvent<Orders>) event); // 类型转换

        // 提取订单金额并求和（这是一个非常简化的聚合）
        DataStream<Double> totalRevenue = insertOrders
                // Step 1: 提取订单金额
                .map(new MapFunction<DataEvent<Orders>, Double>() {
                    @Override
                    public Double map(DataEvent<Orders> event) throws Exception {
                        return event.getDataObject().getO_totalprice(); // 使用 getter
                    }
                })
                .returns(Types.DOUBLE) // 明确返回类型
                // Step 2: 按固定键分组
                .keyBy(new KeySelector<Double, String>() {
                    @Override
                    public String getKey(Double value) throws Exception {
                        return "total"; // 所有金额归到同一个组
                    }
                })
                // Step 3: 在组内求和
                .sum(0); // 对 Double 流求和

        // 输出结果
        totalRevenue.print();

        // 执行
        env.execute("Simple Orders Revenue Job");
    }
}