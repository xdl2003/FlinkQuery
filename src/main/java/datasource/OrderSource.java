package datasource;

import model.DataEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import pojo.Orders;
import tool.InputParser;

public class OrderSource implements SourceFunction<DataEvent<Orders>> {
    private final String filePath;
    private volatile boolean isRunning = true; // 控制 run 方法的循环

    public OrderSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceFunction.SourceContext<DataEvent<Orders>> ctx) throws Exception {
        // 1. 创建一个 InputParser 来读取您的 CSV 文件
        try {
            InputParser parser = new InputParser(filePath);
            DataEvent<?> event;
            // 2. 循环读取，直到文件结束或作业被取消
            while (isRunning && (event = parser.nextEvent()) != null) {
                // 3. 使用 ctx.collect() 将解析出的事件发送到 Flink 流中
                if (event.getTableName().equals("orders")) {
                    ctx.collect((DataEvent<Orders>) event);
                }
            }
        } catch (Exception e) {
            System.out.println("Flink Source run failed");
        }
    }

    @Override
    public void cancel() {
        // 4. 当作业取消时，设置 isRunning 为 false，让 run() 方法的循环退出
        isRunning = false;
    }
}