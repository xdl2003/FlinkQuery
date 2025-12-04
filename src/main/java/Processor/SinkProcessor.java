package Processor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import pojo.Result;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.format.DateTimeFormatter;

public class SinkProcessor extends RichSinkFunction<Result> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // 输出文件路径
    private final String outputPath;
    private BufferedWriter writer;

    // 构造函数：传入输出路径
    public SinkProcessor(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化文件写入器
        this.writer = new BufferedWriter(new FileWriter(outputPath, true));
    }

    @Override
    public void invoke(Result result, Context context) throws Exception {
        if (result == null) {
            return;
        }

        try {
            String orderKey = result.getOrderKey();
            String orderDate = result.getOrderDate();
            String shipPriority = result.getShipPriority();
            BigDecimal revenue = result.getRevenue()
                    .setScale(4, RoundingMode.HALF_UP);

            // 格式化输出行
            String formattedOutput = String.format(
                    "%s, %s, %s, %s",
                    orderKey,
                    orderDate,
                    shipPriority,
                    revenue.toPlainString()
            );

            // 同步写入
            writer.write(formattedOutput);
            writer.newLine();
            writer.flush();

        } catch (Exception e) {
            throw e; // 让 Flink 捕获异常并失败任务
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                System.out.println("Error closing BufferedWriter");
            }
        }
        super.close();
    }
}