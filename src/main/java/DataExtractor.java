import java.io.*;
import java.util.ArrayList;
import java.util.Random;

/**
 * 从原始CSV数据中提取指定表（LI、OR、CU）的记录，总共提取11435条并保存到新文件
 */
public class DataExtractor {
    // 需要提取的总记录数量
    private static final int TOTAL_MAX_RECORDS = 10110000;
    // 目标表前缀
    private static final String[] TARGET_PREFIXES = {"LI", "OR", "CU"};

    /**
     * 提取指定表的记录，直到达到总记录数
     *
     * @param inputFilePath  原始输入文件路径
     * @param outputFilePath 输出文件路径
     * @throws IOException 如果文件操作发生错误
     */
    public void extract(String inputFilePath, String outputFilePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            Random random = new Random();
            String line;
            int extractedCount = 0;
            ArrayList<String> arr = new ArrayList<>();
            // 读取并处理每条记录
            while ((line = reader.readLine()) != null) {
                // 验证行长度是否足够提取前缀
                if (line.length() < 3) {
                    continue; // 跳过无效行
                }

                // 提取表前缀（格式为 "+XX" 或 "-XX"，取后两位）
                String tablePrefix = line.substring(1, 3);

                // 检查是否为目标表前缀
                if (isTargetPrefix(tablePrefix)) {
                    // 写入记录
                    writer.write(line);
                    writer.newLine();
                    extractedCount++;
                    arr.add(line);                    // 每四条尝试删除前面的一条
                    if (extractedCount % 4 == 0) {
                        int k = random.nextInt(extractedCount - 1);
                        line = arr.get(k);
                        if (!line.isEmpty()) {
                            line = "-" + line.substring(1);
                            writer.write(line);
                            writer.newLine();
                            arr.set(k, "");
                        }
                    }
                    // 每提取1000条打印一次进度
                    if (extractedCount % 1000 == 0) {
                        System.out.println("已提取 " + extractedCount + " 条记录...");
                    }
                }
            }

            // 打印提取统计信息
            System.out.println("\n数据提取完成，共提取 " + extractedCount + " 条记录");
            if (extractedCount < TOTAL_MAX_RECORDS) {
                System.out.println("警告：原始文件中符合条件的记录不足" + TOTAL_MAX_RECORDS + "条");
            }
        }
    }

    /**
     * 检查是否为目标表前缀
     */
    private boolean isTargetPrefix(String prefix) {
        for (String target : TARGET_PREFIXES) {
            if (target.equals(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        // 示例用法
        String inputPath = "G:/class/IP/FlinkQuery/input_data_all.csv";    // 原始数据文件路径
        String outputPath = "G:/class/IP/FlinkQuery/new_data.csv";  // 提取后的文件路径

        DataExtractor extractor = new DataExtractor();
        try {
            extractor.extract(inputPath, outputPath);
            System.out.println("数据已成功提取到：" + outputPath);
        } catch (IOException e) {
            System.err.println("提取数据时发生错误：" + e.getMessage());
            e.printStackTrace();
        }
    }
}