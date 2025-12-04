import model.DataEvent;
import tool.InputParser;

import java.io.IOException;

public class TestInputParser {
    public static void main(String[] args) {
        try {
            InputParser parser = new InputParser("G:/class/IP/FlinkQuery/input_data_all.csv");
            DataEvent<?> event;
            int count = 0;
            while ((event = parser.nextEvent()) != null && count < 100) { // 只打印前10个事件
                System.out.println(event);
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
