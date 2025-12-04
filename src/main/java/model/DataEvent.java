package model;

import lombok.Getter;
import lombok.Setter;
import pojo.Customer;

/**
 * 表示从 input_data_all.csv 解析出的一个数据事件。
 * 包含操作类型、表名和具体的 POJO 对象。
 */
@Getter
@Setter
public class DataEvent<T> {

    public enum Operation {
        INSERT('+'),
        DELETE('-'),
        /** Mark an entity as alive (active) */
        SET_ALIVE('0'),

        /** Mark an entity as dead (inactive) */
        SET_DEAD('1'),

        /** Mark the left side of an entity as alive */
        SET_ALIVE_LEFT('2'),

        /** Mark the right side of an entity as alive */
        SET_ALIVE_RIGHT('3'),

        /** Mark the left side of an entity as dead */
        SET_DEAD_LEFT('4'),

        /** Mark the right side of an entity as dead */
        SET_DEAD_RIGHT('5'),

        /** Perform aggregation operation on entities */
        AGGREGATE('6'),

        /** Delete aggregated results */
        AGGREGATE_DELETE('7');

        private final char symbol;

        Operation(char symbol) {
            this.symbol = symbol;
        }

        public char getSymbol() {
            return symbol;
        }

        public static Operation fromSymbol(char symbol) {
            for (Operation op : Operation.values()) {
                if (op.symbol == symbol) {
                    return op;
                }
            }
            throw new IllegalArgumentException("Unknown operation symbol: " + symbol);
        }
    }

    @Setter
    @Getter
    private Operation operation;
    private String tableName;
    private final T dataObject;
    private String keyValue;

    public DataEvent(Operation operation, String tableName, T dataObject) {
        this.operation = operation;
        this.tableName = tableName;
        this.dataObject = dataObject;
    }


    @Override
    public String toString() {
        return "DataEvent{" +
                "operation=" + operation +
                ", tableName='" + tableName + '\'' +
                ", dataObject=" + dataObject +
                '}';
    }
}