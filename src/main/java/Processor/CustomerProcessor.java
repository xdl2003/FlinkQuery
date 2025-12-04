package Processor;

import model.DataEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Customer;

public class CustomerProcessor extends KeyedProcessFunction<String, DataEvent<Customer>, DataEvent<Customer>> {

    @Override
    public void processElement(DataEvent<Customer> customerDataEvent, KeyedProcessFunction<String, DataEvent<Customer>, DataEvent<Customer>>.Context context, Collector<DataEvent<Customer>> collector) throws Exception {
        // 过滤AUTOMOBILE
        if (customerDataEvent.getDataObject().c_mktsegment.equals("AUTOMOBILE")) {
            switch (customerDataEvent.getOperation()) {
                case INSERT:
                    customerDataEvent.setOperation(DataEvent.Operation.SET_ALIVE);
                    customerDataEvent.setKeyValue(String.valueOf(customerDataEvent.getDataObject().c_custkey));
                    collector.collect(customerDataEvent);
                    // System.out.println("CustomProcessor: INSERT");
                    break;
                case DELETE:
                    customerDataEvent.setOperation(DataEvent.Operation.SET_DEAD);
                    customerDataEvent.setKeyValue(String.valueOf(customerDataEvent.getDataObject().c_custkey));
                    collector.collect(customerDataEvent);
                    // System.out.println("CustomProcessor: DELETE");
                    break;
                default:

            }
        }
    }
}
