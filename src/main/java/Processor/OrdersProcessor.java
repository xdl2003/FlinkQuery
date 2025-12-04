package Processor;

import model.DataEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Customer;
import pojo.Orders;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashSet;

public class OrdersProcessor extends KeyedCoProcessFunction<String, DataEvent<Customer>, DataEvent<Orders>, DataEvent<Orders>> {

    private ValueState<HashSet<DataEvent<Orders>>> aliveOrdersState;

    private ValueState<Integer> counterState;

    private ValueState<DataEvent<Customer>> lastAliveCustomerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        aliveOrdersState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("OrdersProcessFunction" + "_aliveOrders",
                        TypeInformation.of(new TypeHint<HashSet<DataEvent<Orders>>>() {
                        })));

        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("OrdersProcessFunction" + "_counter",
                        IntegerTypeInfo.INT_TYPE_INFO));

        lastAliveCustomerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("OrdersProcessFunction" + "_lastAliveCustomer",
                        TypeInformation.of(new TypeHint<DataEvent<Customer>>() {
                        })));
    }

    void initStates() throws IOException {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (aliveOrdersState.value() == null) {
            aliveOrdersState.update(new HashSet<>());
        }
    }

    @Override
    public void processElement1(DataEvent<Customer> customerDataEvent, KeyedCoProcessFunction<String, DataEvent<Customer>, DataEvent<Orders>, DataEvent<Orders>>.Context context, Collector<DataEvent<Orders>> collector) throws Exception {
        initStates();
        switch (customerDataEvent.getOperation()) {
            case SET_ALIVE:
                // System.out.println("OrderProcessor: SET_ALIVE");
                lastAliveCustomerState.update(customerDataEvent);
                counterState.update(counterState.value() + 1);

                for (DataEvent<Orders> order : aliveOrdersState.value()) {
                    Orders newOrder = new Orders(order.getDataObject(), customerDataEvent.getDataObject());
                    DataEvent<Orders> newOrderDE= new DataEvent<Orders>(DataEvent.Operation.SET_ALIVE_RIGHT, "orders", newOrder);
                    newOrderDE.setKeyValue(String.valueOf(newOrder.o_orderkey));
                    collector.collect(newOrderDE);
                }
                break;

            case SET_DEAD:
                // System.out.println("OrderProcessor: SET_DEAD");
                lastAliveCustomerState.update(null);
                counterState.update(counterState.value() - 1);

                for (DataEvent<Orders> order : aliveOrdersState.value()) {
                    Orders newOrder = new Orders(order.getDataObject(), customerDataEvent.getDataObject());
                    DataEvent<Orders> newOrderDE= new DataEvent<Orders>(DataEvent.Operation.SET_DEAD_RIGHT, "orders", newOrder);
                    newOrderDE.setKeyValue(String.valueOf(newOrder.o_orderkey));
                    collector.collect(newOrderDE);
                }
                break;

            default:
        }
    }

    @Override
    public void processElement2(DataEvent<Orders> ordersDataEvent, KeyedCoProcessFunction<String, DataEvent<Customer>, DataEvent<Orders>, DataEvent<Orders>>.Context context, Collector<DataEvent<Orders>> collector) throws Exception {
        initStates();

        if (!LocalDate.parse(ordersDataEvent.getDataObject().o_orderdate).isBefore(
                LocalDate.parse("1995-03-13"))) {
            return;
        }

        switch (ordersDataEvent.getOperation()) {
            case INSERT:
                // System.out.println("OrderProcessor: INSERT");
                aliveOrdersState.value().add(ordersDataEvent);
                aliveOrdersState.update(aliveOrdersState.value());

                if (counterState.value() > 0 && lastAliveCustomerState.value() != null) {
                    Orders newOrder = new Orders(ordersDataEvent.getDataObject(), lastAliveCustomerState.value().getDataObject());
                    DataEvent<Orders> newOrderDE= new DataEvent<Orders>(DataEvent.Operation.SET_ALIVE_RIGHT, "orders", newOrder);
                    newOrderDE.setKeyValue(String.valueOf(newOrder.o_orderkey));
                    collector.collect(newOrderDE);
                }
                break;

            case DELETE:
                if (counterState.value() > 0 && lastAliveCustomerState.value() != null) {
                    Orders newOrder = new Orders(ordersDataEvent.getDataObject(), lastAliveCustomerState.value().getDataObject());
                    DataEvent<Orders> newOrderDE= new DataEvent<Orders>(DataEvent.Operation.SET_DEAD_RIGHT, "orders", newOrder);
                    newOrderDE.setKeyValue(String.valueOf(newOrder.o_orderkey));
                    collector.collect(newOrderDE);
                }

                aliveOrdersState.value().remove(ordersDataEvent);
                aliveOrdersState.update(aliveOrdersState.value());
                break;

            default:
        }
    }
}
