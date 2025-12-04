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
import pojo.Lineitem;
import pojo.Orders;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashSet;

public class LineitemProcessor extends KeyedCoProcessFunction<String, DataEvent<Orders>, DataEvent<Lineitem>, DataEvent<Lineitem>> {

    /** State to store active line items */
    private ValueState<HashSet<DataEvent<Lineitem>>> aliveLineItemsState;

    /** State to count active orders */
    private ValueState<Integer> counterState;

    /** State to store the last active order */
    private ValueState<DataEvent<Orders>> lastAliveOrderState;

    private void initStates() throws IOException {
        if (counterState.value() == null) {
            counterState.update(0);
        }
        if (aliveLineItemsState.value() == null) {
            aliveLineItemsState.update(new HashSet<>());
        }
    }

    private String generateGroupKey(Lineitem lineitem) {
        return String.valueOf(lineitem.getL_orderkey()) + "|" +
                String.valueOf(lineitem.getO_orderdate()) + "|" +
                String.valueOf(lineitem.getO_shippriority());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        aliveLineItemsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("LineItemProcessFunction" + "_aliveLineItems",
                        TypeInformation.of(new TypeHint<HashSet<DataEvent<Lineitem>>>() {
                        })));

        counterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("LineItemProcessFunction" + "_counter",
                        IntegerTypeInfo.of(new TypeHint<Integer>() {})));

        lastAliveOrderState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("LineItemProcessFunction" + "_lastAliveOrder",
                        TypeInformation.of(new TypeHint<DataEvent<Orders>>() {})));
    }

    @Override
    public void processElement1(DataEvent<Orders> ordersDataEvent, KeyedCoProcessFunction<String, DataEvent<Orders>, DataEvent<Lineitem>, DataEvent<Lineitem>>.Context context, Collector<DataEvent<Lineitem>> collector) throws Exception {
        initStates();

        switch (ordersDataEvent.getOperation()) {
            case SET_ALIVE_RIGHT:
                // System.out.println("LineItemProcessor: SET_ALIVE_RIGHT");
                lastAliveOrderState.update(ordersDataEvent);
                counterState.update(counterState.value() + 1);

                for (DataEvent<Lineitem> lineitem : aliveLineItemsState.value()) {
                    Lineitem newLineitem = new Lineitem(lineitem.getDataObject(), ordersDataEvent.getDataObject());
                    DataEvent<Lineitem> newLineItemDE = new DataEvent<>(DataEvent.Operation.AGGREGATE, "lineitem", newLineitem);
                    newLineItemDE.setKeyValue(generateGroupKey(newLineitem));
                    collector.collect(newLineItemDE);
                }
                break;

            case SET_DEAD_RIGHT:
                // System.out.println("LineItemProcessor: SET_DEAD_RIGHT");
                lastAliveOrderState.update(null);
                counterState.update(counterState.value() - 1);

                for (DataEvent<Lineitem> lineitem : aliveLineItemsState.value()) {
                    Lineitem newLineitem = new Lineitem(lineitem.getDataObject(), ordersDataEvent.getDataObject());
                    DataEvent<Lineitem> newLineItemDE = new DataEvent<>(DataEvent.Operation.AGGREGATE_DELETE, "lineitem", newLineitem);
                    newLineItemDE.setKeyValue(generateGroupKey(newLineitem));
                    collector.collect(newLineItemDE);
                }
                break;

            default:

        }
    }

    @Override
    public void processElement2(DataEvent<Lineitem> lineitemDataEvent, KeyedCoProcessFunction<String, DataEvent<Orders>, DataEvent<Lineitem>, DataEvent<Lineitem>>.Context context, Collector<DataEvent<Lineitem>> collector) throws Exception {
        initStates();

        if (!LocalDate.parse(lineitemDataEvent.getDataObject().getL_shipdate()).isAfter(
                LocalDate.parse("1995-03-13"))) {
            return;
        }

        switch (lineitemDataEvent.getOperation()) {
            case INSERT:
                aliveLineItemsState.value().add(lineitemDataEvent);
                aliveLineItemsState.update(aliveLineItemsState.value());

                if (counterState.value() > 0 && lastAliveOrderState.value() != null) {
                    Lineitem newLineitem = new Lineitem(lineitemDataEvent.getDataObject(), lastAliveOrderState.value().getDataObject());
                    DataEvent<Lineitem> newLineItemDE = new DataEvent<>(DataEvent.Operation.AGGREGATE, "lineitem", newLineitem);
                    newLineItemDE.setKeyValue(generateGroupKey(newLineitem));
                    collector.collect(newLineItemDE);
                }
                break;

            case DELETE:
                if (counterState.value() > 0 && lastAliveOrderState.value() != null) {
                    Lineitem newLineitem = new Lineitem(lineitemDataEvent.getDataObject(), lastAliveOrderState.value().getDataObject());
                    DataEvent<Lineitem> newLineItemDE = new DataEvent<>(DataEvent.Operation.AGGREGATE_DELETE, "lineitem", newLineitem);
                    newLineItemDE.setKeyValue(generateGroupKey(newLineitem));
                    collector.collect(newLineItemDE);
                }

                aliveLineItemsState.value().remove(lineitemDataEvent);
                aliveLineItemsState.update(aliveLineItemsState.value());
                break;

            default:
        }
    }
}
