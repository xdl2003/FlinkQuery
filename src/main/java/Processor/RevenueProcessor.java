package Processor;

import model.DataEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Lineitem;
import pojo.Result;

import java.math.BigDecimal;

public class RevenueProcessor extends KeyedProcessFunction<String, DataEvent<Lineitem>, Result> {

    private ValueState<Result> resultState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        resultState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("AggregateProcessFunction" + "_result",
                        TypeInformation.of(Result.class)));
    }

    @Override
    public void processElement(DataEvent<Lineitem> lineitemDataEvent, KeyedProcessFunction<String, DataEvent<Lineitem>, Result>.Context context, Collector<Result> collector) throws Exception {
        if (resultState.value() == null) {
            resultState.update(new Result(lineitemDataEvent));
        }

        Result result = resultState.value();
        BigDecimal revenue = lineitemDataEvent.getDataObject().calculateRevenue();

        switch (lineitemDataEvent.getOperation()) {
            case AGGREGATE:
                result.addRevenue(revenue);
                System.out.println("AggregateProcessor: AGGRREGATE");
                break;
            case AGGREGATE_DELETE:
                result.subtractRevenue(revenue);
                System.out.println("AggregateProcessor: AGGRREGATE_DELETE");
                break;
            default:
                return;
        }

        resultState.update(result);
        collector.collect(result);
    }
}
