package ma.enset.demospringcloudstreamskafka.services;


import ma.enset.demospringcloudstreamskafka.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {

    @Bean
    public Consumer<PageEvent> pageEventConsumer(){
        return (input)->{
            System.out.println("***************************");
            System.out.println(input.toString());
            System.out.println("***************************");

        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){
        return ()-> new PageEvent(
                Math.random()>0.5?"Page1":"Page2",
                Math.random()>0.5?"User1":"User2",
                new Date(),
                new Random().nextInt(9000));
    }
    @Bean
    public Function<PageEvent,PageEvent> pageEventFunction(){
        return (input)->{
            input.setName(input.getName().toUpperCase()+"-Function");
            input.setUser("UserFunction");
            return input;
        };
    }

    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> KStreamFunction(){
        return (input)->{
            return input
                    .filter((k,v)->v.getDuration()>100)
                    .map((k,v)->new KeyValue<>(v.getName(),v.getDuration()))
                    .groupByKey(Grouped.with(Serdes.String(),Serdes.Long()))
                    .windowedBy(TimeWindows.of(Duration.ofSeconds(3)))
                    .reduce((acc, v)->{long sum=(acc+v)/2; return sum;}, Materialized.as("stats-store"))
                    .toStream()
                    .map((k,v)->new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }
}
