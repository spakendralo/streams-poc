package org.home;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.home.model.Account;
import org.home.model.AggregatedCustomer;
import org.home.model.Customer;
import org.home.serdes.JsonPOJODeserializer;
import org.home.serdes.JsonPOJOSerializer;

public class StreamsExercise {

    public static final String OUTPUT_TOPIC = "output8";

    public static void main(String[] args) throws Exception {

        Properties streamsConfiguration = new Properties();

        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-poc2");

        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<Customer> customerSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Customer.class);
        customerSerializer.configure(serdeProps, false);

        final Deserializer<Customer> customerDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Customer.class);
        customerDeserializer.configure(serdeProps, false);

        final Serde<Customer> customerSerde = Serdes.serdeFrom(customerSerializer, customerDeserializer);

        KTable<Integer, Customer> customersTable = builder.table(CustomerDataProducer.CUSTOMERS_TOPIC, Consumed.with(Serdes.Integer(), customerSerde));



        serdeProps = new HashMap<>();
        final Serializer<Account> accountSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Account.class);
        accountSerializer.configure(serdeProps, false);

        final Deserializer<Account> accountDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", Account.class);
        accountDeserializer.configure(serdeProps, false);

        final Serde<Account> accountSerde = Serdes.serdeFrom(accountSerializer, accountDeserializer);


        serdeProps = new HashMap<>();
        final Serializer<AggregatedCustomer> aggregatedCustomerSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AggregatedCustomer.class);
        aggregatedCustomerSerializer.configure(serdeProps, false);

        final Deserializer<AggregatedCustomer> aggregatedCustomerDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AggregatedCustomer.class);
        aggregatedCustomerDeserializer.configure(serdeProps, false);

        final Serde<AggregatedCustomer> aggregatedCustomerSerde = Serdes.serdeFrom(aggregatedCustomerSerializer, aggregatedCustomerDeserializer);


        KTable<String, Account> accounts = builder.table(AccountDataProducer.ACCOUNTS_TOPIC, Consumed.with(Serdes.String(), accountSerde));

        KStream<Integer, Account> accountsRekeyed = accounts.toStream().selectKey((s, account) -> account.getCustomerId());

        KTable<Integer, Account> accountsRekeyedTable = accountsRekeyed.groupByKey(Serialized.with(Serdes.Integer(), accountSerde)).reduce((account, v1) -> account);

        KTable<Integer, AggregatedCustomer> aggregatedCustomerKTable = customersTable.outerJoin(accountsRekeyedTable, (customer, account) -> new AggregatedCustomer(customer, account));

        KStream<Integer, AggregatedCustomer> aggregatedCustomerKStream = aggregatedCustomerKTable.toStream();
        aggregatedCustomerKStream.peek((integer, aggregatedCustomer) -> System.out.println(getString(aggregatedCustomer)));
        aggregatedCustomerKStream.to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), aggregatedCustomerSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    private static String getString(Object obj) {
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter stringWriter = new StringWriter();
        try {
            objectMapper.writeValue(stringWriter, obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringWriter.toString();
    }
}

