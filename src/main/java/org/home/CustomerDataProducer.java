package org.home;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.home.model.Account;
import org.home.model.Customer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class CustomerDataProducer {

    public static final String CUSTOMERS_TOPIC = "customers8";
    private static int customerId = 0;
    private static int accountId = 0;

    public void createCustomers() throws IOException, InterruptedException {
        long numberOfEvents = 5;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(
                props);

        List<String> names = getRandomNames();

        List<Customer> customerList = getCustomers(names);

        List<Account> accountList = getAccount(customerList);

        for (Customer customer : customerList) {
            String string = getCustomerAsString(customer);
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                    CUSTOMERS_TOPIC, customer.getId(), string);
            producer.send(record);
        }

        producer.close();
    }

    private synchronized List<Account> getAccount(List<Customer> customerList) {
        List<Account> accounts = new ArrayList<>();
        Random rand = new Random();
        for (Customer customer : customerList) {
            Account account = createSingleAccount(customerList.get(rand.nextInt(customerList.size())));
            accounts.add(account);
        }
        return accounts;
    }

    private Account createSingleAccount(Customer singleCustomer) {
        Random rand = new Random();
        Account account = new Account();
        account.setId(accountId++);
        String iban = "LU" + rand.nextInt(99) + " " + rand.nextInt(9999)  + " " + rand.nextInt(9999) + " " + rand.nextInt(9999) + " " + rand.nextInt(9999);
        account.setName(iban);
        account.setCustomerId(singleCustomer.getId());
        return account;
    }

    private List<Customer> getCustomers(List<String> names) {
        List<Customer> customers = new ArrayList<>();
        for (String name : names) {
            customers.add(createCustomer(name));
        }
        return customers;
    }

    private String getCustomerAsString(Object customer) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter stringWriter = new StringWriter();
        objectMapper.writeValue(stringWriter, customer);
        return stringWriter.toString();
    }

    private List<String> getRandomNames() throws IOException {
        File file = new File("src/main/resources/random_names.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String text;
        List<String> names = new ArrayList<>();

        while ((text = reader.readLine()) != null) {
            names.add(text);
        }
        return names;
    }

    private synchronized Customer createCustomer(String name) {
        Customer customer = new Customer();
        customer.setId(customerId++);
        customer.setName(name);
        return customer;

    }


    public static void main(String[] args) throws IOException, InterruptedException {
        CustomerDataProducer helloProducer = new CustomerDataProducer();
        helloProducer.createCustomers();
    }


}
