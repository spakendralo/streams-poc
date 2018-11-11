package org.home.model;

public class AggregatedCustomer {
    private Customer customer;
    private Account account;

    public AggregatedCustomer(Customer customer, Account account) {
        this.customer = customer;
        this.account = account;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Account getAccount() {
        return account;
    }

    public void setAccount(Account account) {
        this.account = account;
    }
}
