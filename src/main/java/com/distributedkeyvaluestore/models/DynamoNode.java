package com.distributedkeyvaluestore.models;

public class DynamoNode implements Node {

    private final String address;
    //TODO: Rename this
    private final boolean selfAware;
    private final int number;

    public DynamoNode(String address, boolean selfAware, int number) {
        this.number = number;
        ;
        this.address = address;
        this.selfAware = selfAware;
    }

    public String getAddress() {
        return address;
    }

    public boolean isSelfAware() {
        return selfAware;
    }

    public int getNumber() {
        return number;
    }

    @Override
    public String toString() {
        return "DynamoNode{" +
                "address='" + address + '\'' +
                '}';
    }
}
