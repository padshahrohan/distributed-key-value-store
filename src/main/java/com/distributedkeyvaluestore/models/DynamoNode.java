package com.distributedkeyvaluestore.models;

public class DynamoNode implements Node {

    private final String address;
    private final boolean isCoordinator;
    private final int number;

    public DynamoNode(String address, boolean isCoordinator, int number) {
        this.number = number;
        ;
        this.address = address;
        this.isCoordinator = isCoordinator;
    }

    public String getAddress() {
        return address;
    }

    public boolean isCoordinator() {
        return isCoordinator;
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
