package com.distributedkeyvaluestore.models;

/**
 * Class which defines global definition of number of replicas, read quorum and write quorum
 */
public class Quorum {

    private static int replicas;

    private Quorum() {

    }

    public static void setReplicas(int r) {
        if (replicas == 0) {
            replicas = r;
        }
    }

    /**
     * Method to return the number of nodes into which
     * an object is to be hashed (and read from)
     *
     * @return no. of nodes into which an object is attempted to be hashed
     */
    public static int getReplicas() {
        return replicas;
    }

    /**
     * Method to return the number of nodes from which a read request
     * must be successful to return a success message
     *
     * @return read quorum
     */
    public static int getReadQuorum() {
        return replicas - 1;
    }

    /**
     * Method to return the number of nodes into which a write must
     * be successfully performed to return a success message
     *
     * @return write quorum
     */
    public static int getWriteQuorum() {
        return replicas - 1;
    }
}
