package com.distributedkeyvaluestore.consistenthash;

/**
 * Interface to be used for building a hash function
 */
public interface HashFunction {
    /**
     * Method to return the hash value for a given key
     *
     * @param key the key to be hashed
     * @return the hash value
     */
    String hash(String key);
}
