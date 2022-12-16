/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.distributedkeyvaluestore.consistenthash;

import com.distributedkeyvaluestore.exception.RingEmptyException;
import com.distributedkeyvaluestore.models.Node;
import com.distributedkeyvaluestore.models.Quorum;
import jakarta.annotation.Nonnull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class for managing the hashing of nodes and data objects into nodes in a consistent manner
 *
 * @param <T> An object that extends the {@link Node} interface
 */
@Component
public class HashManager<T extends Node> {

    private final TreeMap<String, VirtualNode<T>> ring;
    private final HashFunction hashFunction;
    private final int vNodeCount;
    private List<T> allNodes;

    public HashManager(@Nonnull HashFunction hashFunction) {
        this.hashFunction = hashFunction;
        this.ring = new TreeMap<>();
        this.allNodes = new ArrayList<>();

        this.vNodeCount = 100;
    }

    /**
     * Adds a new physical node to the hash ring, with 100 virtual nodes
     *
     * @param pNode the physical node to be added to the ring
     */
    public void addNode(T pNode) {
        for (int i = 0; i < vNodeCount; i++) {
            VirtualNode<T> vNode = new VirtualNode<>(pNode, i);
            ring.put(hashFunction.hash(vNode.getAddress()), vNode);
        }
        allNodes.add(pNode);
    }

    /**
     * Method that returns all the nodes to which a data object will be hashed
     *
     * @param objectKey the key of the object to be hashed
     * @return list of nodes to which the object will be hashed
     */
    public ArrayList<T> getNodes(@Nonnull String objectKey) {
        if (ring.isEmpty()) {
            throw new RingEmptyException("Hash ring is empty");
        }
        String hash = hashFunction.hash(objectKey);
        SortedMap<String, VirtualNode<T>> tailMap = ring.tailMap(hash);
        ArrayList<T> nodesList = new ArrayList<>();
        int i = 0;

        String nodeHash = (!tailMap.isEmpty()) ? tailMap.firstKey() : ring.firstKey();
        VirtualNode<T> firstNode = ring.get(nodeHash);
        while (i < Quorum.getReplicas()) {
            VirtualNode<T> node = ring.get(nodeHash);
            if (!nodesList.contains(node.getPhysicalNode())) {
                // if physical node was not already added, add it
                nodesList.add(node.getPhysicalNode());
                i++;
            } else if (node == firstNode) {
                break;
            }

            // get key for next node; loop around if higherKey does not exist
            nodeHash = (ring.higherKey(nodeHash) != null ? ring.higherKey(nodeHash) : ring.firstKey());
        }

        return nodesList;
    }

    public boolean isRingCreated() {
        return !ring.isEmpty();
    }

    public TreeMap<String, VirtualNode<T>> getRing() {
        return ring;
    }

    public List<T> getAllNodes() {
        return allNodes;
    }
}
