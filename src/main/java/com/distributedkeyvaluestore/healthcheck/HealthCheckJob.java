package com.distributedkeyvaluestore.healthcheck;

import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

public class HealthCheckJob {

    private final HashManager<DynamoNode> hashManager;
    private final HealthChecker healthChecker;

    public HealthCheckJob(HashManager<DynamoNode> hashManager, HealthChecker healthChecker) {
        this.hashManager = hashManager;
        this.healthChecker = healthChecker;
    }

    @Scheduled(initialDelay = 30000, fixedDelay = 10000)
    public void doHealthCheck() {
        List<DynamoNode> allNodes = hashManager.getAllNodes();

        allNodes.forEach(node -> {
            try {
                if (!node.isSelfAware()) {
                    healthChecker.check(URIHelper.createURI(node.getAddress()));
                }
            } catch (Exception e) {
                System.out.println("Node with ip : " + node.getAddress() +" is down");
            }

        });
    }

}

