package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.client.HealthChecker;
import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class DynamoServerStarter implements ApplicationListener<ApplicationReadyEvent> {

    private final ApplicationArguments appArgs;
    private final HealthChecker checker;
    private final HashManager<DynamoNode> hashManager;

    public DynamoServerStarter(ApplicationArguments appArgs, HealthChecker checker, HashManager<DynamoNode> hashManager) {
        this.appArgs = appArgs;
        this.checker = checker;
        this.hashManager = hashManager;
    }

    @Override
    public void onApplicationEvent(@NotNull ApplicationReadyEvent applicationReadyEvent) {
        try {
            String[] args = appArgs.getSourceArgs()[0].split(",");
            boolean isCoordinator = true;
            for (String arg : args) {
                String address = arg.split("_")[1];
                String nodeNumber = arg.split("_")[0];
                DynamoNode node = new DynamoNode(address, isCoordinator, Integer.parseInt(nodeNumber));
                hashManager.addNode(node);
                if (!node.isCoordinator()) {
                    checker.check(URIHelper.createURI(address));
                }
                isCoordinator = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

}