package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.client.HealthChecker;
import com.distributedkeyvaluestore.client.URIHelper;
import com.distributedkeyvaluestore.consistenthash.HashingManager;
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
    private final HashingManager<DynamoNode> hashingManager;

    public DynamoServerStarter(ApplicationArguments appArgs, HealthChecker checker, HashingManager<DynamoNode> hashingManager) {
        this.appArgs = appArgs;
        this.checker = checker;
        this.hashingManager = hashingManager;
    }

    @Override
    public void onApplicationEvent(@NotNull ApplicationReadyEvent applicationReadyEvent) {
        //TODO: Create Dynamo nodes here from command line args
        //Ping all nodes to do a health check, if ping does not work properly dont start the app
        //Initialize hashing manager
        try {
            String[] args = appArgs.getSourceArgs()[0].split(",");
            boolean isCoordinator = true;
            for (String arg : args) {
                String address = arg.split("_")[1];
                String nodeNumber = arg.split("_")[0];
                DynamoNode node = new DynamoNode(address, isCoordinator, Integer.parseInt(nodeNumber));
                hashingManager.addNode(node);
                if (!node.isCoordinator()) {
                    //checker.check(URIHelper.createURI(address));
                }
                isCoordinator = false;
            }
        } catch (Exception e) {
            System.out.println("Unable to connect to dynamo nodes");
            System.exit(0);
        }

    }

}