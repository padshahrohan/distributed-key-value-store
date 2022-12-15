package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.healthcheck.HealthChecker;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class DynamoServerStarter implements ApplicationListener<ApplicationReadyEvent> {

    private final ApplicationArguments appArgs;
    private final HashManager<DynamoNode> hashManager;

    public DynamoServerStarter(ApplicationArguments appArgs, HashManager<DynamoNode> hashManager) {
        this.appArgs = appArgs;
        this.hashManager = hashManager;
    }

    @Override
    public void onApplicationEvent(@NotNull ApplicationReadyEvent applicationReadyEvent) {
        try {
            System.out.println(Arrays.toString(appArgs.getSourceArgs()));
            String[] args = appArgs.getSourceArgs()[0].split(",");
            boolean selfAware = true;
            for (String arg : args) {
                String address = arg.split("_")[1];
                String nodeNumber = arg.split("_")[0];
                DynamoNode node = new DynamoNode(address, selfAware, Integer.parseInt(nodeNumber));
                hashManager.addNode(node);
                selfAware = false;
            }
            System.out.println(hashManager.getRing());
            System.out.println(hashManager.getRing().size());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

    }

}