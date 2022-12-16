package com.distributedkeyvaluestore;

import com.distributedkeyvaluestore.healthcheck.HealthChecker;
import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import com.distributedkeyvaluestore.models.Quorum;
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
            System.out.println("Source arguments" + Arrays.toString(appArgs.getSourceArgs()));
            String[] args = appArgs.getSourceArgs()[0].split(",");
            boolean selfAware = true;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];

                if (i == 0) {
                    int replicas = Integer.parseInt(args[i]);
                    System.out.println("Replicas"+ replicas);
                    if (replicas <= args.length - 1) {
                        Quorum.setReplicas(replicas);
                    } else {
                        throw new IllegalArgumentException("Replicas should be less than number of nodes");
                    }
                    continue;
                }
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