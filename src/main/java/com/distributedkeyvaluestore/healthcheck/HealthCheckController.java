package com.distributedkeyvaluestore.healthcheck;

import com.distributedkeyvaluestore.consistenthash.HashManager;
import com.distributedkeyvaluestore.models.DynamoNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/healthCheck")
public class HealthCheckController {

    private final HashManager<DynamoNode> hashManager;

    public HealthCheckController(HashManager<DynamoNode> hashManager) {
        this.hashManager = hashManager;
    }

    @GetMapping
    ResponseEntity<String> healthCheck() {
        Optional<DynamoNode> first = hashManager.getAllNodes().stream().filter(DynamoNode::isSelfAware).findFirst();
        if (first.isPresent()) {
            if (hashManager.isRingCreated()) {
                return ResponseEntity.ok("Node with ip " + first.get().getAddress() + " is running");
            }
            return ResponseEntity.badRequest().body("Node with ip " + first.get().getAddress() + "is down");
        }
        return ResponseEntity.badRequest().body("Node with is down");
    }

}
