package com.distributedkeyvaluestore.healthcheck;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.cloud.openfeign.FeignClientsConfiguration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.net.URI;

@FeignClient(name = "healthChecker", url = "http://this-is-just-a-placeholder")
public interface HealthChecker {

    @GetMapping(value = "/healthCheck")
    ResponseEntity<String> check(URI baseUrl);
}
