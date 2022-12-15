package com.distributedkeyvaluestore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;

@SpringBootApplication
@EnableFeignClients
@ImportAutoConfiguration({FeignAutoConfiguration.class})
@EnableScheduling
public class DistributedKeyValueStoreApplication {
	public static void main(String[] args) {
		Properties properties = new Properties();
		new SpringApplicationBuilder(DistributedKeyValueStoreApplication.class).properties(properties).run(args);
	}

}
