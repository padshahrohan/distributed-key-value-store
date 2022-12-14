package com.distributedkeyvaluestore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.cloud.openfeign.FeignAutoConfiguration;

import java.util.Properties;

@SpringBootApplication
@EnableFeignClients
@ImportAutoConfiguration({FeignAutoConfiguration.class})
public class DistributedKeyValueStoreApplication {
	public static void main(String[] args) {
		String ipAddress = args[0].split("_")[1];
		System.out.println(ipAddress);
		Properties properties = new Properties();
		properties.put("server.address", ipAddress);
		new SpringApplicationBuilder(DistributedKeyValueStoreApplication.class).properties(properties).run(args);
	}

}
