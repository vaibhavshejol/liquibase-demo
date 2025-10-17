package com.bnt.liquibasedemo;

import org.apache.commons.lang3.SystemProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LiquibaseDemoApplication {

	public static void main(String[] args) {
		System.setProperty("file.encoding", "UTF-8");
		java.nio.charset.Charset.defaultCharset();
		SpringApplication.run(LiquibaseDemoApplication.class, args);
		System.out.println("Default charset: " + java.nio.charset.Charset.defaultCharset());
		System.out.println("Default file en: " + SystemProperties.getProperty("file.encoding"));
	}

}
