package com.tn.service.data.jdbc;

import static com.tn.service.PropertyLogger.REGEX_PASSWORD;
import static com.tn.service.PropertyLogger.REGEX_SECRET;
import static com.tn.service.PropertyLogger.sensitive;

import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.tn.service.PropertyLogger;

@SpringBootApplication
public class Application
{
  public static void main(String[] args)
  {
    PropertyLogger propertyLogger = new PropertyLogger(
      LoggerFactory.getLogger(Application.class),
      sensitive(REGEX_PASSWORD),
      sensitive(REGEX_SECRET)
    );

    var application = new SpringApplication(Application.class);
    application.addListeners(propertyLogger);
    application.run(args);
  }
}
