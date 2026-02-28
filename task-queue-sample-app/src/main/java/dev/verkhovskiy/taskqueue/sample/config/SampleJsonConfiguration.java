package dev.verkhovskiy.taskqueue.sample.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Fallback-конфигурация JSON для sample-приложения. */
@Configuration
public class SampleJsonConfiguration {

  /**
   * Регистрирует {@link ObjectMapper}, если он не был создан автоконфигурацией.
   *
   * @return экземпляр object mapper
   */
  @Bean
  @ConditionalOnMissingBean(ObjectMapper.class)
  public ObjectMapper objectMapper() {
    return new ObjectMapper().findAndRegisterModules();
  }
}
