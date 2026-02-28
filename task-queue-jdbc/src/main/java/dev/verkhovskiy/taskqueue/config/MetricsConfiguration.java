package dev.verkhovskiy.taskqueue.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Конфигурация метрик приложения. */
@Configuration
public class MetricsConfiguration {

  /**
   * Создает {@link MeterRegistry}, если он не был предоставлен извне.
   *
   * @return реестр метрик по умолчанию
   */
  @Bean
  @ConditionalOnMissingBean(MeterRegistry.class)
  public MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
  }
}
