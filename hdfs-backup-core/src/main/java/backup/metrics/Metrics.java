package backup.metrics;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

public class Metrics {

  private final static Logger LOG = LoggerFactory.getLogger(Metrics.class);

  public static final MetricRegistry METRICS;
  public static final Slf4jReporter REPORTER;

  static {
    METRICS = new MetricRegistry();
    REPORTER = Slf4jReporter.forRegistry(METRICS)
                            .outputTo(LOG)
                            .convertRatesTo(TimeUnit.SECONDS)
                            .convertDurationsTo(TimeUnit.MILLISECONDS)
                            .build();
    REPORTER.start(1, TimeUnit.MINUTES);
  }

}
