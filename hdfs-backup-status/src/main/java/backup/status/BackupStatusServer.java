package backup.status;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;

import backup.api.Stats;
import backup.api.StatsService;
import spark.ModelAndView;
import spark.ResponseTransformer;
import spark.Service;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class BackupStatusServer {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ResponseTransformer HANDLER = model -> {
    if (model instanceof String) {
      return (String) model;
    } else {
      return OBJECT_MAPPER.writeValueAsString(model);
    }
  };

  public static void main(String[] args) {
    StatsService<Stats> statsService = getStatsService();
    BackupStatusServer server = new BackupStatusServer(9090, statsService);
    server.start();
  }

  private final Service service;
  private final int port;
  private final FreeMarkerEngine freeMarkerEngine;
  private final TemplateViewRoute templateViewRoute;
  private final StatsService<Stats> statsService;

  public BackupStatusServer(int port, StatsService<Stats> statsService) {
    this.port = port;
    this.statsService = statsService;
    templateViewRoute = (TemplateViewRoute) (request, response) -> {
      Stats stats = statsService.getStats();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("backupBytesPerSecond", getMBPerSecond(stats.getBackupBytesPerSecond()));
      attributes.put("backupsInProgressCount", stats.getBackupsInProgressCount());
      attributes.put("finalizedBlocksSizeCount", stats.getFinalizedBlocksSizeCount());
      attributes.put("futureChecksSizeCount", stats.getFutureChecksSizeCount());
      attributes.put("restoreBlocks", stats.getRestoreBlocks());
      attributes.put("restoreBytesPerSecond", getMBPerSecond(stats.getRestoreBytesPerSecond()));
      attributes.put("restoresInProgressCount", stats.getRestoresInProgressCount());
      return new ModelAndView(attributes, "index.ftl");
    };
    freeMarkerEngine = getEngine();
    service = Service.ignite();
  }

  public void start() {
    service.port(port);
    service.staticFileLocation("/hdfsbackupwebapp");
    service.get("/stats", (request, response) -> statsService.getStats(), HANDLER);
    service.get("/index.html", templateViewRoute, freeMarkerEngine);
    service.get("/", templateViewRoute, freeMarkerEngine);
    service.init();
  }

  public void stop() {
    service.stop();
  }

  private static String getMBPerSecond(double bytesPerSecond) {
    return NumberFormat.getInstance().format(bytesPerSecond / 1024 / 1024) + " MB/s";
  }

  private static FreeMarkerEngine getEngine() {
    return new FreeMarkerEngine();
  }

  private static StatsService<Stats> getStatsService() {
    return new StatsService<Stats>() {
      private Random random = new Random();

      @Override
      public Stats getStats() throws IOException {
        return new Stats() {

          @Override
          public int getRestoresInProgressCount() {
            return random.nextInt(10);
          }

          @Override
          public double getRestoreBytesPerSecond() {
            return random.nextInt(Integer.MAX_VALUE);
          }

          @Override
          public int getRestoreBlocks() {
            return random.nextInt(10);
          }

          @Override
          public int getFutureChecksSizeCount() {
            return random.nextInt(10);
          }

          @Override
          public int getFinalizedBlocksSizeCount() {
            return random.nextInt(10);
          }

          @Override
          public int getBackupsInProgressCount() {
            return random.nextInt(10);
          }

          @Override
          public double getBackupBytesPerSecond() {
            return random.nextInt(Integer.MAX_VALUE);
          }
        };
      }
    };
  }

}
