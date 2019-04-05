package backup.web;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;

import backup.api.BackupWebService;
import backup.api.Stats;
import spark.ModelAndView;
import spark.ResponseTransformer;
import spark.Route;
import spark.Service;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class BackupWebServer {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ResponseTransformer HANDLER = model -> {
    if (model instanceof String) {
      return (String) model;
    } else {
      return OBJECT_MAPPER.writeValueAsString(model);
    }
  };

  public static void main(String[] args) {
    BackupWebService<Stats> service = getBackupWebServiceTest();
    BackupWebServer server = new BackupWebServer(9091, service);
    server.start();
  }

  private final Service service;
  private final int port;
  private final FreeMarkerEngine freeMarkerEngine;
  private final TemplateViewRoute templateViewRoute;
  private final BackupWebService<Stats> bws;

  public BackupWebServer(int port, BackupWebService<Stats> bws) {
    this.port = port;
    this.bws = bws;
    templateViewRoute = (TemplateViewRoute) (request, response) -> {
      Stats stats = bws.getStats();
      Map<String, Object> attributes = new HashMap<>();
      attributes.put("backupBytesPerSecond", getMBPerSecond(stats.getBackupBytesPerSecond()));
      attributes.put("backupsInProgressCount", stats.getBackupsInProgressCount());
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
    service.get("/runreport", (Route) (request, response) -> {
      bws.runGc(false);
      response.redirect("/");
      return null;
    }, HANDLER);
    service.get("/runreport-details", (Route) (request, response) -> {
      bws.runGc(true);
      response.redirect("/");
      return null;
    }, HANDLER);
    service.get("/stats", (request, response) -> bws.getStats(), HANDLER);
    service.get("/index.html", templateViewRoute, freeMarkerEngine);
    service.get("/", templateViewRoute, freeMarkerEngine);
    service.init();
  }

  public void stop() {
    service.stop();
  }

  private static String getMBPerSecond(double bytesPerSecond) {
    return NumberFormat.getInstance()
                       .format(bytesPerSecond / 1024 / 1024)
        + " MB/s";
  }

  private static FreeMarkerEngine getEngine() {
    return new FreeMarkerEngine();
  }

  private static BackupWebService<Stats> getBackupWebServiceTest() {
    return new BackupWebService<Stats>() {
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
          public int getBackupsInProgressCount() {
            return random.nextInt(10);
          }

          @Override
          public double getBackupBytesPerSecond() {
            return random.nextInt(Integer.MAX_VALUE);
          }
        };
      }

      @Override
      public void runGc(boolean debug) {
        System.out.println("Starting gc - " + debug);
      }

    };
  }

}
