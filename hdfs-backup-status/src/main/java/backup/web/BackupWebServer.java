package backup.web;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.ServletOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;

import backup.api.BackupWebService;
import backup.api.Stats;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
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
      attributes.put("reportIds", bws.listReports());
      return new ModelAndView(attributes, "index.ftl");
    };
    freeMarkerEngine = getEngine();
    service = Service.ignite();
  }

  public void start() {
    service.port(port);
    service.staticFileLocation("/hdfsbackupwebapp");
    service.get("/runreport", (Route) (request, response) -> {
      bws.runReport(false);
      response.redirect("/");
      return null;
    }, HANDLER);
    service.get("/runreport-details", (Route) (request, response) -> {
      bws.runReport(true);
      response.redirect("/");
      return null;
    }, HANDLER);
    service.get("/report/:report", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        String report = request.params("report");
        response.header("Content-Type", "text/plain; charset=us-ascii");
        byte[] buffer = new byte[1024];
        InputStream inputStream = null;
        try {
          inputStream = bws.getReport(report);
          if (inputStream == null) {
            service.halt(404);
          }
          try (ServletOutputStream outputStream = response.raw()
                                                          .getOutputStream()) {
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
              outputStream.write(buffer, 0, read);
            }
          }
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
        return null;
      }

    });
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
      public void runReport(boolean debug) {
        System.out.println("Starting report - " + debug);
      }

      @Override
      public List<String> listReports() throws IOException {
        return Arrays.asList("1234", "4567");
      }

      @Override
      public InputStream getReport(String id) throws IOException {
        return new ByteArrayInputStream((id + " this is the report").getBytes());
      }

    };
  }

}
