package backup.namenode;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import backup.namenode.ipc.NameNodeBackupRPC;
import backup.namenode.ipc.Stats;

public class NameNodeStatusServer implements Closeable {

  private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
  private static final String TEST_HTML = "text/html";
  private static final String APPLICATION_JSON = "application/json";

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    NameNodeBackupRPC nameNodeBackupRPC = NameNodeBackupRPC.getNameNodeBackupRPC("c-sao-1", conf);
    try (NameNodeStatusServer server = new NameNodeStatusServer(nameNodeBackupRPC, 6001)) {
      server.start();
      server.join();
    }
  }

  private final NameNodeBackupRPC nameNodeBackupRPC;
  private final ObjectMapper mapper = new ObjectMapper();
  private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);
  private final Server server;

  public NameNodeStatusServer(NameNodeBackupRPC nameNodeBackupRPC, int port) {
    this.nameNodeBackupRPC = nameNodeBackupRPC;
    this.server = new Server(port);
  }

  @SuppressWarnings("serial")
  public void start() throws Exception {
    WebAppContext context = new WebAppContext();
    context.setWar(getClass().getResource("/hdfsbackupwebapp")
                             .toString());
    context.setContextPath("/");
    context.setParentLoaderPriority(true);
    context.addServlet(new ServletHolder(new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Stats stats = nameNodeBackupRPC.getStats();
        String json = mapper.writeValueAsString(stats);
        resp.setContentType(APPLICATION_JSON);
        resp.setContentLength(json.length());
        try (PrintWriter writer = resp.getWriter()) {
          writer.print(json);
        }
      }
    }), "/stats");
    context.addServlet(new ServletHolder(new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Stats stats = nameNodeBackupRPC.getStats();

        StringBuilder builder = new StringBuilder();

        builder.append("<html>")
               .append("<head>")
               .append("<title>Backup Stats</title>")
               .append("<meta http-equiv=\"refresh\" content=\"1\">")
               .append("<meta charset=\"utf-8\">")
               .append("<link href=\"/hdfsbackupwebapp/css/bootstrap.min.css\" rel=\"stylesheet\">")
               .append("<link href=\"/hdfsbackupwebapp/css/bs-docs.css\" rel=\"stylesheet\" media=\"screen\">")
               .append("</head>")
               .append("<script src=\"/hdfsbackupwebapp/js/jquery-1.9.1.min.js\"></script>")
               .append("<body>")
               .append(getNow())
               .append("<br>")
               .append("<table class=\"table-bordered table-condensed\">")
               .append("<tbody>");

        Field[] declaredFields = stats.getClass()
                                      .getDeclaredFields();
        for (Field field : declaredFields) {
          try {
            append(builder, field, stats);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
        builder.append("</tbody></table></body></html>");

        String html = builder.toString();
        resp.setContentType(TEST_HTML);
        resp.setContentLength(html.length());
        try (PrintWriter writer = resp.getWriter()) {
          writer.print(html);
        }
      }

      private String getNow() {
        synchronized (simpleDateFormat) {
          return simpleDateFormat.format(new Date());
        }
      }

      private void append(StringBuilder builder, Field field, Stats stats) throws Exception {
        field.setAccessible(true);
        Object object = field.get(stats);
        builder.append("<tr>")
               .append("<td>")
               .append(field.getName())
               .append("</td>")
               .append("<td>")
               .append(object)
               .append("</td>")
               .append("</tr>");
      }
    }), "/stats.html");
    server.setHandler(context);

    server.start();
  }

  public void join() throws Exception {
    if (server != null) {
      server.join();
    }
  }

  @Override
  public void close() throws IOException {
    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        throw new IOException();
      }
    }
  }

}
