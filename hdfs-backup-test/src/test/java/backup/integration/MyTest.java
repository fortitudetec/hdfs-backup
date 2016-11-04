package backup.integration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.BeforeClass;
import org.junit.Test;

public class MyTest {

  public static BlockingQueue<Object> queue = new LinkedBlockingQueue<>();

  private static Object o;

  @BeforeClass
  public static void setup() throws InterruptedException {
    o = queue.take();
  }

  @Test
  public void test1() {
    System.out.println(o);
  }

  @Test
  public void test2() {
    System.out.println(o);
  }

  @Test
  public void test3() {
    System.out.println(o);
  }

}
