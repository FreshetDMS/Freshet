/**
 * Copyright 2017 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pathirage.freshet;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Utils {
  public static <T> T instantiate(final String className, final Class<T> type) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return type.cast(Class.forName(className).newInstance());
  }

  public static void executeUntilSuccessOrTimeout(final ExponentialBackOffFunction fn, final long startTimeMills, final long timeoutMills) {
    int i = 0;

    while ((System.currentTimeMillis() - startTimeMills) < timeoutMills) {
      if(fn.execute()) {
        return;
      }

      try {
        Thread.sleep(fibonacci(i) * 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Exponential back off thread interrupted.", e);
      }
    }

    throw new RuntimeException("Exponential back off timed out.");
  }

  public static long fibonacci(int n) {
    if (n <= 1) return n;
    else return fibonacci(n - 1) + fibonacci(n - 2);
  }

  public interface ExponentialBackOffFunction {
    boolean execute();
  }

  public static Properties getConsumerProperties(String brokers, String groupId) {
    Properties props = new Properties();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

    return props;
  }
}

