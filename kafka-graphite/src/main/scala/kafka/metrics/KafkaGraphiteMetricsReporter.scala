/**
 * Copyright 2015 Alexander Pakulov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.metrics

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.yammer.metrics.Metrics
import com.yammer.metrics.core._
import com.yammer.metrics.reporting.{SocketProvider, GraphiteReporter}
import com.yammer.metrics.reporting.GraphiteReporter.DefaultSocketProvider
import kafka.utils.{VerifiableProperties, Logging}

trait KafkaGraphiteMetricsReporterMBean extends KafkaMetricsReporterMBean

object PartitionPattern {

  var expression = "^(.*)partition\\.(\\d+)\\.topic\\.([a-zA-Z0-9_]+)$";
  var pattern = Pattern.compile(expression)

}

class KafkaGraphiteMetricsReporter extends KafkaMetricsReporter
                                    with KafkaGraphiteMetricsReporterMBean
                                    with Logging {

  private var underlying: GraphiteReporter = null
  private var running = false
  private var initialized = false

  override def getMBeanName: String = "kafka:type=kafka.metrics.KafkaGraphiteMetricsReporter"

  override def init(props: VerifiableProperties) {
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaGraphiteMetricsConfig(props)
        val socketProvider = new DefaultSocketProvider(metricsConfig.host, metricsConfig.port)

        val metricPredicate = new MetricPredicate {
          val include = Option(metricsConfig.include)
          val exclude = Option(metricsConfig.exclude)

          override def matches(name: MetricName, metric: Metric): Boolean = {
            if (include.isDefined && !include.get.matcher(groupMetricName(name)).matches()) {
              return false
            }
            if (exclude.isDefined && exclude.get.matcher(groupMetricName(name)).matches()) {
              return false
            }
            true
          }

          private def groupMetricName(name: MetricName): String = {
            val result = new StringBuilder().append(name.getGroup).append('.').append(name.getType).append('.')
            if (name.hasScope) {
              val scope = name.getScope.replaceAll("clientId\\.", "")

              if (scope.matches(PartitionPattern.expression)) {
                result.append(name.getName + ".")

                val patternMatcher = PartitionPattern.pattern.matcher(scope)
                patternMatcher.find()

                val topic: String = patternMatcher.group(3)
                val partition: String = patternMatcher.group(2)
                val prefix: String = patternMatcher.group(1)

                if (prefix != null && !prefix.equals("")) {
                  result.append(prefix + ".")
                }
                result.append(topic + "." + partition)
              } else {
                result.append(scope)
              }
            } else {
              result.append(name.getName)
            }

            result.toString().replace(' ', '_')
          }
        }

        info("Configuring Kafka Graphite Reporter with host=%s, port=%d, prefix=%s and include=%s, exclude=%s, jvm=%s".format(
          metricsConfig.host, metricsConfig.port, metricsConfig.prefix, metricsConfig.include, metricsConfig.exclude, metricsConfig.jvm))
        underlying = new RTBGraphiteReporter(Metrics.defaultRegistry, metricsConfig.prefix, metricPredicate,
                                          socketProvider, Clock.defaultClock)
        // Controls JVM metrics output
        underlying.printVMMetrics = metricsConfig.jvm
        if (metricsConfig.enabled) {
          initialized = true
          startReporter(metricsConfig.pollingIntervalSecs)
        }
      }
    }
  }

  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        underlying.start(pollingPeriodSecs, TimeUnit.SECONDS)
        running = true
        info("Started Kafka Graphite metrics reporter with polling period %d seconds".format(pollingPeriodSecs))
      }
    }
  }

  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        underlying.shutdown()
        running = false
        info("Stopped Kafka Graphite metrics reporter")
        underlying = null
      }
    }
  }
}


class RTBGraphiteReporter(metricsRegistry: MetricsRegistry, prefix: String, predicate: MetricPredicate,
                          socketProvider: SocketProvider, clock: Clock) extends GraphiteReporter(metricsRegistry, prefix, predicate, socketProvider, clock) {


  override def sanitizeName(name: MetricName): String = {
    val result = new StringBuilder().append(name.getGroup).append('.').append(name.getType).append('.')
    if (name.hasScope) {
      result.append(name.getName + ".")
      val scope = name.getScope.replaceAll("clientId\\.", "")
      if (scope.matches(PartitionPattern.expression)) {

        val patternMatcher = PartitionPattern.pattern.matcher(scope)
        patternMatcher.find()

        val topic: String = patternMatcher.group(3)
        val partition: String = patternMatcher.group(2)
        val prefix: String = patternMatcher.group(1)

        if (prefix != null && !prefix.equals("")) {
          result.append(prefix + ".")
        }
        result.append(topic + "." + partition)
      } else {
        result.append(scope)
      }
    } else {
      result.append(name.getName)
    }
    result.toString().replace(' ', '_')
  }
}
