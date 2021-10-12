package io.conduktor.ksm.source

import com.typesafe.config.Config
import io.conduktor.ksm.parser.AclParserRegistry
import io.conduktor.ksm.source
import org.slf4j.LoggerFactory
import skinny.http.{HTTP, HTTPException, Request, Response}

import java.io.{BufferedReader, StringReader}

class NacosSourceAcl(parserRegistry: AclParserRegistry)
    extends SourceAcl(parserRegistry) {

  private val log = LoggerFactory.getLogger(classOf[NacosSourceAcl])

  override val CONFIG_PREFIX: String = "nacos"
  final val HOSTNAME_CONFIG = "hostname"
  final val DATAID_CONFIG = "dataid"
  final val GROUP_CONFIG = "group"

  var hostname: String = _
  var dataId: String = _
  var group: String = _

  /**
    * internal config definition for the module
    */
  override def configure(config: Config): Unit = {
    hostname = config.getString(HOSTNAME_CONFIG)
    dataId = config.getString(DATAID_CONFIG)
    group = config.getString(GROUP_CONFIG)
  }

  override def refresh(): Option[ParsingContext] = {
    val url =
      s"http://$hostname/nacos/v1/cs/configs?dataId=$dataId&group=$group"
    val request: Request = new Request(url)
    // super important in order to properly fail in case a timeout happens for example
    request.enableThrowingIOException(true)

    val response: Response = HTTP.get(request)
    response.status match {
      case 200 =>
        // we receive a valid response
        val reader = new BufferedReader(new StringReader(response.textBody))
        Some(source.ParsingContext(parserRegistry.getParserByFilename(dataId), reader))
      case _ =>
        // uncaught error
        log.warn(response.asString)
        throw HTTPException(Some(response.asString), response)
    }
  }

  /**
    * Close all the necessary underlying objects or connections belonging to this instance
    */
  override def close(): Unit = {
    // HTTP
  }
}
