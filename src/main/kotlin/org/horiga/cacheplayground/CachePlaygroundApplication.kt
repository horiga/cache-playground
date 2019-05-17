package org.horiga.cacheplayground

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import org.springframework.boot.runApplication
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.io.File
import java.io.FileInputStream
import java.nio.file.FileSystems
import java.nio.file.Files
import java.util.concurrent.TimeUnit

@SpringBootApplication
@EnableConfigurationProperties(PlaygroundProperties::class)
class CachePlaygroundApplication

fun main(args: Array<String>) {
    runApplication<CachePlaygroundApplication>(*args)
}

@ConfigurationProperties(prefix = "playground")
data class PlaygroundProperties(
    var originalFile: String = "/tmp/ORIGINAL.txt",
    @NestedConfigurationProperty
    var cache: PlaygroundCacheProperties = PlaygroundCacheProperties()
) {
    data class PlaygroundCacheProperties(
        var maximumSize: Long = 10,
        var refreshAfterWriteDuration: Long = 180,
        var expireAfterWriteDuration: Long = 300
    )
}

data class Original(
    val success: Boolean = true,
    val kv: Map<String, String> = emptyMap()
)

@RestController
class PlaygroundRestController(properties: PlaygroundProperties) {

    companion object {
        val objectMapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .configure(SerializationFeature.INDENT_OUTPUT, true)
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!
        val log = LoggerFactory.getLogger(PlaygroundRestController::class.java)!!
    }

    val filePath = properties.originalFile

    val cache: LoadingCache<String, String> = Caffeine.newBuilder()
        .maximumSize(properties.cache.maximumSize)
        .refreshAfterWrite(properties.cache.refreshAfterWriteDuration, TimeUnit.SECONDS)
        .expireAfterWrite(properties.cache.expireAfterWriteDuration, TimeUnit.SECONDS)
        .removalListener(CacheRemovalListener())
        .recordStats()
        .build { readOriginal(it) }

    @GetMapping("cache/{key}")
    fun get(@PathVariable key: String) = cache.get(key) ?: "the '$key' does not exist in cache!!"

    @GetMapping("cache/refresh/{key}")
    fun refresh(@PathVariable key: String) = cache.refresh(key)

    @GetMapping("cache-stats")
    fun stats() = cache.stats()

    @GetMapping("cache-all")
    fun asMap() = cache.asMap()

    @GetMapping("cache-invalidate")
    fun invalidate() = cache.invalidateAll()

    @Throws(Exception::class)
    private fun readOriginal(key: String): String? {
        log.info("[load cache from $filePath] key=$key")
        if (!Files.exists(FileSystems.getDefault().getPath(filePath)))
            throw RuntimeException("original file is not exists")
        val original: Original = objectMapper.readValue(FileInputStream(File(filePath)))
        return when {
            !original.success -> throw RuntimeException("system error!!")
            !original.kv.containsKey(key) -> {
                null
            }
            original.kv[key] == "exception" -> throw RuntimeException("")
            else -> original.kv[key]
            }
    }

    class CacheRemovalListener : RemovalListener<String, String> {
        companion object {
            val log = LoggerFactory.getLogger(CacheRemovalListener::class.java)!!
        }

        override fun onRemoval(key: String?, value: String?, cause: RemovalCause) {
            log.info("Handle onRemoval!! key=$key, value=$value, cause=$cause")
        }
    }
}