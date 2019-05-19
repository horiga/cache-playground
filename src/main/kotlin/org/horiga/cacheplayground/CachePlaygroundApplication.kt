package org.horiga.cacheplayground

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.github.benmanes.caffeine.cache.RemovalListener
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty
import org.springframework.boot.runApplication
import org.springframework.cache.annotation.CacheConfig
import org.springframework.cache.annotation.Cacheable
import org.springframework.cache.annotation.EnableCaching
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
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
@EnableCaching
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
class PlaygroundRestController(
        properties: PlaygroundProperties,
        val service: PlaygroundCacheService,
        originalRepository: OriginalRepository) {

    companion object {
        val log = LoggerFactory.getLogger(PlaygroundRestController::class.java)!!
    }

    val cache: LoadingCache<String, String> = Caffeine.newBuilder()
            .maximumSize(properties.cache.maximumSize)
            .refreshAfterWrite(properties.cache.refreshAfterWriteDuration, TimeUnit.SECONDS)
            .expireAfterWrite(properties.cache.expireAfterWriteDuration, TimeUnit.SECONDS)
            .removalListener(RemovalListener { key, value, cause ->
                log.info("Handle onRemoval!! key=$key, value=$value, cause=$cause")
            })
            .recordStats()
            .build { originalRepository.readOriginal(it) }

    @GetMapping("sp-cache/{key}")
    fun getFromSpringCache(@PathVariable key: String) = service.getFromSpringCache(key)

    @GetMapping("cache/{key}")
    fun get(@PathVariable key: String) = cache.get(key) ?: "the '$key' does not exist in cache!!"

    @GetMapping("cache/refresh/{key}")
    fun refresh(@PathVariable key: String) = cache.refresh(key)

    @GetMapping("cache-all")
    fun asMap() = cache.asMap()

    @GetMapping("cache-invalidate")
    fun invalidate() = cache.invalidateAll()
}

@Service
@CacheConfig(cacheNames = ["playground"])
class PlaygroundCacheService(val originalRepository: OriginalRepository) {
    @Cacheable
    fun getFromSpringCache(key: String): String = originalRepository.readOriginal(key)
            ?: "the '$key' does not exist in cache!!"
}

@Repository
class OriginalRepository(properties: PlaygroundProperties) {
    companion object {
        val log = LoggerFactory.getLogger(OriginalRepository::class.java)!!
        val objectMapper = jacksonObjectMapper()
                .registerModule(JavaTimeModule())
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!
    }

    private val filePath = properties.originalFile

    private var previous: Original? = null

    @Throws(Exception::class)
    fun readOriginal(key: String): String? {
        try {
            log.info("[load cache from $filePath] key=$key")
            if (!Files.exists(FileSystems.getDefault().getPath(filePath))) {
                throw RuntimeException("original file is not exists")
            }
            val original: Original = objectMapper.readValue(FileInputStream(File(filePath)))
            return when {
                !original.success -> throw RuntimeException("system error!!")
                !original.kv.containsKey(key) -> {
                    null
                }
                original.kv[key] == "exception" -> throw RuntimeException("")
                else -> {
                    previous = original
                    original.kv[key]
                }
            }
        } catch (e: Exception) {
            log.warn("!!!! Handle unknown error, return previous values. !!!!", e)
            // 何かしらの原因で失敗した場合に、前回成功した結果があればそれを返す
            return previous?.kv?.get(key)
        }
    }
}