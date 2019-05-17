package org.horiga.cacheplayground

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CachePlaygroundApplication

fun main(args: Array<String>) {
	runApplication<CachePlaygroundApplication>(*args)
}
