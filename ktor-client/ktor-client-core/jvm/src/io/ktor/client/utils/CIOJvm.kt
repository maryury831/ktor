/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.client.utils

import io.ktor.client.*
import io.ktor.util.*
import kotlinx.coroutines.*
import io.ktor.utils.io.pool.*
import java.nio.*
import kotlin.coroutines.*

/**
 * Singleton pool of [ByteBuffer] objects used for [HttpClient].
 */
@InternalAPI
val HttpClientDefaultPool = ByteBufferPool()

@InternalAPI
class ByteBufferPool : DefaultPool<ByteBuffer>(DEFAULT_HTTP_POOL_SIZE) {
    override fun produceInstance(): ByteBuffer = ByteBuffer.allocate(DEFAULT_HTTP_BUFFER_SIZE)!!

    override fun clearInstance(instance: ByteBuffer): ByteBuffer = instance.apply { clear() }
}

/**
 * Run request blocking in [HttpClient] dispatcher.
 */
@KtorExperimentalAPI
actual fun <T> HttpClient.runBlocking(block: suspend CoroutineScope.() -> T): T =
    runBlocking(EmptyCoroutineContext, block)
