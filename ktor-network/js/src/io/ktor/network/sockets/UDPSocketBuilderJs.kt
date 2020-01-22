/*
 * Copyright 2014-2020 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.network.sockets

import io.ktor.network.selector.*
import io.ktor.network.util.*

internal actual fun UDPSocketBuilder.Companion.connectUDP(
    selector: SelectorManager,
    remoteAddress: NetworkAddress,
    localAddress: NetworkAddress?,
    options: SocketOptions.UDPSocketOptions
): ConnectedDatagramSocket {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}

internal actual fun UDPSocketBuilder.Companion.bindUDP(
    selector: SelectorManager,
    localAddress: NetworkAddress?,
    options: SocketOptions.UDPSocketOptions
): BoundDatagramSocket {
    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
}