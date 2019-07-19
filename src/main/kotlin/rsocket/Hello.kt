package rsocket

import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.subscribers.DefaultSubscriber
import io.rsocket.kotlin.DefaultPayload
import io.rsocket.kotlin.Payload
import io.rsocket.kotlin.RSocketFactory
import io.rsocket.kotlin.transport.netty.client.TcpClientTransport
import io.rsocket.kotlin.transport.netty.client.WebsocketClientTransport
import io.rsocket.kotlin.transport.netty.server.TcpServerTransport
import io.rsocket.kotlin.transport.netty.server.WebsocketServerTransport
import io.rsocket.kotlin.util.AbstractRSocket
import reactor.core.publisher.toFlux
import reactor.util.Loggers
import java.math.BigDecimal
import java.util.concurrent.CountDownLatch

fun main() {
    Loggers.useJdkLoggers()
    RSocketFactory.receive()
        .acceptor {
            { _, _ ->
                Single.just(object : AbstractRSocket() {
                    override fun requestResponse(payload: Payload): Single<Payload> =
                        Single.just(DefaultPayload.text("Test Response"))

                    override fun requestStream(payload: Payload): Flowable<Payload> {
                        return Flowable.fromPublisher(sequence {
                            var previous = BigDecimal(0)
                            var current = BigDecimal(1)
                            yield(current)
                            while (true) {
                                val next = previous + current
                                yield(next)
                                previous = current
                                current = next
                            }
                        }.map { DefaultPayload.text(it.toString()) }.toFlux())
                    }
                })
            }
        }
        .transport(WebsocketServerTransport.create("localhost", 8080))
        .start()
        .flatMapCompletable { it.onClose() }
        .subscribe()

    val clientSocket = RSocketFactory.connect()
        .transport(WebsocketClientTransport.create("localhost", 8080))
        .start()
        .blockingGet()

    val latch = CountDownLatch(1)

    clientSocket.requestStream(DefaultPayload.EMPTY)
        .map { it.dataUtf8 }
        .map { it.toBigDecimal() }
        .subscribe(object : DefaultSubscriber<BigDecimal>() {
            var state: Long = 0
            var expect: Long = 10
            override fun onStart() {
                request(expect)
            }

            override fun onNext(t: BigDecimal?) {

                if (t != null) {
                    if (t < BigDecimal.ZERO) {
                        cancel()
                        latch.countDown()
                        return
                    }
                }

                println("We Got Element : $t")
                state++

                if (state == expect) {
                    state = 0
                    expect *= 2
                    request(expect)
                }
            }

            override fun onError(t: Throwable?) {
                latch.countDown()
                println("We Got Error $t")
            }

            override fun onComplete() {
                latch.countDown()
                println("We Got Completion")
            }
        })

    latch.await()
}