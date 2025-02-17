package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter<T>(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(10_000)
    private val mutex = ReentrantLock()

    private val orderWaitingQueue = ConcurrentLinkedDeque<T>()

    override fun tick(): Boolean {
        if (sum.get() > rate) {
            return false
        } else {
            mutex.withLock {
                val now = System.currentTimeMillis()
                if (sum.get() <= rate) {
                    queue.add(Measure(1, now))
                    sum.incrementAndGet()
                    return true
                } else return false
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(10)
        }
    }

    suspend fun tickValueBlocking(order: T, delayTime: kotlin.time.Duration) {
        while (!tickValue(order)) {
            delay(delayTime)
        }
    }

    private fun tickValue(order: T): Boolean = when {
        !orderWaitingQueue.contains(order) -> {
            orderWaitingQueue.add(order)
            false
        }

        else -> when {
            sum.get() < rate && orderWaitingQueue.peek() == order -> {
                mutex.withLock {
                    val now = System.currentTimeMillis()
                    if (sum.get() <= rate) {
                        queue.add(Measure(1, now))
                        sum.incrementAndGet()
                        orderWaitingQueue.removeFirst()
                        true
                    } else false
                }
            }

            else -> false
        }
    }

    data class Measure(
        val value: Long, val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val head = queue.peek()
            val winStart = System.currentTimeMillis() - window.toMillis()
            if (head == null || head.timestamp > winStart) {
                continue
            }
            sum.addAndGet(-1)
            queue.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
