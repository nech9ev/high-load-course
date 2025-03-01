package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Service
class OrderPayer {

    val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

// PaymentAccountProperties(serviceName=onlineStore, accountName=acc-5, parallelRequests=5, rateLimitPerSec=3, price=30, averageProcessingTime=PT4.9S, enabled=false)
// {
//  "ratePerSecond": 2,
//  "testCount": 100,
//  "processingTimeMillis": 60000
//}

    /**
     * 1) 2 -> payment (2)
     * 2) 2 -> payment (4)
     * 3) 2 -> payment (5) -> hold (1)
     * 4) 2 -> payment (5) -> hold (3)
     * 5) 2 -> payment (5) -> hold (5)
     * 6) 2 -> payment (5) +2 -> hold (5) -2
     * 7) 2 -> payment (5) +2 -> hold (5) -2
     * 8) 2 -> payment (5) +2 -> hold (5) -2
     * 9) 2 -> payment (5) +2 -> hold (3) -2
     * 10) 2 -> payment (5) +2 -> hold (3) -2
     *
     *
     * 1) window control - максимум 5 запросов в in flight
     * 2) новые запросы сначала в hold
     * 3) самые старые из hold в payment
     */

    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val semaphore = Semaphore(permits = 5)

    private val counter = MutableStateFlow(0)

    private val slideWindow = SlidingWindowRateLimiter<Order>(
        rate = SERVICE_RATE_LIMIT_PER_SECOND,
        window = Duration.ofSeconds(1),
    )

    private val checkDelayTime =
        1.seconds.inWholeMilliseconds.div(SERVICE_RATE_LIMIT_PER_SECOND).milliseconds.div(SPEED_CHECK_COFF)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val timeout = createdAt.plus(55100)
        val order = Order(createdAt, paymentId)
        val orderIndex = counter.getAndUpdate { oldCounter -> oldCounter.plus(1) }
        coroutineScope.launch {
            semaphore.withPermit {
                slideWindow.tickValueBlocking(order, checkDelayTime)
                val permittedAt = System.currentTimeMillis()
                when {
                    permittedAt < timeout -> {
                        logger.error("ORDER index: $orderIndex")
                        val createdEvent = paymentESService.create {
                            it.create(
                                paymentId, orderId, amount
                            )
                        }
                        logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                    }

                    else -> {
                        logger.error("TIMEEOUT ORDER index: $orderIndex")
                    }
                }
            }
        }
        return createdAt
    }

    data class Order(
        val createdAt: Long,
        val paymentId: UUID,
    )

    private companion object {

        const val SERVICE_RATE_LIMIT_PER_SECOND = 3L

        const val SPEED_CHECK_COFF = 1
    }
}
