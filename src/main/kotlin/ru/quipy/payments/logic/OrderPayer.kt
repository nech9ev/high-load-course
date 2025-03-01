package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
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

//    {
//        "serviceName": "onlineStore",
//        "accountName": "acc-3",
//        "parallelRequests": 30,
//        "rateLimitPerSec": 10,
//        "price": 30,
//        "averageProcessingTime": "PT1S"
//    }

    private val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val slideWindow = SlidingWindowRateLimiter<Order>(
        rate = SERVICE_RATE_LIMIT_PER_SECOND,
        window = Duration.ofSeconds(1),
    )

    private val checkDelayTime = 1.seconds.inWholeMilliseconds
        .div(SERVICE_RATE_LIMIT_PER_SECOND).milliseconds.div(SPEED_CHECK_COFF)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        coroutineScope.launch {
            val order = Order(createdAt, paymentId)
            slideWindow.tickValueBlocking(order, checkDelayTime)
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId, orderId, amount
                )
            }
            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }

    data class Order(
        val createdAt: Long,
        val paymentId: UUID,
    )

    private companion object {

        const val SERVICE_RATE_LIMIT_PER_SECOND = 10L

        const val SPEED_CHECK_COFF = 1
    }
}
