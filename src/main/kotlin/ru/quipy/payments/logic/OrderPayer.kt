package ru.quipy.payments.logic

import kotlinx.coroutines.*
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
//  "testCount": 500,
//  "processingTimeMillis": 60000
//}

    private val semaphore = Semaphore(permits = 5)

    private val slideWindow = SlidingWindowRateLimiter<Order>(
        rate = SERVICE_RATE_LIMIT_PER_SECOND,
        window = Duration.ofSeconds(1),
    )

    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        scope.launch {
            val result = withTimeoutOrNull(deadline - createdAt) {
                semaphore.withPermit {
                    slideWindow.tickSuspendBlocking()
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId, orderId, amount
                        )
                    }
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                    logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
                }
            }
            if (result == null) {
                semaphore.release()
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
    }
}
