package io.nekohasekai.sagernet.auth

import android.content.Context
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.ktx.Logs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * NekoBox999: license enforcement inside the :bg service process.
 *
 * The startup gate lives in the UI process, but the proxy runs in
 * :bg — without this watchdog a connection started before expiry
 * would simply keep running past it. While a proxy is up, the
 * watchdog wakes once a minute; once the stored expiry has passed
 * it validates on its own (at most once per ten minutes) and a
 * server denial cuts the connection through the normal graceful
 * stop, exactly like pressing the stop button. A server-side
 * renewal just lets it keep running, and a network error never
 * cuts — the check retries until a real answer lands.
 */
object NbExpiryWatchdog {

    private const val TICK_MS = 60_000L
    private const val VALIDATE_RETRY_MS = 10 * 60_000L

    private var job: Job? = null

    @Volatile
    private var lastValidateMs = 0L

    fun start(scope: CoroutineScope, context: Context) {
        if (job?.isActive == true) return
        val appCtx = context.applicationContext
        job = scope.launch {
            while (isActive) {
                runCatching { tick(appCtx) }
                delay(TICK_MS)
            }
        }
    }

    fun stop() {
        job?.cancel()
        job = null
    }

    private suspend fun tick(context: Context) {
        if (!NbAuthManager.hasToken(context)) return
        // Null expiry = permanent license; nothing ever cuts.
        val exp = NbAuthManager.expiresAt(context) ?: return
        if (System.currentTimeMillis() / 1000L <= exp) return
        // Wall clock is past expiry; confirm with the watermark
        // clock before spending a validation on it.
        if (!NbAuthManager.localExpiryPassed(context)) return
        val now = System.currentTimeMillis()
        if (now - lastValidateMs < VALIDATE_RETRY_MS) return
        lastValidateMs = now
        when (NbAuthManager.validateNow(context)) {
            is NbAuthManager.Outcome.Denied -> {
                Logs.w("NbExpiryWatchdog: license denied after expiry, stopping service")
                SagerNet.stopService()
                stop()
            }
            // Ok: the stored expiry moved forward; the next tick
            // sees a live license again. NetworkError: unproven —
            // keep the tunnel and retry on the slow cadence.
            else -> Unit
        }
    }
}
