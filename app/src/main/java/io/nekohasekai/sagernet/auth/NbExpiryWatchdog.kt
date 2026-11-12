package io.nekohasekai.sagernet.auth

import android.content.Context
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.ktx.Logs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
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
 *
 * The validation runs on a fixed cadence while connected, not
 * only after the locally stored expiry passes: the server gates
 * on its own record, so a license shortened or revoked
 * server-side is discovered within one cadence even when the
 * stored expiry still reads far in the future. Every decision
 * is written to the app log so a missed cut is diagnosable.
 */
object NbExpiryWatchdog {

    private const val TICK_MS = 30_000L
    private const val PERIODIC_VALIDATE_MS = 5 * 60_000L
    private const val EXPIRED_RETRY_MS = 60_000L

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private var job: Job? = null

    @Volatile
    private var lastValidateMs = 0L

    fun start(context: Context) {
        if (job?.isActive == true) return
        val appCtx = context.applicationContext
        Logs.w("NbExpiryWatchdog: armed")
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
        val now = System.currentTimeMillis()
        // Fast path once the stored expiry has passed (wall clock
        // first, watermark clock confirms); otherwise the slow
        // periodic cadence is what discovers server-side changes.
        val expiredLocally = NbAuthManager.expiresAt(context)
            ?.let { now / 1000L > it && NbAuthManager.localExpiryPassed(context) }
            ?: false
        val interval = if (expiredLocally) EXPIRED_RETRY_MS else PERIODIC_VALIDATE_MS
        if (now - lastValidateMs < interval) return
        lastValidateMs = now
        when (val r = NbAuthManager.validateNow(context)) {
            is NbAuthManager.Outcome.Denied -> {
                Logs.w("NbExpiryWatchdog: server denied (${r.reason}); stopping service")
                SagerNet.stopService()
                stop()
            }
            is NbAuthManager.Outcome.Ok -> {
                // validateNow stored the server's current expiry;
                // if it is already past, the license is dead even
                // though the server answered ok.
                if (NbAuthManager.localExpiryPassed(context)) {
                    Logs.w("NbExpiryWatchdog: validated but expiry already passed; stopping service")
                    SagerNet.stopService()
                    stop()
                }
            }
            NbAuthManager.Outcome.NetworkError -> {
                if (expiredLocally) {
                    Logs.w("NbExpiryWatchdog: expired locally, server unreachable; keeping tunnel until a real answer lands")
                }
            }
        }
    }
}
