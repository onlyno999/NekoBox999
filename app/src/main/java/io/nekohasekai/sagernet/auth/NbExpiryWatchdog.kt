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
 * Rule (owner-set, 2026-11-12): the moment the locally stored
 * expiry passes, the connection stops — full stop. The decision
 * is purely local: no online validation is consulted first,
 * and having no network changes nothing. (The earlier
 * validate-then-decide design let a tunnel live on whenever
 * the background validation could not produce a verdict.)
 *
 * The watchdog runs only while a proxy is up. It also stops
 * the service when no license token exists at all: a running
 * proxy without a session is only reachable if the session
 * was wiped mid-connection (e.g. the server revoked the
 * device and the UI cleared it), never through normal use —
 * the startup gate keeps unlicensed installs from connecting.
 *
 * A permanent license (no stored expiry) never triggers.
 * The stored expiry itself is refreshed by the app's normal
 * entry validation whenever the UI is opened.
 */
object NbExpiryWatchdog {

    private const val TICK_MS = 30_000L

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private var job: Job? = null

    fun start(context: Context) {
        if (job?.isActive == true) return
        val appCtx = context.applicationContext
        Logs.w("NbExpiryWatchdog: armed")
        job = scope.launch {
            while (isActive) {
                runCatching { tick(appCtx) }
                if (job?.isActive != true) break
                delay(TICK_MS)
            }
        }
    }

    fun stop() {
        job?.cancel()
        job = null
    }

    private fun tick(context: Context) {
        if (!NbAuthManager.hasToken(context)) {
            Logs.w("NbExpiryWatchdog: no license session while connected; stopping service")
            SagerNet.stopService()
            stop()
            return
        }
        if (NbAuthManager.localExpiryPassed(context)) {
            Logs.w("NbExpiryWatchdog: local expiry passed; stopping service")
            SagerNet.stopService()
            stop()
        }
    }
}
