package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.bg.proto.GeoTest
import io.nekohasekai.sagernet.bg.proto.UrlTest
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.ProfileManager
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.ktx.app
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Per-country load balancing for NekoBox999: one country section can
 * be marked "auto". A round URL-tests only that country's nodes,
 * selects the reachable one with the lowest latency, and (when the
 * service is running) switches the connection to it. Rounds repeat
 * every 5 minutes for that country only. The loop lives in the app
 * process; it resumes on the next app start while a country is active.
 */
object NbLoadBalancer {

    private const val TAG = "NbLB"
    private const val PREFS = "nb999_lb"
    private const val KEY_COUNTRY = "lb_country"
    private const val INTERVAL_MS = 5 * 60_000L

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val roundRunning = AtomicBoolean(false)

    @Volatile
    private var roundDone: kotlinx.coroutines.CompletableDeferred<Unit>? = null

    @Volatile
    private var loopJob: Job? = null

    fun activeCountry(context: Context): String? =
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .getString(KEY_COUNTRY, null)?.takeIf { it.isNotBlank() }

    fun setActive(context: Context, countryCode: String?) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().apply {
            if (countryCode.isNullOrBlank()) remove(KEY_COUNTRY)
            else putString(KEY_COUNTRY, countryCode.uppercase())
        }.apply()
        if (countryCode.isNullOrBlank()) {
            loopJob?.cancel()
            loopJob = null
        } else {
            ensureLoop(context)
            scope.launch { runRound() }
        }
    }

    /** Start the 5-minute loop if a country is active and no loop runs. */
    fun ensureLoop(context: Context) {
        if (activeCountry(context) == null) return
        if (loopJob?.isActive == true) return
        loopJob = scope.launch {
            while (isActive) {
                delay(INTERVAL_MS)
                runRound()
            }
        }
    }

    private suspend fun managedGroupId(): Long? = withContext(Dispatchers.IO) {
        SagerDatabase.groupDao.allGroups().firstOrNull { group ->
            group.subscription?.link?.let { NbAuthManager.isManagedLink(it) } == true
        }?.id
    }

    suspend fun runRound() {
        if (!roundRunning.compareAndSet(false, true)) return
        val doneSignal = kotlinx.coroutines.CompletableDeferred<Unit>()
        roundDone = doneSignal
        try {
            val cc = activeCountry(app) ?: return
            val groupId = managedGroupId() ?: return
            val all = withContext(Dispatchers.IO) {
                SagerDatabase.proxyDao.getByGroup(groupId)
            }
            if (all.isEmpty()) return

            // Phase 1: re-check the regions of the WHOLE group, not
            // just the active country's section. Exits move: a node
            // filed under one country may sit somewhere else by now,
            // and one filed elsewhere may have moved in. Every node
            // whose exit country is known lands in its real section.
            val sweepQueue = ConcurrentLinkedQueue(all)
            val sweepers = mutableListOf<Job>()
            repeat(32) {
                sweepers.add(scope.launch {
                    while (true) {
                        val profile = sweepQueue.poll() ?: break
                        val stored = NbGeoStore.countryFor(app, profile.requireBean())
                        val actual = runCatching {
                            GeoTest().doFetchCountry(profile)
                        }.getOrNull()?.uppercase()?.takeIf { it.length == 2 }
                        if (actual != null && actual != stored) {
                            NbGeoStore.record(app, profile.requireBean(), actual)
                        }
                    }
                })
            }
            sweepers.forEach { it.join() }

            // Phase 2: speed-test the active country's members (the
            // membership just corrected by the sweep) and race them.
            val members = all.filter {
                NbGeoStore.countryFor(app, it.requireBean()) == cc
            }
            if (members.isEmpty()) {
                Log.i(TAG, "round for $cc: no members after region sweep")
                return
            }
            Log.i(TAG, "round for $cc over ${members.size} profiles")

            val queue = ConcurrentLinkedQueue(members)
            val eligible = ConcurrentLinkedQueue<ProxyEntity>()
            val workers = mutableListOf<Job>()
            val concurrent = DataStore.connectionTestConcurrent.coerceIn(1, 16)
            repeat(concurrent) {
                workers.add(scope.launch {
                    while (true) {
                        val profile = queue.poll() ?: break
                        if (testPing(profile)) {
                            eligible.add(profile)
                        }
                    }
                })
            }
            workers.forEach { it.join() }

            val best = eligible.minByOrNull { it.ping }
            if (best == null) {
                Log.i(TAG, "round for $cc: nothing reachable")
                return
            }
            if (DataStore.selectedProxy != best.id) {
                val previous = DataStore.selectedProxy
                DataStore.selectedProxy = best.id
                ProfileManager.postUpdate(previous)
                ProfileManager.postUpdate(best.id)
                Log.i(TAG, "auto-connect -> ${best.requireBean().displayName()} (${best.ping}ms)")
                if (DataStore.serviceState.canStop) {
                    SagerNet.reloadService()
                }
            } else {
                Log.i(TAG, "round for $cc: staying on ${best.requireBean().displayName()} (${best.ping}ms)")
            }
        } catch (e: Exception) {
            Log.w(TAG, "round failed", e)
        } finally {
            roundRunning.set(false)
            doneSignal.complete(Unit)
        }
    }

    /**
     * Connect-time pick: do NOT wait for a fresh round. The round
     * that started when the user switched the country to auto has
     * already measured everyone, so connect immediately to the
     * best known node of the active country, then let a background
     * round refresh regions and speeds (it switches over if it
     * finds something better).
     */
    suspend fun pickForConnect() {
        val cc = activeCountry(app) ?: return
        val groupId = managedGroupId() ?: return
        val profiles = withContext(Dispatchers.IO) {
            SagerDatabase.proxyDao.getByGroup(groupId)
        }
        val inCountry = profiles.filter {
            NbGeoStore.countryFor(app, it.requireBean()) == cc
        }
        val current = inCountry.firstOrNull { it.id == DataStore.selectedProxy }
        val best = when {
            current != null -> current
            else -> inCountry.filter { it.status == 1 && it.ping > 0 }
                .minByOrNull { it.ping }
                ?: inCountry.filter { it.ping > 0 }.minByOrNull { it.ping }
                ?: inCountry.firstOrNull()
        }
        if (best != null && DataStore.selectedProxy != best.id) {
            val previous = DataStore.selectedProxy
            DataStore.selectedProxy = best.id
            ProfileManager.postUpdate(previous)
            ProfileManager.postUpdate(best.id)
            Log.i(TAG, "connect pick -> ${best.requireBean().displayName()} (${best.ping}ms)")
        }
        scope.launch { runRound() }
    }

    /** Speed-test one candidate of the active country. */
    private suspend fun testPing(profile: ProxyEntity): Boolean {
        return try {
            val result = UrlTest().doTest(profile)
            profile.status = 1
            profile.ping = result
            ProfileManager.updateProfile(profile)
            true
        } catch (e: Exception) {
            profile.status = 3
            profile.error = e.message
            ProfileManager.updateProfile(profile)
            false
        }
    }

}
