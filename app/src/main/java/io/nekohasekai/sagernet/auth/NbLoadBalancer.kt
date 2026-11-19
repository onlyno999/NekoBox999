package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.aidl.ISagerNetService
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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Per-country load balancing for NekoBox999: one country section can
 * be marked "auto". Since 1.3.15 the upkeep follows the standby
 * model: rounds every 10 minutes test a rotating batch of FIVE of
 * the active country's members and only refresh their records (a
 * healthy line is never displaced for speed), and death is judged
 * by the core itself — the sing-box fork counts consecutive failed
 * dials of real traffic on the main line, the watchdog reads that
 * number every 10 seconds through the service, and at 3 in a row
 * the line is swapped at once to the best recorded standby.
 * Whole-group region sweeps run every 30 minutes. The loop lives
 * in the app process; it resumes on the next app start while a
 * country is active.
 */
object NbLoadBalancer {

    private const val TAG = "NbLB"
    private const val PREFS = "nb999_lb"
    private const val KEY_COUNTRY = "lb_country"
    private const val SPEED_INTERVAL_MS = 10 * 60_000L
    private const val REGION_INTERVAL_MS = 30 * 60_000L
    private const val WATCHDOG_MS = 10_000L
    private const val BATCH_SIZE = 5
    private const val DEATH_THRESHOLD = 3L

    /** Tag of the general line in the generated config
     *  (ConfigBuilder.TAG_PROXY). */
    private const val MAIN_LINE_TAG = "proxy"

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val roundRunning = AtomicBoolean(false)
    private val sweepRunning = AtomicBoolean(false)

    @Volatile
    private var roundDone: kotlinx.coroutines.CompletableDeferred<Unit>? = null

    @Volatile
    private var speedJob: Job? = null

    @Volatile
    private var regionJob: Job? = null

    @Volatile
    private var watchdogJob: Job? = null

    /**
     * How the watchdog reaches the running service (its core
     * holds the dial-health counters). Set by MainActivity; null
     * whenever no activity is bound, in which case the watchdog
     * simply skips — reading a number is all it ever does.
     */
    @Volatile
    var serviceProvider: (() -> ISagerNetService?)? = null

    /** Selection the core's dial-health counter currently
     *  belongs to; a change restarts the count. */
    @Volatile
    private var healthForSelected: Long = -1L
    private var stallSampleFor: Long = -1L
    private var lastTxTotal: Long = 0L
    private var lastRxTotal: Long = 0L

    fun activeCountry(context: Context): String? =
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .getString(KEY_COUNTRY, null)?.takeIf { it.isNotBlank() }

    fun setActive(context: Context, countryCode: String?) {
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().apply {
            if (countryCode.isNullOrBlank()) remove(KEY_COUNTRY)
            else putString(KEY_COUNTRY, countryCode.uppercase())
        }.apply()
        if (countryCode.isNullOrBlank()) {
            stopLoops()
        } else {
            ensureLoop(context)
            scope.launch {
                runRegionSweep()
                pickForConnect()
            }
        }
    }

    private fun stopLoops() {
        speedJob?.cancel(); speedJob = null
        regionJob?.cancel(); regionJob = null
        watchdogJob?.cancel(); watchdogJob = null
        healthForSelected = -1L
    }

    // ---- network-change region refresh ----

    private const val NET_SWEEP_COOLDOWN_MS = 3 * 60_000L
    private const val NET_SETTLE_MS = 3_000L

    private val netWatchRegistered = AtomicBoolean(false)

    @Volatile
    private var lastNetwork: android.net.Network? = null

    @Volatile
    private var lastNetSweepAt = 0L

    @Volatile
    private var lastLinkAddrs: String? = null

    /**
     * Whenever the network changes — a different network
     * entirely (Wi-Fi to mobile data, Telecom to Unicom,
     * a new Wi-Fi) or the same network handing out new
     * addresses (4G to 5G style handovers) — run one
     * whole-group region sweep in the background: the exit
     * addresses move with the network, so everything is
     * re-filed against the new one. Regions only, never a
     * speed round — the connected line already swaps
     * itself after three dead dials. Registered once per
     * process; the state at registration is the baseline,
     * so only genuine changes fire. A short cooldown keeps
     * flapping networks from sweeping back to back.
     */
    fun watchNetwork(context: Context) {
        if (!netWatchRegistered.compareAndSet(false, true)) return
        val cm = context.applicationContext
            .getSystemService(android.net.ConnectivityManager::class.java) ?: return
        lastNetwork = runCatching { cm.activeNetwork }.getOrNull()
        runCatching {
            cm.registerDefaultNetworkCallback(
                object : android.net.ConnectivityManager.NetworkCallback() {
                    override fun onAvailable(network: android.net.Network) {
                        if (network == lastNetwork) return
                        lastNetwork = network
                        fireNetSweep()
                    }

                    override fun onLinkPropertiesChanged(
                        network: android.net.Network,
                        lp: android.net.LinkProperties
                    ) {
                        val addrs = lp.linkAddresses
                            .mapNotNull { it.address.hostAddress }
                            .sorted()
                            .joinToString(",")
                        val prev = lastLinkAddrs
                        lastLinkAddrs = addrs
                        if (prev != null && prev != addrs) fireNetSweep()
                    }

                    override fun onLost(network: android.net.Network) {
                        if (network == lastNetwork) lastNetwork = null
                    }
                }
            )
        }.onFailure { Log.w(TAG, "network watch failed", it) }
    }

    private fun fireNetSweep() {
        val now = System.currentTimeMillis()
        if (now - lastNetSweepAt < NET_SWEEP_COOLDOWN_MS) return
        lastNetSweepAt = now
        scope.launch {
            delay(NET_SETTLE_MS)
            runRegionSweep()
        }
    }

    /** Start the periodic loops if a country is active. */
    fun ensureLoop(context: Context) {
        watchNetwork(context)
        if (activeCountry(context) == null) return
        if (speedJob?.isActive != true) {
            speedJob = scope.launch {
                while (isActive) {
                    delay(SPEED_INTERVAL_MS)
                    runSpeedRound()
                }
            }
        }
        if (regionJob?.isActive != true) {
            regionJob = scope.launch {
                while (isActive) {
                    delay(REGION_INTERVAL_MS)
                    runRegionSweep()
                }
            }
        }
        if (watchdogJob?.isActive != true) {
            watchdogJob = scope.launch {
                while (isActive) {
                    delay(WATCHDOG_MS)
                    watchdogTick()
                }
            }
        }
    }

    /**
     * Watchdog, standby model: no probing at all. The core counts
     * consecutive failed dials of REAL traffic on the main line
     * (a handshake that never came back); this tick only reads
     * that number through the service, every 10 seconds. At
     * [DEATH_THRESHOLD] in a row the node in use is dead: swap at
     * once to the best recorded standby of the active country. A
     * change of selection restarts the count, so every member is
     * judged on its own dials only. No traffic means no signal
     * and no verdict — an idle line is left alone.
     */
    private suspend fun watchdogTick() {
        if (!DataStore.serviceState.connected) return
        if (activeCountry(app) == null) return
        val service = serviceProvider?.invoke() ?: return
        val groupId = managedGroupId() ?: return
        val selectedId = DataStore.selectedProxy
        if (healthForSelected != selectedId) {
            healthForSelected = selectedId
            runCatching { service.resetDialHealth(MAIN_LINE_TAG) }
            return
        }
        val failures = runCatching {
            service.queryDialFailures(MAIN_LINE_TAG)
        }.getOrDefault(0L)
        if (failures >= DEATH_THRESHOLD) {
            Log.i(TAG, "watchdog: line dead ($failures consecutive failed dials)")
            failOverFromRecord(groupId, selectedId)
            healthForSelected = -1L
            scope.launch { runSpeedRound() }
        }
        if (DataStore.selectedProxy == selectedId) {
            stallCheckTick(groupId, selectedId, service)
        }
    }

    /**
     * Stall check: a node can stay reachable - handshakes fine,
     * tunnel up - yet return no data at all, which dial health
     * cannot see. Every watchdog tick compares the main line's
     * session totals: data went out in the last window and not
     * one byte came back is suspicious, and one active probe
     * confirms it on the spot. Probe timeout: the node is
     * stalled, fail over exactly like a dead node. Probe alive:
     * a slow server, not the node's fault, leave the line alone.
     */
    private suspend fun stallCheckTick(
        groupId: Long, selectedId: Long, service: ISagerNetService,
    ) {
        val speed = runCatching { service.querySpeed() }.getOrNull() ?: return
        if (stallSampleFor != selectedId) {
            stallSampleFor = selectedId
            lastTxTotal = speed.txTotal
            lastRxTotal = speed.rxTotal
            return
        }
        val sentDelta = speed.txTotal - lastTxTotal
        val recvDelta = speed.rxTotal - lastRxTotal
        lastTxTotal = speed.txTotal
        lastRxTotal = speed.rxTotal
        if (sentDelta <= 0L || recvDelta != 0L) return
        // Never fight a standby round for the test machinery;
        // the next tick re-evaluates with fresh totals.
        if (roundRunning.get()) return
        val profile = withContext(Dispatchers.IO) {
            SagerDatabase.proxyDao.getById(selectedId)
        } ?: return
        val ok = runCatching { testPing(profile) }.getOrDefault(false)
        if (ok) {
            Log.i(TAG, "stall check: sent $sentDelta B, 0 B back, probe live; not the node")
            return
        }
        Log.i(TAG, "stall check: sent $sentDelta B, 0 B back, probe timed out; failing over")
        failOverFromRecord(groupId, selectedId)
        healthForSelected = -1L
        stallSampleFor = -1L
        scope.launch { runSpeedRound() }
    }

    /**
     * Instant failover from the recorded test history: pick the
     * fastest other member of the active country whose last recorded
     * test passed, and switch to it right away. No testing happens
     * on this path, so the swap itself takes no time.
     */
    private suspend fun failOverFromRecord(groupId: Long, deadId: Long) {
        val cc = activeCountry(app) ?: return
        val candidates = withContext(Dispatchers.IO) {
            SagerDatabase.proxyDao.getByGroup(groupId)
        }.filter {
            it.id != deadId && it.status == 1 && it.ping > 0 &&
                NbGeoStore.countryFor(app, it.requireBean()) == cc
        }
        val best = candidates.minByOrNull { it.ping } ?: run {
            Log.i(TAG, "watchdog failover: no recorded alternative in $cc")
            return
        }
        val previous = DataStore.selectedProxy
        DataStore.selectedProxy = best.id
        ProfileManager.postUpdate(previous)
        ProfileManager.postUpdate(best.id)
        Log.i(TAG, "watchdog failover -> ${best.requireBean().displayName()} (${best.ping}ms)")
        if (DataStore.serviceState.canStop) {
            SagerNet.reloadService()
        }
    }

    private suspend fun managedGroupId(): Long? = withContext(Dispatchers.IO) {
        SagerDatabase.groupDao.allGroups().firstOrNull { group ->
            group.subscription?.link?.let { NbAuthManager.isManagedLink(it) } == true
        }?.id
    }

    /** A full pass: whole-group region sweep, then a standby batch. */
    suspend fun runRound() {
        runRegionSweep()
        runSpeedRound()
    }

    /**
     * Re-check the regions of the WHOLE group, not just the active
     * country's section. Exits move: a node filed under one country
     * may sit somewhere else by now, and one filed elsewhere may
     * have moved in. Every node whose exit country is known lands
     * in its real section.
     */
    suspend fun runRegionSweep() {
        if (!sweepRunning.compareAndSet(false, true)) return
        try {
            val groupId = managedGroupId() ?: return
            val all = withContext(Dispatchers.IO) {
                SagerDatabase.proxyDao.getByGroup(groupId)
            }
            if (all.isEmpty()) return
            // Same two passes as the probe sweep, 100 concurrent:
            // primary source for the whole group first, fallback
            // only for the nodes the primary could not place.
            // Only changes are recorded; a failed lookup never
            // erases a known country.
            val leftovers = ConcurrentLinkedQueue<ProxyEntity>()
            NbGeoProbe.runPhase(all) { profile ->
                val stored = NbGeoStore.countryFor(app, profile.requireBean())
                val actual = runCatching {
                    GeoTest().doFetchCountryPrimary(profile)
                }.getOrNull()?.uppercase()?.takeIf { it.length == 2 }
                if (actual != null) {
                    if (actual != stored) {
                        NbGeoStore.record(app, profile.requireBean(), actual)
                    }
                } else {
                    leftovers.add(profile)
                }
            }
            NbGeoProbe.runPhase(leftovers.toList()) { profile ->
                val stored = NbGeoStore.countryFor(app, profile.requireBean())
                val actual = runCatching {
                    GeoTest().doFetchCountryFallback(profile)
                }.getOrNull()?.uppercase()?.takeIf { it.length == 2 }
                if (actual != null && actual != stored) {
                    NbGeoStore.record(app, profile.requireBean(), actual)
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "region sweep failed", e)
        } finally {
            sweepRunning.set(false)
        }
    }

    /**
     * Standby round: test the next [BATCH_SIZE] members of the
     * active country, rotating through it round by round, and
     * refresh their records. The results only stock the standby
     * shelf — this round NEVER moves the main connection: a
     * healthy line keeps its node until the core's dial health
     * condemns it. A country of five or fewer is covered whole
     * in one round.
     */
    suspend fun runSpeedRound() {
        if (!roundRunning.compareAndSet(false, true)) return
        val doneSignal = kotlinx.coroutines.CompletableDeferred<Unit>()
        roundDone = doneSignal
        try {
            val cc = activeCountry(app) ?: return
            val groupId = managedGroupId() ?: return
            val all = withContext(Dispatchers.IO) {
                SagerDatabase.proxyDao.getByGroup(groupId)
            }
            val members = all.filter {
                NbGeoStore.countryFor(app, it.requireBean()) == cc
            }.sortedBy { it.id }
            if (members.isEmpty()) {
                Log.i(TAG, "standby round for $cc: no members")
                return
            }
            val prefs = app.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            val cursorKey = "rot|$groupId|$cc"
            val lastId = prefs.getLong(cursorKey, -1L)
            val startIdx = members.indexOfFirst { it.id > lastId }
                .let { if (it < 0) 0 else it }
            val batch = (0 until minOf(BATCH_SIZE, members.size))
                .map { members[(startIdx + it) % members.size] }
            Log.i(TAG, "standby batch for $cc: ${batch.size} of ${members.size} profiles")
            withContext(Dispatchers.IO) {
                batch.map { profile ->
                    async { runCatching { testPing(profile) } }
                }.awaitAll()
            }
            prefs.edit().putLong(cursorKey, batch.last().id).apply()
        } catch (e: Exception) {
            Log.w(TAG, "standby round failed", e)
        } finally {
            roundRunning.set(false)
            doneSignal.complete(Unit)
        }
    }

    /**
     * Connect-time pick: do NOT wait for a fresh round. Connect
     * immediately to the best known node of the active country
     * from the recorded results (keeping the current selection
     * when it already sits in the country), then let a background
     * standby round keep the records fresh.
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
        scope.launch { runSpeedRound() }
    }

    /** Speed-test one candidate of the active country. */
    internal suspend fun testPing(profile: ProxyEntity): Boolean {
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
