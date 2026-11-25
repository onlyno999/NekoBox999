package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import io.nekohasekai.sagernet.bg.proto.GeoTest
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.GroupManager
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.fmt.AbstractBean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Country store + prober for NekoBox999: after a subscription sync,
 * every profile is asked (through itself) for https://cp.cloudflare.com/cdn-cgi/trace,
 * the resulting country code is kept here keyed by the same bean key
 * the subscription sync uses to remember the selection, and the group
 * is re-sorted so profiles sit grouped by country.
 */
object NbGeoStore {

    private const val PREFS = "nb999_geo"
    private const val KEY_MAP = "geo_map"
    private const val KEY_TRY = "geo_try"
    private const val RETRY_MS = 24 * 60 * 60_000L

    @Volatile
    private var cache: MutableMap<String, String>? = null

    @Volatile
    private var tryCache: MutableMap<String, Long>? = null

    private fun prefs(context: Context) =
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    private fun loadMap(context: Context): MutableMap<String, String> {
        cache?.let { return it }
        val out = mutableMapOf<String, String>()
        val raw = prefs(context).getString(KEY_MAP, null)
        if (raw != null) {
            runCatching {
                val json = JSONObject(raw)
                val keys = json.keys()
                while (keys.hasNext()) {
                    val k = keys.next()
                    out[k] = json.getString(k)
                }
            }
        }
        cache = out
        return out
    }

    private fun loadTry(context: Context): MutableMap<String, Long> {
        tryCache?.let { return it }
        val out = mutableMapOf<String, Long>()
        val raw = prefs(context).getString(KEY_TRY, null)
        if (raw != null) {
            runCatching {
                val json = JSONObject(raw)
                val keys = json.keys()
                while (keys.hasNext()) {
                    val k = keys.next()
                    out[k] = json.getLong(k)
                }
            }
        }
        tryCache = out
        return out
    }

    private fun save(context: Context) {
        val mapJson = JSONObject()
        loadMap(context).forEach { (k, v) -> mapJson.put(k, v) }
        val tryJson = JSONObject()
        loadTry(context).forEach { (k, v) -> tryJson.put(k, v) }
        prefs(context).edit()
            .putString(KEY_MAP, mapJson.toString())
            .putString(KEY_TRY, tryJson.toString())
            .apply()
    }

    fun keyOf(bean: AbstractBean): String = NbSubscriptionSync.keyOf(bean)

    fun countryFor(context: Context, bean: AbstractBean): String? =
        loadMap(context)[keyOf(bean)]

    fun attemptedRecently(context: Context, bean: AbstractBean): Boolean {
        val ts = loadTry(context)[keyOf(bean)] ?: return false
        return System.currentTimeMillis() - ts < RETRY_MS
    }

    // Deleted nodes leave stale entries behind forever; cap both
    // maps and drop the oldest first (mutableMapOf keeps insertion
    // order) so they stay bounded no matter how many nodes come and go.
    private const val MAX_GEO_ENTRIES = 1000
    private const val PRUNE_TO_ENTRIES = 800

    private val lock = Any()

    /** Restart cleanup: trim both maps to the cap when the app
     *  (re)starts, oldest first; the file is rewritten only when
     *  something was actually dropped. */
    fun pruneOnStart(context: Context) {
        synchronized(lock) {
            val map = loadMap(context)
            val tryMap = loadTry(context)
            var dropped = false
            if (map.size > MAX_GEO_ENTRIES) {
                pruneLocked(map)
                dropped = true
            }
            if (tryMap.size > MAX_GEO_ENTRIES) {
                pruneLocked(tryMap)
                dropped = true
            }
            if (dropped) save(context)
        }
    }

    private fun pruneLocked(map: MutableMap<String, *>) {
        if (map.size <= MAX_GEO_ENTRIES) return
        val it = map.keys.iterator()
        var drop = map.size - PRUNE_TO_ENTRIES
        while (drop-- > 0 && it.hasNext()) {
            it.next()
            it.remove()
        }
    }

    fun record(context: Context, bean: AbstractBean, countryCode: String?) {
        val key = keyOf(bean)
        synchronized(lock) {
            loadTry(context)[key] = System.currentTimeMillis()
            if (!countryCode.isNullOrBlank()) {
                loadMap(context)[key] = countryCode.uppercase()
            }
            pruneLocked(loadTry(context))
            pruneLocked(loadMap(context))
            save(context)
        }
    }

    private const val KEY_COLLAPSE = "geo_collapse"

    /** Stored expand state for a list section; null = no stored state. */
    fun sectionExpanded(context: Context, groupId: Long, key: String): Boolean? {
        val raw = prefs(context).getString(KEY_COLLAPSE, null) ?: return null
        return runCatching {
            val json = JSONObject(raw)
            if (json.has("$groupId|$key")) json.getBoolean("$groupId|$key") else null
        }.getOrNull()
    }

    fun setSectionExpanded(context: Context, groupId: Long, key: String, expanded: Boolean) {
        val raw = prefs(context).getString(KEY_COLLAPSE, null)
        val json = runCatching { if (raw != null) JSONObject(raw) else JSONObject() }
            .getOrDefault(JSONObject())
        json.put("$groupId|$key", expanded)
        prefs(context).edit().putString(KEY_COLLAPSE, json.toString()).apply()
    }

    /** Section title for a country, e.g. "🇺🇸 美国". */
    fun countryTitle(countryCode: String): String {
        val cc = countryCode.uppercase()
        val cn = runCatching {
            java.util.Locale("", cc).getDisplayCountry(java.util.Locale.CHINESE)
        }.getOrNull()?.takeIf { it.isNotBlank() && it != cc } ?: cc
        val flag = flag(cc)
        return if (flag.isEmpty()) cn else "$flag $cn"
    }

    /** Regional-indicator flag emoji for a 2-letter country code. */
    fun flag(countryCode: String): String {
        val cc = countryCode.uppercase()
        if (cc.length != 2) return ""
        val sb = StringBuilder()
        for (c in cc) {
            if (c !in 'A'..'Z') return ""
            sb.append(Character.toChars(0x1F1E6 + (c - 'A')))
        }
        return sb.toString()
    }
}

object NbGeoProbe {

    private const val TAG = "NbGeo"
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val running = AtomicBoolean(false)

    /** Fire-and-forget geo probe of the managed group. */
    fun enqueue(context: Context, groupId: Long) {
        if (!running.compareAndSet(false, true)) return
        val appContext = context.applicationContext
        scope.launch {
            try {
                probe(appContext, groupId)
            } catch (e: Exception) {
                Log.w(TAG, "probe failed", e)
            } finally {
                running.set(false)
            }
        }
    }

    /**
     * Post-sync pass over the profiles whose region is still
     * unknown — after a wholesale subscription sync those are
     * exactly the nodes the update just brought in (known beans
     * keep their stored country). Worked in batches of ten:
     * region first, then latency, then the next batch; a batch
     * holding a dead node or two changes nothing.
     */
    suspend fun probe(context: Context, groupId: Long) = withContext(Dispatchers.IO) {
        val profiles = SagerDatabase.proxyDao.getByGroup(groupId)
        val targets = profiles.filter { profile ->
            val bean = profile.requireBean()
            NbGeoStore.countryFor(context, bean) == null &&
                !NbGeoStore.attemptedRecently(context, bean)
        }
        if (targets.isEmpty()) return@withContext
        Log.i(TAG, "post-sync: ${targets.size} new profiles, batches of 10")
        for (chunk in targets.chunked(10)) {
            probeProfiles(context, chunk)
            chunk.map { profile ->
                async { runCatching { NbLoadBalancer.testPing(profile) } }
            }.awaitAll()
            // Sections are rendered from the country store; reload per batch.
            GroupManager.postReload(groupId)
        }
    }

    /**
     * One sweep phase over [targets] at [concurrency] workers;
     * yields the moment the user starts a manual connection test
     * when [yieldToManualTest] is set.
     */
    suspend fun runPhase(
        targets: List<ProxyEntity>,
        concurrency: Int = 100,
        yieldToManualTest: Boolean = false,
        task: suspend (ProxyEntity) -> Unit,
    ) = withContext(Dispatchers.IO) {
        if (targets.isEmpty()) return@withContext
        val queue = ConcurrentLinkedQueue(targets)
        val jobs = mutableListOf<kotlinx.coroutines.Job>()
        repeat(minOf(concurrency, targets.size)) {
            jobs.add(launch {
                while (true) {
                    if (yieldToManualTest && DataStore.runningTest) break
                    val profile = queue.poll() ?: break
                    task(profile)
                }
            })
        }
        jobs.forEach { it.join() }
    }

    /**
     * Two-phase country sweep at 100 concurrent: the primary
     * source (Cloudflare trace) runs over every target first;
     * only the nodes still unknown after that pass see the
     * fallback source (ip.sb). Each node is recorded exactly
     * once, when its final phase resolves.
     */
    suspend fun probeProfiles(
        context: Context,
        targets: List<ProxyEntity>,
        onProbed: (suspend () -> Unit)? = null,
    ) {
        if (targets.isEmpty()) return
        val leftovers = ConcurrentLinkedQueue<ProxyEntity>()
        runPhase(targets, yieldToManualTest = true) { profile ->
            val bean = profile.requireBean()
            val cc = runCatching {
                GeoTest().doFetchCountryPrimary(profile)
            }.getOrNull()
            if (cc != null) {
                NbGeoStore.record(context, bean, cc)
                onProbed?.let { runCatching { it() } }
                Log.i(TAG, "geo ${bean.displayName()} -> $cc (primary)")
            } else {
                leftovers.add(profile)
            }
        }
        val phase2 = leftovers.toList()
        if (phase2.isEmpty()) return
        runPhase(phase2, yieldToManualTest = true) { profile ->
            val bean = profile.requireBean()
            val cc = runCatching {
                GeoTest().doFetchCountryFallback(profile)
            }.getOrNull()
            NbGeoStore.record(context, bean, cc)
            onProbed?.let { runCatching { it() } }
            Log.i(TAG, "geo ${bean.displayName()} -> ${cc ?: "?"} (fallback)")
        }
    }

}
