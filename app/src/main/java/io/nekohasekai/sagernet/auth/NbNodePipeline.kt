package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import io.nekohasekai.sagernet.bg.proto.UrlTest
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.GroupManager
import io.nekohasekai.sagernet.database.ProfileManager
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.ktx.readableMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Post-sync node pipeline for NekoBox999, in the user's fixed order:
 * FIRST speed-test the fresh nodes (which also proves liveness and
 * writes truthful statuses), THEN geo-probe only the nodes that
 * tested alive, and the country sections render from the store last.
 * The geo phase never overlaps this test phase.
 */
object NbNodePipeline {

    private const val TAG = "NbPipeline"
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val running = AtomicBoolean(false)

    fun enqueue(context: Context, groupId: Long) {
        if (!running.compareAndSet(false, true)) return
        val appContext = context.applicationContext
        scope.launch {
            try {
                run(appContext, groupId)
            } catch (e: Exception) {
                Log.w(TAG, "pipeline failed", e)
            } finally {
                running.set(false)
            }
        }
    }

    private suspend fun run(context: Context, groupId: Long) = withContext(Dispatchers.IO) {
        val profiles = SagerDatabase.proxyDao.getByGroup(groupId)
        if (profiles.isEmpty()) return@withContext

        // Phase 1: speed test every node whose status is unknown.
        val fresh = profiles.filter { it.status == 0 }
        if (fresh.isNotEmpty()) {
            Log.i(TAG, "phase 1: url-test ${fresh.size} fresh nodes")
            val queue = ConcurrentLinkedQueue(fresh)
            val jobs = mutableListOf<Job>()
            val concurrent = DataStore.connectionTestConcurrent.coerceIn(1, 16)
            repeat(concurrent) {
                jobs.add(launch {
                    while (true) {
                        val profile = queue.poll() ?: break
                        testOne(profile)
                    }
                })
            }
            jobs.forEach { it.join() }
            GroupManager.postReload(groupId)
        }

        // Phase 2: geo-probe only nodes that tested alive and still
        // lack a country (or were not attempted in the last day).
        val alive = profiles.filter { profile ->
            profile.status == 1 &&
                NbGeoStore.countryFor(context, profile.requireBean()) == null &&
                !NbGeoStore.attemptedRecently(context, profile.requireBean())
        }
        if (alive.isNotEmpty()) {
            Log.i(TAG, "phase 2: geo-probe ${alive.size} alive nodes")
            NbGeoProbe.probeProfiles(context, alive)
            GroupManager.postReload(groupId)
        }
    }

    private suspend fun testOne(profile: ProxyEntity) {
        try {
            val result = UrlTest().doTest(profile)
            profile.status = 1
            profile.ping = result
        } catch (e: Exception) {
            profile.status = 3
            profile.error = e.readableMessage
        }
        runCatching { ProfileManager.updateProfile(profile) }
    }
}
