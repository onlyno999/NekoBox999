package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import libcore.Libcore
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * NekoBox999: the CN domain rule set, delivered remotely and
 * filtered on-device. The raw V2-open geosite-cn data claims
 * Google hosts (dl.google.com, googleapis, gvt, gstatic),
 * which stalls Play Store downloads when they land on the
 * direct path, so the app downloads the set itself, lets the
 * core library strip every Google entry, and only ever hands
 * the core the filtered local copy. Refreshed at most once a
 * day; a failed refresh keeps the previous filtered copy.
 */
object NbRuleSets {

    private const val TAG = "NbRuleSets"
    private const val DIR_NAME = "rulesets"
    private const val FILE_NAME = "geosite-cn.srs"
    private const val MAX_AGE_MS = 24 * 60 * 60 * 1000L

    private val URLS = listOf(
        "https://cdn.jsdelivr.net/gh/SagerNet/sing-geosite@rule-set/geosite-cn.srs",
        "https://raw.githubusercontent.com/SagerNet/sing-geosite/rule-set/geosite-cn.srs",
    )

    private val running = AtomicBoolean(false)

    fun filteredFile(ctx: Context): File =
        File(File(ctx.filesDir, DIR_NAME), FILE_NAME)

    /** Blocking; call from a background thread. */
    fun ensureFresh(ctx: Context) {
        val dest = filteredFile(ctx)
        if (dest.exists() && dest.length() > 0 &&
            System.currentTimeMillis() - dest.lastModified() < MAX_AGE_MS
        ) {
            return
        }
        if (!running.compareAndSet(false, true)) return
        try {
            val dir = dest.parentFile ?: return
            if (!dir.exists()) dir.mkdirs()
            val raw = File(dir, "geosite-cn-raw.srs")
            val out = File(dir, "geosite-cn-filtered.srs")
            val client = OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build()
            var downloaded = false
            for (url in URLS) {
                try {
                    client.newCall(Request.Builder().url(url).build()).execute().use { resp ->
                        if (!resp.isSuccessful) return@use
                        val bytes = resp.body?.bytes() ?: return@use
                        raw.writeBytes(bytes)
                        downloaded = true
                    }
                    if (downloaded) break
                } catch (e: Exception) {
                    Log.w(TAG, "download failed from $url: $e")
                }
            }
            if (!downloaded) return
            Libcore.filterCnRuleSet(raw.absolutePath, out.absolutePath)
            if (out.exists() && out.length() > 0) {
                out.renameTo(dest)
                dest.setLastModified(System.currentTimeMillis())
            }
            raw.delete()
            out.delete()
        } catch (e: Exception) {
            Log.w(TAG, "refresh failed: $e")
        } finally {
            running.set(false)
        }
    }
}
