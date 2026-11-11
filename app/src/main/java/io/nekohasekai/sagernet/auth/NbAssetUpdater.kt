package io.nekohasekai.sagernet.auth

import android.content.Context
import android.util.Log
import io.nekohasekai.sagernet.database.DataStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import libcore.Libcore
import moe.matsuri.nb4a.utils.Util
import org.json.JSONObject
import java.io.File

/**
 * Headless geo-asset (route rules database) updater for NekoBox999:
 * the same sources and version check as the assets screen, run
 * silently from the main screen — first open, then at most once
 * every 24 hours.
 */
object NbAssetUpdater {

    private const val TAG = "NbAsset"
    private const val INTERVAL_MS = 24 * 60 * 60_000L

    private val assetNames = arrayOf("geoip.db", "geosite.db")

    // Same provider table as the assets screen, in the same order.
    private val providers = listOf(
        mapOf("geoip.db" to "SagerNet/sing-geoip", "geosite.db" to "SagerNet/sing-geosite"),
        mapOf("geoip.db" to "soffchen/sing-geoip", "geosite.db" to "soffchen/sing-geosite"),
        mapOf(
            "geoip.db" to "Chocolate4U/Iran-sing-box-rules",
            "geosite.db" to "Chocolate4U/Iran-sing-box-rules",
        ),
        mapOf(
            "geoip.db" to "L11R/antizapret-sing-box-geo",
            "geosite.db" to "L11R/antizapret-sing-box-geo",
        ),
    )

    fun due(context: Context): Boolean {
        val last = NbAuthManager.lastAssetCheckMs(context)
        return last <= 0L || System.currentTimeMillis() - last >= INTERVAL_MS
    }

    suspend fun updateAll(context: Context) = withContext(Dispatchers.IO) {
        NbAuthManager.markAssetChecked(context)
        val dir = context.getExternalFilesDir(null) ?: context.filesDir
        for (name in assetNames) {
            runCatching {
                updateOne(dir, name)
            }.onFailure {
                Log.w(TAG, "asset $name update failed", it)
            }
        }
    }

    private fun updateOne(dir: File, fileName: String) {
        val file = File(dir, fileName)
        val versionFile = File(dir, file.nameWithoutExtension + ".version.txt")
        val localVersion = if (versionFile.isFile) {
            try {
                versionFile.readText().trim()
            } catch (e: Exception) {
                ""
            }
        } else {
            ""
        }

        val provider = providers.getOrNull(DataStore.rulesProvider) ?: providers[0]
        val repo = provider[fileName] ?: return

        val client = Libcore.newHttpClient().apply {
            modernTLS()
            keepAlive()
            trySocks5(DataStore.mixedPort)
        }
        try {
            var response = client.newRequest().apply {
                setURL("https://api.github.com/repos/$repo/releases/latest")
            }.execute()

            val release = JSONObject(Util.getStringBox(response.contentString))
            val tagName = release.optString("tag_name")
            if (tagName.isBlank() || tagName == localVersion) return

            val assets = release.getJSONArray("assets")
            var downloadUrl: String? = null
            for (i in 0 until assets.length()) {
                val asset = assets.getJSONObject(i)
                if (asset.optString("name") == fileName) {
                    downloadUrl = asset.optString("browser_download_url")
                    break
                }
            }
            val url = downloadUrl ?: error("asset $fileName not in release")

            response = client.newRequest().apply {
                setURL(url)
            }.execute()

            val cacheFile = File(dir, "$fileName.tmp")
            dir.mkdirs()
            response.writeTo(cacheFile.canonicalPath)

            if (fileName.endsWith(".xz")) {
                Libcore.unxz(cacheFile.absolutePath, file.absolutePath)
                cacheFile.delete()
            } else {
                if (file.exists()) file.delete()
                cacheFile.renameTo(file)
            }
            versionFile.writeText(tagName)
            Log.i(TAG, "asset $fileName updated to $tagName")
        } finally {
            client.close()
        }
    }
}
