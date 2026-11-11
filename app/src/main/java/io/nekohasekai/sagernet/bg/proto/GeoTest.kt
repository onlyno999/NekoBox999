package io.nekohasekai.sagernet.bg.proto

import io.nekohasekai.sagernet.database.ProxyEntity
import org.json.JSONObject

/**
 * Fetches the exit country through a single profile, so the answer
 * describes that profile's exit location. NekoBox999 addition.
 *
 * Primary source is Cloudflare's trace endpoint (plain text with a
 * `loc=XX` line, tiny and fast, happy with heavy concurrency); the
 * ip.sb geo JSON is the fallback.
 */
class GeoTest {

    val link = "https://api.ip.sb/geoip"
    private val traceLink = "https://cp.cloudflare.com/cdn-cgi/trace"
    private val timeout = 8000

    suspend fun doFetch(profile: ProxyEntity): String {
        return TestInstance(profile, link, timeout).doFetch()
    }

    /** Two-letter exit country code for the profile, or null. */
    suspend fun doFetchCountry(profile: ProxyEntity): String? {
        runCatching {
            val body = TestInstance(profile, traceLink, timeout).doFetch()
            for (line in body.lineSequence()) {
                if (line.startsWith("loc=")) {
                    return line.substring(4).trim()
                        .takeIf { it.length == 2 }
                }
            }
        }
        return runCatching {
            JSONObject(doFetch(profile)).optString("country_code")
                .takeIf { it.isNotBlank() }
        }.getOrNull()
    }

}
