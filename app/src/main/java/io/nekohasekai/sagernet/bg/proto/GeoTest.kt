package io.nekohasekai.sagernet.bg.proto

import io.nekohasekai.sagernet.database.ProxyEntity

/**
 * Fetches the geo-IP JSON through a single profile, so the answer
 * describes that profile's exit location. NekoBox999 addition.
 */
class GeoTest {

    val link = "https://api.ip.sb/geoip"
    private val timeout = 8000

    suspend fun doFetch(profile: ProxyEntity): String {
        return TestInstance(profile, link, timeout).doFetch()
    }

}
