package io.nekohasekai.sagernet.auth

import android.content.Context
import android.provider.Settings
import android.util.Base64
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.spec.ECGenParameterSpec
import java.security.spec.X509EncodedKeySpec
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * NekoBox999 licensing, ported from the v2one999 client's online
 * authorization (same backend, same protocol). The app embeds only
 * the authorization base address; the real subscription content is
 * fetched per device after activation and never stored in the build.
 *
 * The first authorization (activation code or console direct-open
 * claim) is always online and stores the server-issued expiry; from
 * then on the stored expiry governs locally — the app opens without
 * a network check-in and re-validates in the background near expiry
 * or after the local expiry has passed. Subscription pulls stay
 * server-checked on every fetch.
 */
object NbAuthManager {

    private const val TAG = "NbAuth"
    private const val BASE_URL = "https://v2one999.xxxxxxx.nyc.mn"
    private const val SALT = "v2onlyno999-act-v1"
    private const val B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

    private const val PREFS = "nb999_auth"
    private const val KEY_TOKEN = "pref_online_token"
    private const val KEY_EXPIRES_AT = "pref_online_expires_at"
    private const val KEY_LAST_OK_MS = "pref_online_last_ok_ms"
    private const val KEY_MAX_SEEN_MS = "pref_online_max_seen_ms"
    private const val KEY_PRIV = "pref_online_privkey"
    private const val KEY_PUB = "pref_online_pubkey"
    private const val KEY_FALLBACK_ID = "pref_fallback_android_id"
    private const val KEY_LAST_SYNC_MS = "pref_last_sync_ms"
    private const val KEY_LAST_ASSET_MS = "pref_last_asset_ms"

    /** The built-in subscription group is managed when its link is the base address. */
    fun isManagedLink(link: String?): Boolean {
        val l = link?.trim()?.trimEnd('/') ?: return false
        return l == BASE_URL
    }

    private const val NEAR_EXPIRY_WINDOW_MS = 24 * 60 * 60_000L
    private const val ENTRY_CHECK_INTERVAL_MS = 24 * 60 * 60_000L

    private val httpClient: OkHttpClient by lazy {
        OkHttpClient.Builder()
            .connectTimeout(10, TimeUnit.SECONDS)
            .readTimeout(25, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .build()
    }

    sealed interface Outcome {
        data class Ok(val expiresAt: Long?) : Outcome
        data class Denied(val reason: String) : Outcome
        data object NetworkError : Outcome
    }

    sealed interface FetchOutcome {
        data class Ok(val content: String) : FetchOutcome
        data class Denied(val reason: String) : FetchOutcome
        data object NetworkError : FetchOutcome
    }

    // ---------- storage ----------

    private fun prefs(context: Context) =
        context.applicationContext.getSharedPreferences(PREFS, Context.MODE_PRIVATE)

    private fun getStr(context: Context, key: String): String? =
        prefs(context).getString(key, null)

    private fun getLong(context: Context, key: String, def: Long): Long =
        prefs(context).getLong(key, def)

    private fun putStr(context: Context, key: String, value: String?) {
        prefs(context).edit().putString(key, value).apply()
    }

    private fun putLong(context: Context, key: String, value: Long) {
        prefs(context).edit().putLong(key, value).apply()
    }

    /** Epoch ms of the last successful built-in subscription sync; 0 = never. */
    fun lastSyncMs(context: Context): Long = getLong(context, KEY_LAST_SYNC_MS, 0L)

    fun markSynced(context: Context) {
        putLong(context, KEY_LAST_SYNC_MS, System.currentTimeMillis())
    }

    /** Epoch ms of the last geo-asset update check; 0 = never. */
    fun lastAssetCheckMs(context: Context): Long = getLong(context, KEY_LAST_ASSET_MS, 0L)

    fun markAssetChecked(context: Context) {
        putLong(context, KEY_LAST_ASSET_MS, System.currentTimeMillis())
    }

    // ---------- machine code (same format as the v2one999 client) ----------

    fun machineCode(context: Context): String {
        var androidId = try {
            Settings.Secure.getString(context.contentResolver, Settings.Secure.ANDROID_ID)
        } catch (e: Exception) {
            null
        }
        if (androidId.isNullOrBlank()) {
            androidId = getStr(context, KEY_FALLBACK_ID)
            if (androidId.isNullOrBlank()) {
                androidId = UUID.randomUUID().toString()
                putStr(context, KEY_FALLBACK_ID, androidId)
            }
        }
        val digest = MessageDigest.getInstance("SHA-256")
            .digest("$SALT|$androidId|${context.packageName}".toByteArray(Charsets.UTF_8))
        val raw = b32Encode(digest.copyOf(10))
        return raw.chunked(4).joinToString("-")
    }

    private fun b32Encode(data: ByteArray): String {
        val sb = StringBuilder()
        var buffer = 0
        var bits = 0
        for (b in data) {
            buffer = (buffer shl 8) or (b.toInt() and 0xFF)
            bits += 8
            while (bits >= 5) {
                bits -= 5
                sb.append(B32[(buffer shr bits) and 31])
            }
        }
        if (bits > 0) {
            sb.append(B32[(buffer shl (5 - bits)) and 31])
        }
        return sb.toString()
    }

    // ---------- session state ----------

    fun hasToken(context: Context): Boolean = !token(context).isNullOrBlank()

    /** Server-issued expiry in epoch seconds; null = permanent or unknown. */
    fun expiresAt(context: Context): Long? {
        val v = getLong(context, KEY_EXPIRES_AT, -1L)
        return if (v <= 0L) null else v
    }

    /** Watermark clock: wall time that never moves backwards on this install. */
    fun effectiveNowMs(context: Context): Long {
        val effective = maxOf(
            System.currentTimeMillis(),
            getLong(context, KEY_MAX_SEEN_MS, 0L),
        )
        putLong(context, KEY_MAX_SEEN_MS, effective)
        return effective
    }

    fun localExpiryPassed(context: Context): Boolean {
        val exp = expiresAt(context) ?: return false
        return effectiveNowMs(context) / 1000L > exp
    }

    fun hasLiveSession(context: Context): Boolean =
        hasToken(context) && !localExpiryPassed(context)

    fun lastValidatedMs(context: Context): Long = getLong(context, KEY_LAST_OK_MS, 0L)

    fun validationDue(context: Context): Boolean {
        if (!hasToken(context)) return false
        val exp = expiresAt(context) ?: return false
        return exp * 1000L - effectiveNowMs(context) <= NEAR_EXPIRY_WINDOW_MS
    }

    fun entryCheckDue(context: Context): Boolean {
        if (!hasToken(context)) return false
        val last = lastValidatedMs(context)
        if (last <= 0L) return true
        return effectiveNowMs(context) - last >= ENTRY_CHECK_INTERVAL_MS
    }

    fun clearSession(context: Context) {
        putStr(context, KEY_TOKEN, null)
        putLong(context, KEY_EXPIRES_AT, -1L)
        putLong(context, KEY_LAST_OK_MS, 0L)
    }

    private fun token(context: Context): String? = getStr(context, KEY_TOKEN)

    private fun storeSession(context: Context, token: String, expiresAt: Long?) {
        putStr(context, KEY_TOKEN, token)
        putLong(context, KEY_EXPIRES_AT, expiresAt ?: -1L)
        putLong(context, KEY_LAST_OK_MS, effectiveNowMs(context))
    }

    // ---------- device key + signer ----------

    /** SPKI (X.509) public key of this device, Base64; generated once. */
    fun devicePubkeyB64(context: Context): String {
        val existing = getStr(context, KEY_PUB)
        if (!existing.isNullOrBlank()) return existing
        val generator = KeyPairGenerator.getInstance("EC")
        generator.initialize(ECGenParameterSpec("secp256r1"))
        val pair = generator.generateKeyPair()
        val pub = Base64.encodeToString(pair.public.encoded, Base64.NO_WRAP)
        val priv = Base64.encodeToString(pair.private.encoded, Base64.NO_WRAP)
        putStr(context, KEY_PUB, pub)
        putStr(context, KEY_PRIV, priv)
        return pub
    }

    /** Loads the stored device public key. */
    fun devicePublicKey(context: Context): java.security.PublicKey? {
        val pub = getStr(context, KEY_PUB) ?: return null
        return try {
            KeyFactory.getInstance("EC")
                .generatePublic(X509EncodedKeySpec(Base64.decode(pub, Base64.DEFAULT)))
        } catch (e: Exception) {
            Log.e(TAG, "bad stored device pubkey", e)
            null
        }
    }

    /** SHA-256 (hex) of this build's own signing certificate. */
    fun signerSha256(context: Context): String? = try {
        val pm = context.packageManager
        val pkg = context.packageName
        val signatures = if (android.os.Build.VERSION.SDK_INT >= 28) {
            pm.getPackageInfo(pkg, android.content.pm.PackageManager.GET_SIGNING_CERTIFICATES)
                .signingInfo?.apkContentsSigners
        } else {
            @Suppress("DEPRECATION")
            pm.getPackageInfo(pkg, android.content.pm.PackageManager.GET_SIGNATURES).signatures
        }
        signatures?.firstOrNull()?.let {
            MessageDigest.getInstance("SHA-256")
                .digest(it.toByteArray())
                .joinToString("") { b -> "%02x".format(b) }
        }
    } catch (e: Exception) {
        Log.e(TAG, "signer digest unavailable", e)
        null
    }

    // ---------- server calls ----------

    private fun baseBody(context: Context): JSONObject {
        val body = JSONObject()
            .put("machineCode", machineCode(context))
            .put("pubkey", devicePubkeyB64(context))
            .put("platform", "android")
        signerSha256(context)?.let { body.put("signer", it) }
        return body
    }

    suspend fun activateWithCode(context: Context, code: String): Outcome {
        val body = baseBody(context).put("code", code.trim())
        return activationCall(context, "/api/activate", body)
    }

    /** Console direct-open: succeeds when the console enabled this machine. */
    suspend fun claim(context: Context): Outcome =
        activationCall(context, "/api/claim", baseBody(context))

    suspend fun validateNow(context: Context): Outcome {
        val current = token(context) ?: return Outcome.Denied("no_token")
        val body = JSONObject()
            .put("machineCode", machineCode(context))
            .put("token", current)
        val (status, json) = postJson("/api/validate", body)
            ?: return Outcome.NetworkError
        if (status == 200 && json?.optBoolean("ok") == true) {
            val exp = if (json.isNull("expires_at")) null else json.optLong("expires_at")
            putLong(context, KEY_EXPIRES_AT, exp ?: -1L)
            putLong(context, KEY_LAST_OK_MS, effectiveNowMs(context))
            return Outcome.Ok(exp)
        }
        val reason = json?.optString("reason")?.takeIf { it.isNotBlank() } ?: "denied"
        if (reason == "bad_token" || reason == "no_device" || reason == "not_found") {
            clearSession(context)
        }
        return Outcome.Denied(reason)
    }

    /** Pulls the subscription content for this device's channel. */
    suspend fun fetchSubscription(context: Context): FetchOutcome {
        val current = token(context) ?: return FetchOutcome.Denied("no_token")
        val body = JSONObject()
            .put("machineCode", machineCode(context))
            .put("token", current)
        return withContext(Dispatchers.IO) {
            try {
                val request = Request.Builder()
                    .url(BASE_URL + "/api/subscription")
                    .post(body.toString().toRequestBody(JSON_MEDIA))
                    .build()
                httpClient.newCall(request).execute().use { response ->
                    val text = response.body?.string().orEmpty()
                    if (response.isSuccessful && text.isNotBlank()) {
                        FetchOutcome.Ok(text)
                    } else {
                        val reason = try {
                            JSONObject(text).optString("reason")
                        } catch (e: Exception) {
                            ""
                        }
                        if (reason == "bad_token" || reason == "no_device") {
                            clearSession(context)
                        }
                        FetchOutcome.Denied(reason.ifBlank { "sub_denied" })
                    }
                }
            } catch (e: Exception) {
                Log.e(TAG, "subscription fetch failed", e)
                FetchOutcome.NetworkError
            }
        }
    }

    private suspend fun activationCall(
        context: Context,
        path: String,
        body: JSONObject,
    ): Outcome {
        val (status, json) = postJson(path, body) ?: return Outcome.NetworkError
        if (status == 200 && json?.optBoolean("ok") == true) {
            val newToken = json.optString("token")
            if (newToken.isBlank()) return Outcome.Denied("no_token_returned")
            val exp = if (json.isNull("expires_at")) null else json.optLong("expires_at")
            storeSession(context, newToken, exp)
            return Outcome.Ok(exp)
        }
        val reason = json?.optString("reason")?.takeIf { it.isNotBlank() } ?: "denied"
        return Outcome.Denied(reason)
    }

    /** Returns (HTTP status, parsed JSON) or null on any network failure. */
    private suspend fun postJson(path: String, body: JSONObject): Pair<Int, JSONObject?>? =
        withContext(Dispatchers.IO) {
            try {
                val request = Request.Builder()
                    .url(BASE_URL + path)
                    .post(body.toString().toRequestBody(JSON_MEDIA))
                    .build()
                httpClient.newCall(request).execute().use { response ->
                    val text = response.body?.string().orEmpty()
                    val json = try {
                        JSONObject(text)
                    } catch (e: Exception) {
                        null
                    }
                    response.code to json
                }
            } catch (e: Exception) {
                Log.e(TAG, "POST $path failed", e)
                null
            }
        }

    private val JSON_MEDIA = "application/json; charset=utf-8".toMediaType()
}
