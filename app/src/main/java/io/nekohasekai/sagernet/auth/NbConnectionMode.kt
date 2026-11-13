package io.nekohasekai.sagernet.auth

import android.content.Context
import io.nekohasekai.sagernet.SagerNet

/**
 * NekoBox999 connection mode, switched from the drawer's
 * connection-mode entry:
 *  - RULES (default): traffic follows the user's route rules,
 *    unmatched traffic takes the main line.
 *  - GLOBAL: route rules are skipped, everything takes the
 *    main line.
 *  - DIRECT: route rules are skipped and the route final is
 *    the direct outbound, everything goes direct.
 * ConfigBuilder reads this for real builds only; test
 * configurations are unaffected.
 */
object NbConnectionMode {

    const val RULES = 0
    const val GLOBAL = 1
    const val DIRECT = 2

    private const val PREFS = "nb999_mode"
    private const val KEY = "connection_mode"

    fun current(ctx: Context = SagerNet.application): Int =
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getInt(KEY, RULES)

    fun set(ctx: Context, mode: Int) {
        ctx.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
            .edit().putInt(KEY, mode).apply()
    }

    fun label(mode: Int): String = when (mode) {
        GLOBAL -> "全局模式"
        DIRECT -> "直连模式"
        else -> "规则模式"
    }
}
