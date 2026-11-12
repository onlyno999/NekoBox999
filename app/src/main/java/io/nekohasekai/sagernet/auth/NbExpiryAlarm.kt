package io.nekohasekai.sagernet.auth

import android.app.AlarmManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.os.Build
import io.nekohasekai.sagernet.SagerNet
import io.nekohasekai.sagernet.ktx.Logs

/**
 * NekoBox999: expiry enforcement as an alarm clock, not a poll.
 *
 * Whenever the app learns the license expiry (activation,
 * direct-open claim, background re-validation), [reschedule]
 * stores nothing new — it simply points an AlarmManager alarm
 * at the already-stored expiry moment. When the alarm fires,
 * [NbExpiryReceiver] stops the service. No loops, no periodic
 * checks, no network in the decision: the backend stops
 * serving an expired device anyway; this is only the local
 * cutoff that mirrors it.
 *
 * If the stored expiry is already past when (re)arming, the
 * service stops on the spot. A permanent license disarms.
 * Renewals move the single alarm (same PendingIntent), and
 * the receiver re-checks the stored expiry before cutting,
 * so a stale alarm can never cut a renewed license.
 */
object NbExpiryAlarm {

    const val ACTION_EXPIRY = "io.nekohasekai.sagernet.auth.EXPIRY"
    private const val REQUEST_CODE = 991207

    private fun pendingIntent(context: Context): PendingIntent {
        val intent = Intent(context, NbExpiryReceiver::class.java).setAction(ACTION_EXPIRY)
        return PendingIntent.getBroadcast(
            context, REQUEST_CODE, intent,
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
        )
    }

    /** Re-point the alarm at the currently stored expiry. */
    fun reschedule(context: Context) {
        val appCtx = context.applicationContext
        val am = appCtx.getSystemService(Context.ALARM_SERVICE) as AlarmManager
        val pi = pendingIntent(appCtx)
        if (!NbAuthManager.hasToken(appCtx)) {
            am.cancel(pi)
            return
        }
        val exp = NbAuthManager.expiresAt(appCtx)
        if (exp == null) {
            am.cancel(pi)
            return
        }
        if (NbAuthManager.localExpiryPassed(appCtx)) {
            am.cancel(pi)
            Logs.w("NbExpiryAlarm: expiry already passed; stopping service")
            SagerNet.stopService()
            return
        }
        val triggerAt = exp * 1000L
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && !am.canScheduleExactAlarms()) {
                throw SecurityException("exact alarms not granted")
            }
            am.setExactAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pi)
        } catch (e: SecurityException) {
            am.setAndAllowWhileIdle(AlarmManager.RTC_WAKEUP, triggerAt, pi)
        }
        Logs.w("NbExpiryAlarm: armed for $triggerAt")
    }

    /** Called when the alarm fires: cut if the license is dead. */
    fun onAlarmFired(context: Context) {
        val appCtx = context.applicationContext
        if (!NbAuthManager.hasToken(appCtx) || NbAuthManager.localExpiryPassed(appCtx)) {
            Logs.w("NbExpiryAlarm: fired with a dead license; stopping service")
            SagerNet.stopService()
        } else {
            // The expiry moved (renewed) after this alarm was set.
            reschedule(appCtx)
        }
    }
}
