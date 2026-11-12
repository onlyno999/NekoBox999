package io.nekohasekai.sagernet.auth

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/** Entry point for the expiry alarm; see [NbExpiryAlarm]. */
class NbExpiryReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent?) {
        if (intent?.action != NbExpiryAlarm.ACTION_EXPIRY) return
        NbExpiryAlarm.onAlarmFired(context)
    }
}
