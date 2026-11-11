package io.nekohasekai.sagernet.ui

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import androidx.lifecycle.lifecycleScope
import io.nekohasekai.sagernet.GroupType
import io.nekohasekai.sagernet.R
import io.nekohasekai.sagernet.auth.NbAuthManager
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.auth.NbSubscriptionSync
import kotlinx.coroutines.delay
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * Startup gate for NekoBox999: shown whenever there is no live
 * license session. Tries the silent paths first (console direct-open
 * claim, or re-validation of a locally expired license), then falls
 * back to activation-code entry. On success it pulls the built-in
 * subscription group once and hands over to MainActivity.
 */
class AuthGateActivity : AppCompatActivity() {

    private lateinit var statusView: TextView
    private lateinit var codeInput: EditText
    private lateinit var activateBtn: Button

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_auth_gate)

        val machineCode = NbAuthManager.machineCode(this)
        findViewById<TextView>(R.id.auth_machine_code).text = machineCode
        statusView = findViewById(R.id.auth_status)
        codeInput = findViewById(R.id.auth_code_input)
        activateBtn = findViewById(R.id.auth_activate_btn)

        findViewById<Button>(R.id.auth_copy_btn).setOnClickListener {
            val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
            cm.setPrimaryClip(ClipData.newPlainText("machineCode", machineCode))
            Toast.makeText(this, "机器码已复制", Toast.LENGTH_SHORT).show()
        }
        activateBtn.setOnClickListener { activateWithInput() }

        autoAuth()
    }

    private fun setBusy(busy: Boolean, text: String) {
        statusView.text = text
        activateBtn.isEnabled = !busy
    }

    private fun autoAuth() {
        lifecycleScope.launch {
            if (NbAuthManager.hasLiveSession(this@AuthGateActivity)) {
                enterApp()
                return@launch
            }
            if (NbAuthManager.hasToken(this@AuthGateActivity)) {
                // Locally expired: a renewal may exist server-side.
                setBusy(true, "授权已到期，正在联网验证…")
                when (val r = NbAuthManager.validateNow(this@AuthGateActivity)) {
                    is NbAuthManager.Outcome.Ok -> enterApp()
                    is NbAuthManager.Outcome.Denied ->
                        setBusy(false, "授权验证未通过（${r.reason}）。如已续费，请重新输入激活码。")
                    NbAuthManager.Outcome.NetworkError ->
                        setBusy(false, "网络不通，无法验证到期授权。请联网后重启应用，或重新输入激活码。")
                }
                return@launch
            }
            // No token: the console may have direct-opened this machine.
            setBusy(true, "正在验证授权…")
            when (val r = NbAuthManager.claim(this@AuthGateActivity)) {
                is NbAuthManager.Outcome.Ok -> enterApp()
                is NbAuthManager.Outcome.Denied ->
                    setBusy(false, "本机尚未开通。把机器码报给卖家开通，或输入激活码。")
                NbAuthManager.Outcome.NetworkError ->
                    setBusy(false, "网络不通，无法验证授权。请联网后重试，或输入激活码。")
            }
        }
    }

    private fun activateWithInput() {
        val code = codeInput.text?.toString()?.trim().orEmpty()
        if (code.isBlank()) {
            Toast.makeText(this, "先输入激活码", Toast.LENGTH_SHORT).show()
            return
        }
        lifecycleScope.launch {
            setBusy(true, "正在激活…")
            when (val r = NbAuthManager.activateWithCode(this@AuthGateActivity, code)) {
                is NbAuthManager.Outcome.Ok -> enterApp()
                is NbAuthManager.Outcome.Denied ->
                    setBusy(false, "激活失败（${r.reason}），检查激活码或联系卖家。")
                NbAuthManager.Outcome.NetworkError ->
                    setBusy(false, "网络不通，激活失败。联网后重试。")
            }
        }
    }

    private suspend fun enterApp() {
        setBusy(true, "授权通过，正在获取订阅…")
        val group = NbSubscriptionSync.ensureManagedGroup()
        when (val r = NbSubscriptionSync.sync(group)) {
            is NbSubscriptionSync.Result.Ok ->
                setBusy(true, "订阅已下发：${r.count} 个节点，正在进入…")
            is NbSubscriptionSync.Result.Failed ->
                setBusy(true, "订阅没拉下来：${r.reason}。先进应用，可在抽屉里点更新当前组订阅。")
        }
        delay(900)
        startActivity(Intent(this, MainActivity::class.java))
        finish()
    }
}
