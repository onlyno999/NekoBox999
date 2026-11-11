package io.nekohasekai.sagernet.auth

import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.GroupManager
import io.nekohasekai.sagernet.database.ProfileManager
import io.nekohasekai.sagernet.database.ProxyGroup
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.fmt.AbstractBean
import io.nekohasekai.sagernet.group.RawUpdater
import io.nekohasekai.sagernet.ktx.app
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Wholesale subscription sync for the built-in group, mirroring the
 * v2one999 client: pull the device subscription from the licensing
 * backend, parse it, and rebuild the group from scratch — keeping
 * the current selection when the same server is still present.
 * Runs entirely off the main thread.
 */
object NbSubscriptionSync {

    sealed interface Result {
        data class Ok(val count: Int) : Result
        data class Failed(val reason: String) : Result
    }

    /**
     * The built-in subscription group, created on the spot when it
     * does not exist yet — first entry right after activation must
     * not depend on the main UI having seeded it first.
     */
    suspend fun ensureManagedGroup(): ProxyGroup = withContext(Dispatchers.IO) {
        val groups = SagerDatabase.groupDao.allGroups()
        groups.firstOrNull {
            NbAuthManager.isManagedLink(it.subscription?.link)
        }?.let { return@withContext it }
        // Build the seed the same way the configuration UI does.
        val subBean = io.nekohasekai.sagernet.database.SubscriptionBean()
        subBean.link = NbAuthManager.homeUrl()
        subBean.autoUpdate = true
        subBean.autoUpdateDelay = 720
        val group = ProxyGroup(
            name = "v2one999",
            type = io.nekohasekai.sagernet.GroupType.SUBSCRIPTION,
            subscription = subBean
        )
        group.initializeDefaultValues()
        group.id = SagerDatabase.groupDao.createGroup(group)
        if (groups.isEmpty()) {
            SagerDatabase.groupDao.createGroup(ProxyGroup(ungrouped = true))
        }
        group
    }

    suspend fun sync(group: ProxyGroup): Result = withContext(Dispatchers.IO) {
        val selectedId = DataStore.selectedProxy
        val selectedKey = try {
            SagerDatabase.proxyDao.getById(selectedId)
                ?.requireBean()
                ?.let { keyOf(it) }
        } catch (e: Exception) {
            null
        }

        when (val fr = NbAuthManager.fetchSubscription(app)) {
            is NbAuthManager.FetchOutcome.Denied ->
                Result.Failed("授权未通过（${fr.reason}）")

            NbAuthManager.FetchOutcome.NetworkError ->
                Result.Failed("网络错误，订阅获取失败")

            is NbAuthManager.FetchOutcome.Ok -> {
                val beans = try {
                    RawUpdater.parseRaw(fr.content)
                } catch (e: Exception) {
                    null
                }
                if (beans.isNullOrEmpty()) {
                    Result.Failed("订阅内容解析不出节点")
                } else {
                    SagerDatabase.proxyDao.deleteByGroup(group.id)
                    var newSelected = 0L
                    for (bean in beans) {
                        val entity = ProfileManager.createProfile(group.id, bean)
                        if (selectedKey != null && keyOf(bean) == selectedKey) {
                            newSelected = entity.id
                        }
                    }
                    if (newSelected > 0L) {
                        DataStore.selectedProxy = newSelected
                    }
                    GroupManager.postReload(group.id)
                    NbAuthManager.markSynced(app)
                    // NekoBox999: probe node countries, then group by country.
                    Result.Ok(beans.size)
                }
            }
        }
    }

    internal fun keyOf(bean: AbstractBean): String =
        "${bean.javaClass.name}|${bean.serverAddress}|${bean.serverPort}"
}
