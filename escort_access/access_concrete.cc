#include "access_concrete.h"

unsigned int AccessService::g_uiRequestSequence = 0;
int AccessService::g_nRefCount = 0;
pthread_mutex_t AccessService::g_mutex4RequestSequence;
pthread_mutex_t AccessService::g_mutex4DevList;
pthread_mutex_t AccessService::g_mutex4GuarderList;
pthread_mutex_t AccessService::g_mutex4TaskList;
pthread_mutex_t AccessService::g_mutex4TaskId;
zhash_t * AccessService::g_deviceList = NULL;
zhash_t * AccessService::g_guarderList = NULL;
zhash_t * AccessService::g_taskList = NULL;
BOOL AccessService::g_bInitBuffer = FALSE;

#define MAKE_APPMSGHEAD(x) {x.marker[0] = 'E'; x.marker[1] = 'C'; x.version[0] = '1';\
 x.version[1] = '0';}

static unsigned long strdatetime2time(const char * strDatetime)
{
	struct tm tm_curr;
	sscanf_s(strDatetime, "%04d%02d%02d%02d%02d%02d", &tm_curr.tm_year, &tm_curr.tm_mon, &tm_curr.tm_mday,
		&tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
	tm_curr.tm_year -= 1900;
	tm_curr.tm_mon -= 1;
	return (unsigned long)mktime(&tm_curr);
}

static void format_datetime(unsigned long ulSrcTime, char * pStrDatetime, size_t nStrDatetimeLen)
{
	tm tm_time;
	time_t srcTime = ulSrcTime;
	localtime_s(&tm_time, &srcTime);
	char szDatetime[20] = { 0 };
	snprintf(szDatetime, sizeof(szDatetime), "%04d%02d%02d%02d%02d%02d", tm_time.tm_year + 1900,
		tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	size_t nLen = strlen(szDatetime);
	if (pStrDatetime && nStrDatetimeLen >= nLen) {
		strncpy_s(pStrDatetime, nStrDatetimeLen, szDatetime, nLen);
	}
}

static char * make_zkpath(int num, ...)
{
	const char * tmp_string;
	va_list arguments;
	va_start(arguments, num);
	int nTotalLen = 0;
	for (int i = 0; i < num; i++) {
		tmp_string = va_arg(arguments, const char *);
		if (tmp_string) {
			nTotalLen += strlen(tmp_string);
		}
	}
	va_end(arguments);
	char * path = (char *)malloc(nTotalLen + 1);
	if (path) {
		memset(path, 0, nTotalLen + 1);
		va_start(arguments, num);
		for (int i = 0; i < num; i++) {
			tmp_string = va_arg(arguments, const char *);
			if (tmp_string) {
				strcat_s(path, nTotalLen + 1, tmp_string);
			}
		}
	}
	return path;
}

void __stdcall fMsgCb(int nType, void * pMsg, void * pUserData)
{
	AccessService * pService = (AccessService *)pUserData;
	if (pService) {
		switch (nType) {
			case MSG_LINK_CONNECT: {
				std::string strLink = (char *)pMsg;
				access_service::LinkDataInfo * pLinkData = new access_service::LinkDataInfo();
				strncpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), strLink.c_str(), strLink.size());
				pLinkData->pLingeData = NULL;
				pLinkData->nLinkState = 0;
				pLinkData->ulTotalDataLen = 0;
				pLinkData->ulLackDataLen = 0;
				pLinkData->ulLingeDataLen = 0;
				pthread_mutex_lock(&pService->m_mutex4LinkDataList);
				pService->m_linkDataList.insert(std::make_pair(strLink, pLinkData));
				pthread_mutex_unlock(&pService->m_mutex4LinkDataList);
				break;
			}
			case MSG_LINK_DISCONNECT: {
				const char * pStrLink = (char *)pMsg;
				pService->handleLinkDisconnect(pStrLink);
				break;
			}
			case MSG_DATA: {
				MessageContent * pMsgCtx = (MessageContent *)pMsg;
				MessageContent * pMsgCtxCopy = new MessageContent();
				size_t nSize = sizeof(MessageContent);
				memcpy_s(pMsgCtxCopy, nSize, pMsgCtx, nSize);
				pMsgCtxCopy->pMsgData = new unsigned char[pMsgCtxCopy->ulMsgDataLen + 1];
				memcpy_s(pMsgCtxCopy->pMsgData, pMsgCtxCopy->ulMsgDataLen, pMsgCtx->pMsgData, 
					pMsgCtx->ulMsgDataLen);
				if (!pService->addAppMsg(pMsgCtxCopy)) {
					delete[] pMsgCtxCopy->pMsgData;
					delete pMsgCtxCopy;
				}
				break;
			}
		}
	}
}

void zk_server_watcher(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx)
{
	if (type == ZOO_SESSION_EVENT) {
		AccessService * pInst = (AccessService *)watcherCtx;
		if (state == ZOO_CONNECTED_STATE) {
			if (pInst && pInst->m_nRun) {
				pInst->m_bConnectZk = true;
			}
		}
		else if (state == ZOO_EXPIRED_SESSION_STATE) {
			if (pInst) {
				pInst->m_bConnectZk = false;
				zookeeper_close(pInst->m_zkHandle);
				pInst->m_zkHandle = NULL;
			}
		}
	}
}

void zk_escort_create_completion(int rc, const char * name, const void * data)
{
	AccessService * pService = (AccessService *)data;
	switch (rc) {
		case ZCONNECTIONLOSS: 
		case ZOPERATIONTIMEOUT: {
			if (pService && pService->m_nRun && pService->m_zkHandle) {
				zoo_acreate(pService->m_zkHandle, "/escort", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0,
					zk_escort_create_completion, data);
			}
			break;
		}
		case ZOK: {
			break;
		}
		case ZNODEEXISTS: {
			break;
		}
	}
}

void zk_access_create_completion(int rc, const char * name, const void * data)
{
	AccessService * pService = (AccessService *)data;
	switch (rc) {
		case ZCONNECTIONLOSS: 
		case ZOPERATIONTIMEOUT: {
			if (pService && pService->m_nRun) {
				zoo_acreate(pService->m_zkHandle, "/escort/access", "", 1024, &ZOO_OPEN_ACL_UNSAFE, 0,
					zk_access_create_completion, data);
			}
			break;
		}
		case ZOK: {
			break;
		}
		case ZNODEEXISTS: {
			break;
		}
		default: break;
	}
}

void zk_access_master_create_completion(int rc, const char * name, const void * data)
{
	AccessService * pService = (AccessService *)data;
	switch (rc) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pService && pService->m_nRun) {
				pService->competeForMaster();
			}
			break;
		}
		case ZOK: {			
			if (pService && pService->m_nRun) {
				if (pService->m_zkAccess.usRank == 0 && strlen(pService->m_zkNodePath) > 0) {
					pService->removeSlaver();
				}
				size_t nSize = strlen(name);
				memcpy_s(pService->m_zkNodePath, sizeof(pService->m_zkNodePath), name, nSize);
				pService->m_zkNodePath[nSize] = '\0';
				pService->m_zkAccess.usRank = 1;
				pService->setAccessData(name, &pService->m_zkAccess, sizeof(pService->m_zkAccess));
			}
			break;
		}
		case ZNODEEXISTS: {
			if (pService && pService->m_nRun) {
				pService->masterExist();
				pService->runAsSlaver();
			}
		}
	}
}

void zk_access_master_exists_watcher(zhandle_t * zh, int type, int state, const char * path,
	void * watcherCtx)
{
	if (type == ZOO_DELETED_EVENT) {
		AccessService * pService = (AccessService *)watcherCtx;
		if (pService) {
			if (pService->m_nRun) {
				pService->competeForMaster();
			}
		}
	}
}

void zk_access_master_exists_completion(int rc, const Stat * stat, const void * data)
{
	AccessService * pService = (AccessService *)data;
	switch (rc) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pService && pService->m_nRun) {
				pService->masterExist();
			}
			break;
		}
		case ZOK: {
			break;
		}
		case ZNONODE: {
			if (pService && pService->m_nRun) {
				pService->competeForMaster();
			}
			break;
		}
	}
}

void zk_access_set_completion(int rc, const struct Stat * stat, const void * data)
{
	switch (rc) {
		case ZOK: {
			break;
		}
		case ZCONNECTIONLOSS: 
		case ZOPERATIONTIMEOUT: {
			AccessService * pService = (AccessService *)data;
			if (pService && pService->m_nRun) {
				pService->setAccessData(pService->m_zkNodePath, &pService->m_zkAccess, sizeof(pService->m_zkAccess));
			}
			break;
		}
		case ZNONODE: {
			break;
		}
	}
}

void zk_access_slaver_create_completion(int rc, const char * name, const void * data)
{
	AccessService * pService = (AccessService *)data;
	switch (rc) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pService && pService->m_nRun) {
				pService->runAsSlaver();
			}
			break;
		}
		case ZOK: {
			if (pService && pService->m_nRun) {
				size_t nSize = strlen(name);
				memcpy_s(pService->m_zkNodePath, sizeof(pService->m_zkNodePath), name, nSize);
				pService->m_zkNodePath[nSize] = '\0';
				pService->m_zkAccess.usRank = 0;
				pService->setAccessData(name, &pService->m_zkAccess, sizeof(pService->m_zkAccess));
			}
			break;
		}
	}
}

AccessService::AccessService(const char * pZkHost, const char * pRoot)
{
	pthread_mutex_init(&m_mutex4LogQueue, NULL);
	pthread_cond_init(&m_cond4LogQueue, NULL);
	pthread_mutex_init(&m_mutex4AppMsgQueue, NULL);
	pthread_cond_init(&m_cond4AppMsgQueue, NULL);
	pthread_mutex_init(&m_mutex4LinkDataList, NULL);
	pthread_mutex_init(&m_mutex4LinkList, NULL);
	pthread_mutex_init(&m_mutex4SubscribeList, NULL);
	pthread_mutex_init(&m_mutex4InteractionMsgQueue, NULL);
	pthread_cond_init(&m_cond4InteractionMsgQueue, NULL);
	pthread_mutex_init(&m_mutex4TopicMsgQueue, NULL);
	pthread_cond_init(&m_cond4TopicMsgQueue, NULL);
	pthread_mutex_init(&m_mutex4LocalTopicMsgList, NULL);
	pthread_mutex_init(&m_mutex4RemoteLink, NULL);
	if (g_nRefCount == 0) {
		pthread_mutex_init(&g_mutex4RequestSequence, NULL);
		pthread_mutex_init(&g_mutex4DevList, NULL);
		pthread_mutex_init(&g_mutex4GuarderList, NULL);
		pthread_mutex_init(&g_mutex4TaskList, NULL);
		pthread_mutex_init(&g_mutex4TaskId, NULL);
		g_deviceList = zhash_new();
		g_guarderList = zhash_new();
		g_taskList = zhash_new(); 
		//readDataBuffer();
		g_bInitBuffer = FALSE;
		g_nRefCount = 1;
	}
	else {
		g_nRefCount++;
	}
	m_nRun = 0;
	m_pthdLog.p = NULL;
	m_pthdNetwork.p = NULL;
	m_pthdAppMsg.p = NULL;
	m_pthdSupervisor.p = NULL;
	m_pthdTopicMsg.p = NULL;
	m_pthdInteractionMsg.p = NULL;

	m_ctx = NULL;
	m_interactor = NULL;
	m_subscriber = NULL;

	m_linkList = zhash_new();
	m_subscribeList = zhash_new();
	m_localTopicMsgList = zlist_new();

	m_szHost[0] = '\0';
	m_szLogRoot[0] = '\0';
	m_nLogInst = 0;
	m_nLogType = LOGTYPE_FILE;

	m_bConnectZk = false;
	if (pZkHost && strlen(pZkHost)) {
		strncpy_s(m_szHost, sizeof(m_szHost), pZkHost, strlen(pZkHost));
	}
	initZookeeper();

	if (pRoot && strlen(pRoot)) {
		strncpy_s(m_szLogRoot, sizeof(m_szLogRoot), pRoot, strlen(pRoot));
	}
	initLog();

}

AccessService::~AccessService()
{
	if (m_nRun) {
		StopAccessService();
	}
	if (m_linkList) {
		zhash_destroy(&m_linkList);
		m_linkList = NULL;
	}
	if (m_subscribeList) {
		zhash_destroy(&m_subscribeList);
		m_subscribeList = NULL;
	}
	if (m_localTopicMsgList) {
		zlist_destroy(&m_localTopicMsgList);
		m_localTopicMsgList = NULL;
	}
	if (m_nLogInst) {
		LOG_Release(m_nLogInst);
		m_nLogInst = 0;
	}
	g_nRefCount--;
	if (g_nRefCount <= 0) {
		g_bInitBuffer = FALSE;
		if (g_deviceList) {
			zhash_destroy(&g_deviceList);
			g_deviceList = NULL;
		}
		if (g_guarderList) {
			zhash_destroy(&g_guarderList);
			g_guarderList = NULL;
		}
		if (g_taskList) {
			zhash_destroy(&g_guarderList);
			g_taskList = NULL;
		}
		pthread_mutex_destroy(&g_mutex4RequestSequence);
		pthread_mutex_destroy(&g_mutex4DevList);
		pthread_mutex_destroy(&g_mutex4GuarderList);
		pthread_mutex_destroy(&g_mutex4TaskList);
		pthread_mutex_destroy(&g_mutex4TaskId);
		g_nRefCount = 0;
	}
	pthread_mutex_destroy(&m_mutex4LogQueue);
	pthread_mutex_destroy(&m_mutex4LinkDataList);
	pthread_mutex_destroy(&m_mutex4AppMsgQueue);
	pthread_mutex_destroy(&m_mutex4LinkList);
	pthread_mutex_destroy(&m_mutex4SubscribeList);
	pthread_mutex_destroy(&m_mutex4InteractionMsgQueue);
	pthread_mutex_destroy(&m_mutex4TopicMsgQueue);
	pthread_cond_destroy(&m_cond4LogQueue);
	pthread_cond_destroy(&m_cond4AppMsgQueue);
	pthread_cond_destroy(&m_cond4InteractionMsgQueue);
	pthread_cond_destroy(&m_cond4TopicMsgQueue);
	pthread_mutex_destroy(&m_mutex4LocalTopicMsgList);
	pthread_mutex_destroy(&m_mutex4RemoteLink);
	if (m_zkHandle) {
		zookeeper_close(m_zkHandle);
		m_zkHandle = NULL;
	}

	if (m_ctx) {
		zctx_destroy(&m_ctx);
	}
}

//1. need know MSG-SERVER information: PUB server address and port, Talk server address and port
//
int AccessService::StartAccessService(const char * pHost, unsigned short usServicePort, 
	const char * pMidwareHost, unsigned short usPublishPort, unsigned short usContactPort, 
	const char * pDbProxyHost, unsigned short usQueryPort)
{
	if (m_nRun) {
		return 0;
	}
	if (!m_ctx) {
		m_ctx = zctx_new();
	}
	//start zeroMq
	m_subscriber = zsocket_new(m_ctx, ZMQ_SUB);
	zsocket_set_subscribe(m_subscriber, "");
	zsocket_connect(m_subscriber, "tcp://%s:%d", strlen(pMidwareHost) ? pMidwareHost : "127.0.0.1", 
		usPublishPort > 0 ? usPublishPort : 25000);
	m_interactor = zsocket_new(m_ctx, ZMQ_DEALER);
	zuuid_t * uuid = zuuid_new();
	const char * szUuid = zuuid_str(uuid);
	zsocket_set_identity(m_interactor, szUuid);
	zsocket_connect(m_interactor, "tcp://%s:%d", strlen(pMidwareHost) ? pMidwareHost : "127.0.0.1",
		usContactPort > 0 ? usContactPort : 25001);
	zuuid_destroy(&uuid);

	m_seeker = zsocket_new(m_ctx, ZMQ_REQ);
	uuid = zuuid_new();
	const char * szUuid2 = zuuid_str(uuid);
	zsocket_set_identity(m_seeker, szUuid2);
	zsocket_connect(m_seeker, "tcp://%s:%u", strlen(pDbProxyHost) ? pDbProxyHost : "127.0.0.1",
		usQueryPort > 0 ? usQueryPort : 21800);
	zuuid_destroy(&uuid);

	m_remoteMsgSrvLink.nActive = 0;
	m_remoteMsgSrvLink.ulLastActiveTime = 0;
	m_remoteProxyLink.nActive = 0;
	m_remoteProxyLink.ulLastActiveTime = 0;
	
	//open access port
	unsigned int uiSrvInst = TS_StartServer(usServicePort, m_nLogType, fMsgCb, this);
	if (uiSrvInst > 0) {
		m_uiSrvInst = uiSrvInst;
		m_usSrvPort = usServicePort;
		m_nRun = 1;
		m_nTimerTickCount = 0;
		m_loop = zloop_new();
		m_nTimer4Supervise = zloop_timer(m_loop, 10000, 0, supervise, this);//10s
		if (m_pthdLog.p == NULL) {
			pthread_create(&m_pthdLog, NULL, dealLogThread, this);
		}
		if (m_pthdNetwork.p == NULL) {
			pthread_create(&m_pthdNetwork, NULL, dealNetworkThread, this);
		}
		if (m_pthdAppMsg.p == NULL) {
			pthread_create(&m_pthdAppMsg, NULL, dealAppMsgThread, this);
		}
		if (m_pthdSupervisor.p == NULL) {
			pthread_create(&m_pthdSupervisor, NULL, superviseThread, this);
		}
		if (m_pthdTopicMsg.p == NULL) {
			pthread_create(&m_pthdTopicMsg, NULL, dealTopicMsgThread, this);
		}
		if (m_pthdInteractionMsg.p == NULL) {
			pthread_create(&m_pthdInteractionMsg, NULL, dealInteractionMsgThread, this);
		}

		unsigned long now = (unsigned long)time(NULL);
		char szDatetime[16] = { 0 };
		format_datetime(now, szDatetime, sizeof(szDatetime));
		char szMsg[256] = { 0 };
		snprintf(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\"}", MSG_SUB_SNAPSHOT, getNextRequestSequence(), szDatetime);
		sendDataViaInteractor(szMsg, strlen(szMsg));

		//提交数据到zookeeper节点
		if (strlen(pHost)) {
			strncpy_s(m_zkAccess.szHostIp, sizeof(m_zkAccess.szHostIp), pHost, strlen(pHost));
		}
		else {
			snprintf(m_zkAccess.szHostIp, sizeof(m_zkAccess.szHostIp), "127.0.0.1");
		}
		m_zkAccess.usAccessPort = usServicePort;
		competeForMaster();

		if (g_bInitBuffer == FALSE) {
			readDataBuffer();
			g_bInitBuffer = TRUE;
		}

		char szLog[256] = { 0 };
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]access service start %u\r\n", __FUNCTION__, 
			__LINE__, usServicePort);
		writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		return 0;
	}

	zsocket_destroy(m_ctx, m_subscriber);
	zsocket_destroy(m_ctx, m_interactor);
	zsocket_destroy(m_ctx, m_seeker);
	zctx_destroy(&m_ctx);

	return -1;
}

int AccessService::StopAccessService()
{
	if (m_nRun == 0) {
		return 0;
	}
	m_nRun = 0;
	char szLog[256] = { 0 };
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]access serivce stop\r\n", __FUNCTION__,
		__LINE__);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
	if (m_uiSrvInst) {
		TS_StopServer(m_uiSrvInst);
		m_uiSrvInst = 0;
	}
	if (m_pthdNetwork.p) {
		pthread_join(m_pthdNetwork, NULL);
		m_pthdNetwork.p = NULL;
	}
	if (m_pthdAppMsg.p) {
		pthread_cond_broadcast(&m_cond4AppMsgQueue);
		pthread_join(m_pthdAppMsg, NULL);
		m_pthdAppMsg.p = NULL;
	}
	if (m_pthdTopicMsg.p) {
		pthread_cond_broadcast(&m_cond4TopicMsgQueue);
		pthread_join(m_pthdTopicMsg, NULL);
		m_pthdTopicMsg.p = NULL;
	}
	if (m_pthdInteractionMsg.p) {
		pthread_cond_broadcast(&m_cond4InteractionMsgQueue);
		pthread_join(m_pthdInteractionMsg, NULL);
		m_pthdInteractionMsg.p = NULL;
	}
	if (m_pthdSupervisor.p) {
		pthread_join(m_pthdSupervisor, NULL);
		m_pthdSupervisor.p = NULL;
	}
	if (m_pthdLog.p) {
		pthread_cond_broadcast(&m_cond4LogQueue);
		pthread_join(m_pthdLog, NULL);
		m_pthdLog.p = NULL;
	}
	if (m_loop) {
		zloop_timer_end(m_loop, m_nTimer4Supervise);
		zloop_destroy(&m_loop);
	}
	if (m_ctx) {
		zsocket_destroy(m_ctx, m_subscriber);
		zsocket_destroy(m_ctx, m_interactor);
		zsocket_destroy(m_ctx, m_seeker);
		zctx_destroy(&m_ctx);
		m_subscriber = NULL;
		m_interactor = NULL;
		m_seeker = NULL;
		m_ctx = NULL;
	}

	//删除zookeeper自身节点
	if (m_bConnectZk) {
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		m_zkNodePath[0] = '\0';
		m_bConnectZk = false;
	}
	return E_OK;
}

void AccessService::SetLogType(int nLogType)
{
	if (m_nLogInst) {
		if (m_nLogType != nLogType) {
			LOG_INFO logInfo;
			LOG_GetConfig(m_nLogInst, &logInfo);
			if (logInfo.type != nLogType) {
				logInfo.type = nLogType;
				LOG_SetConfig(m_nLogInst, logInfo);
				m_nLogType = nLogType;
			}			
		}
	}
}

int AccessService::GetStatus()
{
	return m_nRun;
}

void AccessService::initLog()
{
	if (m_nLogInst == 0) {
		m_nLogInst = LOG_Init();
		if (m_nLogInst) {
			LOG_INFO logInfo;
			logInfo.type = m_nLogType;
			char szLogDir[256] = { 0 };
			snprintf(szLogDir, 256, "%slog\\", m_szLogRoot);
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strcat_s(szLogDir, 256, "escort_accesss\\");
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strncpy_s(logInfo.path, sizeof(logInfo.path), szLogDir, strlen(szLogDir));
			LOG_SetConfig(m_nLogInst, logInfo);
		}
	}
}

bool AccessService::addLog(access_service::LogContext * pLog)
{
	bool result = false;
	do {
		if (pLog) {
			if (pLog->pLogData && pLog->uiDataLen) {
				pthread_mutex_lock(&m_mutex4LogQueue);
				m_logQueue.push(pLog);
				if (m_logQueue.size() == 1) {
					pthread_cond_signal(&m_cond4LogQueue);
				}
				pthread_mutex_unlock(&m_mutex4LogQueue);
				result = true;
			}
		}
	} while (0);
	return result;
}

void AccessService::writeLog(const char * pLogContent, int nLogCategoryType, int nLogType)
{
	if (pLogContent && strlen(pLogContent)) {
		access_service::LogContext * pLog = new access_service::LogContext();
		pLog->nLogCategory = nLogCategoryType;
		pLog->nLogType = nLogType;
		size_t nSize = strlen(pLogContent);
		pLog->uiDataLen = (unsigned int)nSize;
		pLog->pLogData = (char *)malloc(nSize + 1);
		if (pLog->pLogData) {
			memcpy_s(pLog->pLogData, nSize, pLogContent, nSize);
			pLog->pLogData[nSize] = '\0';
		}
		if (!addLog(pLog)) {
			free(pLog->pLogData);
			pLog->pLogData = NULL;
			delete pLog;
			pLog = NULL;
		}
	}
}

void AccessService::dealLog()
{
	do {
		pthread_mutex_lock(&m_mutex4LogQueue);
		while (m_nRun && m_logQueue.empty()) {
			pthread_cond_wait(&m_cond4LogQueue, &m_mutex4LogQueue);
		}
		if (!m_nRun && m_logQueue.empty()) {
			pthread_mutex_unlock(&m_mutex4LogQueue);
			break;
		}
		access_service::LogContext * pLog = m_logQueue.front();
		m_logQueue.pop();
		pthread_mutex_unlock(&m_mutex4LogQueue);
		if (pLog) {
			if (pLog->pLogData) {
				if (m_nLogInst) {
					LOG_Log(m_nLogInst, pLog->pLogData, pLog->nLogCategory, pLog->nLogType);
				}
				free(pLog->pLogData);
				pLog->pLogData = NULL;
			}
			delete pLog;
			pLog = NULL;
		}
	} while (1);
}

bool AccessService::addAppMsg(MessageContent * pMsg)
{
	bool result = false;
	do {
		if (pMsg && pMsg->pMsgData && strlen(pMsg->szEndPoint)) {
			pthread_mutex_lock(&m_mutex4AppMsgQueue);
			m_appMsgQueue.push(pMsg);
			if (m_appMsgQueue.size() == 1) {
				pthread_cond_broadcast(&m_cond4AppMsgQueue);
			}
			pthread_mutex_unlock(&m_mutex4AppMsgQueue);
			result = true;
		}
	} while (0);
	return result;
}

void AccessService::dealAppMsg()
{
	do {
		pthread_mutex_lock(&m_mutex4AppMsgQueue);
		while (m_nRun && m_appMsgQueue.empty()) {
			pthread_cond_wait(&m_cond4AppMsgQueue, &m_mutex4AppMsgQueue);
		}
		if (!m_nRun && m_appMsgQueue.empty()) {
			pthread_mutex_unlock(&m_mutex4AppMsgQueue);
			break;
		}
		MessageContent * pMsg = m_appMsgQueue.front();
		m_appMsgQueue.pop();
		pthread_mutex_unlock(&m_mutex4AppMsgQueue);
		if (pMsg) {		
			if (pMsg->pMsgData && pMsg->ulMsgDataLen) {
				parseAppMsg(pMsg);
				delete [] pMsg->pMsgData;
				pMsg->pMsgData = NULL;
				pMsg->ulMsgDataLen = 0;
			}
			delete pMsg;
			pMsg = NULL;
		}
	} while (1);
}

void AccessService::parseAppMsg(MessageContent * pMsg)
{
	char szLog[512] = { 0 };
	if (pMsg) {
		std::string strEndPoint = pMsg->szEndPoint;
		//check weather exists data linger
		unsigned char * pBuf = NULL;
		unsigned long ulBufLen = 0;
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strEndPoint);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData->ulLackDataLen == 0) {
				pBuf = (unsigned char *)malloc(pMsg->ulMsgDataLen + 1);
				ulBufLen = pMsg->ulMsgDataLen;
				memcpy_s(pBuf, ulBufLen, pMsg->pMsgData, ulBufLen);
				pBuf[ulBufLen] = '0';
			}
			else {
				if (pLinkData->ulLackDataLen <= pMsg->ulMsgDataLen) { //full 
					ulBufLen = pLinkData->ulLingeDataLen + pMsg->ulMsgDataLen;
					pBuf = (unsigned char *)malloc(ulBufLen + 1);
					memcpy_s(pBuf, ulBufLen, pLinkData->pLingeData, pLinkData->ulLingeDataLen);
					memcpy_s(pBuf + pLinkData->ulLingeDataLen, ulBufLen - pLinkData->ulLingeDataLen, pMsg->pMsgData, 
						pMsg->ulMsgDataLen);
					pBuf[ulBufLen] = '\0';
					pLinkData->ulLackDataLen = 0;
					pLinkData->ulLingeDataLen = 0;
					free(pLinkData->pLingeData);
					pLinkData->pLingeData = NULL;
				}
				else if (pLinkData->ulLackDataLen > pMsg->ulMsgDataLen) { //still lack 
					memcpy_s(pLinkData->pLingeData + pLinkData->ulLingeDataLen, pLinkData->ulLackDataLen, pMsg->pMsgData, 
						pMsg->ulMsgDataLen);
					pLinkData->ulLingeDataLen += pMsg->ulMsgDataLen;
					pLinkData->ulLackDataLen -= pMsg->ulMsgDataLen;
				}
			}
		}
		else {
			printf("...oppppppps, are U kiding me? link[%s] not found\n", strEndPoint.c_str());
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]link(%s) not found\r\n", __FUNCTION__, __LINE__,
				strEndPoint.c_str());
			writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (pBuf && ulBufLen) {
			unsigned long ulIndex = 0;
			unsigned long ulBeginIndex = 0;
			unsigned long ulEndIndex = 0;
			unsigned int uiUnitLen = 0;
			do {
				int n = getWholeMessage(pBuf, ulBufLen, ulIndex, ulBeginIndex, ulEndIndex, uiUnitLen);
				if (n == 0) {
					break;
				}
				else if (n == 1) {
					pthread_mutex_lock(&m_mutex4LinkDataList);
					iter = m_linkDataList.find(strEndPoint);
					if (iter != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData = iter->second;
						if (pLinkData) {
							pLinkData->ulTotalDataLen = (unsigned long)uiUnitLen;
							pLinkData->ulLingeDataLen = ulBufLen - ulBeginIndex;
							pLinkData->ulLackDataLen = pLinkData->ulTotalDataLen - pLinkData->ulLingeDataLen;
							pLinkData->pLingeData = (unsigned char *)malloc(pLinkData->ulTotalDataLen);
							memcpy_s(pLinkData->pLingeData, pLinkData->ulTotalDataLen, pBuf + ulBeginIndex, 
								pLinkData->ulLingeDataLen);
						}
					}
					pthread_mutex_unlock(&m_mutex4LinkDataList);
					break;
				}
				else if (n == 2) {
					ulIndex = ulEndIndex;
					decryptMessage(pBuf, ulBeginIndex, ulEndIndex);
					char * pContent = (char *)malloc(uiUnitLen + 1);
					memcpy_s(pContent, uiUnitLen, pBuf + ulBeginIndex, uiUnitLen);
					pContent[uiUnitLen] = '\0';
					rapidjson::Document doc;
					if (!doc.Parse(pContent).HasParseError()) {
						access_service::eAppCommnad nCmd = access_service::E_CMD_UNDEFINE;
						if (doc.HasMember("cmd")) {
							if (doc["cmd"].IsInt()) {
								nCmd = (access_service::eAppCommnad)(doc["cmd"].GetInt());
							}
						}
						switch (nCmd) {
							case access_service::E_CMD_LOGIN: {
								if (doc.HasMember("account") && doc.HasMember("passwd") && doc.HasMember("datetime")) {
									access_service::AppLoginInfo loginInfo;
									bool bValidAccount = false;
									bool bValidPasswd = false;
									bool bValidDatetime = false;
									if (doc["account"].IsString()) {
										size_t nSize = doc["account"].GetStringLength();
										const char * pAccount = doc["account"].GetString();
										if (pAccount && nSize) {
											strncpy_s(loginInfo.szUser, sizeof(loginInfo.szUser), pAccount, nSize);
											bValidAccount = true;
										}
									}
									if (doc["passwd"].IsString()) {
										size_t nSize = doc["passwd"].GetStringLength();
										const char * pPwd = doc["passwd"].GetString();
										if (pPwd && nSize) {
											strncpy_s(loginInfo.szPasswd, sizeof(loginInfo.szPasswd), pPwd, nSize);
											bValidPasswd = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDateTime = doc["datetime"].GetString();
										if (pDateTime && nSize) {
											strncpy_s(loginInfo.szDateTime, sizeof(loginInfo.szDateTime), pDateTime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidAccount && bValidPasswd && bValidDatetime) {
										loginInfo.uiReqSeq = 0;	//loginInfo.uiReqSeq = getNextRequestSequence();
										handleAppLogin(loginInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, one or more"
											" parameter needed, account=%s, passwd=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidAccount ? loginInfo.szUser : "null",
											bValidPasswd ? loginInfo.szPasswd : "null", bValidDatetime ? loginInfo.szDateTime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_LOGOUT: {
								if (doc.HasMember("session") && doc.HasMember("datetime")) {
									access_service::AppLogoutInfo logoutInfo;
									bool bValidSession = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(logoutInfo.szSession, sizeof(logoutInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(logoutInfo.szDateTime, sizeof(logoutInfo.szDateTime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidDatetime) {
										logoutInfo.uiReqSeq = 0;	//logoutInfo.uiReqSeq = getNextRequestSequence();
										handleAppLogout(logoutInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, one or more "
											"parameter needed, session=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint,
											bValidSession ? logoutInfo.szSession : "null", bValidDatetime ? logoutInfo.szDateTime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_BIND_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("deviceId") && doc.HasMember("datetime")) {
									access_service::AppBindInfo bindInfo;
									bindInfo.szFactoryId[0] = '\0';
									bool bValidSession = false;
									bool bValidDevice = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										const char * pDeviceId = doc["deviceId"].GetString();
										if (pDeviceId && nSize) {
											strncpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), pDeviceId, nSize);
											bValidDevice = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidDevice && bValidDatetime) {
										bindInfo.nMode = 0;
										bindInfo.uiReqSeq = getNextRequestSequence();
										handleAppBind(bindInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, one or more "
											"parameter needed, session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? bindInfo.szSesssion : "null",
											bValidDevice ? bindInfo.szDeviceId : "null", bValidDatetime ? bindInfo.szDateTime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_UNBIND_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("deviceId") && doc.HasMember("datetime")) {
									access_service::AppBindInfo bindInfo;
									bindInfo.szFactoryId[0] = '\0';
									bool bValidSession = false;
									bool bValidDevice = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										const char * pDeviceId = doc["deviceId"].GetString();
										if (pDeviceId && nSize) {
											strncpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), pDeviceId, nSize);
											bValidDevice = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidDevice && bValidDatetime) {
										bindInfo.nMode = 1;
										bindInfo.uiReqSeq = getNextRequestSequence();
										handleAppBind(bindInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, one or more "
											"parameter needed, session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? bindInfo.szSesssion : "null",
											bValidDevice ? bindInfo.szDeviceId : "null", bValidDatetime ? bindInfo.szDateTime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_TASK: {
								if (doc.HasMember("session") && doc.HasMember("type") && doc.HasMember("destination")
									&& doc.HasMember("target") && doc.HasMember("limit") && doc.HasMember("datetime")) {
									access_service::AppSubmitTaskInfo taskInfo;
									taskInfo.szTarget[0] = '\0';
									taskInfo.szDestination[0] = '\0';
									bool bValidSession = false;
									bool bValidType = false;
									bool bValidLimit = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["type"].IsInt()) {
										taskInfo.usTaskType = (unsigned short)doc["type"].GetInt();
										bValidType = true;
									}
									if (doc["limit"].IsInt()) {
										taskInfo.usTaskLimit = (unsigned short)doc["limit"].GetInt();
										bValidLimit = true;
									}
									if (doc["destination"].GetString()) {
										size_t nSize = doc["destination"].GetStringLength();
										const char * pDestination = doc["destination"].GetString();
										if (pDestination && nSize) {
											strncpy_s(taskInfo.szDestination, sizeof(taskInfo.szDestination), pDestination, nSize);
										}
									}
									if (doc["target"].GetString()) {
										size_t nSize = doc["target"].GetStringLength();
										const char * pTarget = doc["target"].GetString();
										if (pTarget && nSize) {
											strncpy_s(taskInfo.szTarget, sizeof(taskInfo.szTarget), pTarget, nSize);
										}
									}
									if (doc["datetime"].GetString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidType && bValidLimit && bValidDatetime) {
										taskInfo.uiReqSeq = getNextRequestSequence();
										handleAppSubmitTask(taskInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, one or more"
											" parameter needed, session=%s, type=%d, limit=%d, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? taskInfo.szSession : "null",
											bValidType ? taskInfo.usTaskType : -1, bValidLimit ? taskInfo.usTaskLimit : -1,
											bValidDatetime ? taskInfo.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_TASK_CLOSE: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("closeType")
									&& doc.HasMember("datetime")) {
									access_service::AppCloseTaskInfo taskInfo;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidCloseType = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										const char * pTaskId = doc["taskId"].GetString();
										if (pTaskId && nSize) {
											strncpy_s(taskInfo.szTaskId, sizeof(taskInfo.szTaskId), pTaskId, nSize);
											bValidTask = true;
										}
									}
									if (doc["closeType"].IsInt()) {
										taskInfo.nCloseType = doc["closeType"].GetInt();
										bValidCloseType = true;
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidCloseType && bValidDatetime) {
										taskInfo.uiReqSeq = getNextRequestSequence();
										handleAppCloseTask(taskInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, one or "
											"more parameter needed, session=%s, taskId=%s, closeType=%d, datetime=%s\r\n", __FUNCTION__,
											__LINE__, pMsg->szEndPoint, bValidSession ? taskInfo.szSession : "null",
											bValidTask ? taskInfo.szTaskId : "null", bValidCloseType ? taskInfo.nCloseType : -1,
											bValidDatetime ? taskInfo.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_POSITION_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("lat")
									&& doc.HasMember("lng") && doc.HasMember("datetime")) {
									access_service::AppPositionInfo posInfo;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidLat = false;
									bool bValidLng = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(posInfo.szSession, sizeof(posInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										const char * pTaskId = doc["taskId"].GetString();
										if (pTaskId && nSize) {
											strncpy_s(posInfo.szTaskId, sizeof(posInfo.szTaskId), pTaskId, nSize);
											bValidTask = true;
										}
									}
									if (doc["lat"].IsDouble()) {
										posInfo.dLat = doc["lat"].GetDouble();
										bValidLat = true;
									}
									if (doc["lng"].IsDouble()) {
										posInfo.dLng = doc["lng"].GetDouble();
										bValidLng = true;
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(posInfo.szDatetime, sizeof(posInfo.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidLat && bValidLng && bValidDatetime) {
										posInfo.uiReqSeq = getNextRequestSequence();
										handleAppPosition(posInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, one or more"
											" parameter needed, session=%s, taskId=%s, lat=%f, lng=%f, datetime=%s\r\n", __FUNCTION__,
											__LINE__, pMsg->szEndPoint, bValidSession ? posInfo.szSession : "null",
											bValidTask ? posInfo.szTaskId : "null", bValidLat ? posInfo.dLat : 0.0000,
											bValidLng ? posInfo.dLng : 0.0000, bValidDatetime ? posInfo.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_FLEE_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("datetime")) {
									access_service::AppSubmitFleeInfo fleeInfo;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(fleeInfo.szSession, sizeof(fleeInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										const char * pTaskId = doc["taskId"].GetString();
										if (pTaskId && nSize) {
											strncpy_s(fleeInfo.szTaskId, sizeof(fleeInfo.szTaskId), pTaskId, nSize);
											bValidTask = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(fleeInfo.szDatetime, sizeof(fleeInfo.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidDatetime) {
										fleeInfo.nMode = 0;
										fleeInfo.uiReqSeq = getNextRequestSequence();
										handleAppFlee(fleeInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, one or more "
											"parameter needed, session=%s, taskId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? fleeInfo.szSession : "null",
											bValidTask ? fleeInfo.szTaskId : "null", bValidDatetime ? fleeInfo.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_FLEE_REVOKE_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("datetime")) {
									access_service::AppSubmitFleeInfo fleeInfo;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(fleeInfo.szSession, sizeof(fleeInfo.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										const char * pTaskId = doc["taskId"].GetString();
										if (pTaskId && nSize) {
											strncpy_s(fleeInfo.szTaskId, sizeof(fleeInfo.szTaskId), pTaskId, nSize);
											bValidTask = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(fleeInfo.szDatetime, sizeof(fleeInfo.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidDatetime) {
										fleeInfo.nMode = 1;
										fleeInfo.uiReqSeq = getNextRequestSequence();
										handleAppFlee(fleeInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s, one or "
											"more parameter needed, session=%s, taskId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? fleeInfo.szSession : "null",
											bValidTask ? fleeInfo.szTaskId : "null", bValidDatetime ? fleeInfo.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s, JSON data"
										" format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case access_service::E_CMD_KEEPALIVE: {
								if (doc.HasMember("session") && doc.HasMember("seq") && doc.HasMember("datetime")) {
									access_service::AppKeepAlive keepAlive;
									bool bValidSession = false;
									bool bValidSeq = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										const char * pSession = doc["session"].GetString();
										if (pSession && nSize) {
											strncpy_s(keepAlive.szSession, sizeof(keepAlive.szSession), pSession, nSize);
											bValidSession = true;
										}
									}
									if (doc["seq"].IsInt()) {
										keepAlive.uiSeq = (unsigned int)doc["seq"].GetInt();
										bValidSeq = true;
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										const char * pDatetime = doc["datetime"].GetString();
										if (pDatetime && nSize) {
											strncpy_s(keepAlive.szDatetime, sizeof(keepAlive.szDatetime), pDatetime, nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidSeq && bValidDatetime) {
										handleAppKeepAlive(keepAlive, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, one or more "
											"parameter needed, session=%s, seq=%d, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? keepAlive.szSession : "null",
											bValidSeq ? keepAlive.uiSeq : -1, bValidDatetime ? keepAlive.szDatetime : "null");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, JSON data format "
										"error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]can't recognise command: %d\r\n", __FUNCTION__,
									__LINE__, nCmd);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]can't parse JSON content: %s\r\n", __FUNCTION__, 
							__LINE__, pContent);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					free(pContent);
					pContent = NULL;
				}
			} while (1);
			free(pBuf);
			pBuf = NULL;
			ulBufLen = 0;
		}
	}
}

int AccessService::getWholeMessage(const unsigned char * pData, unsigned long ulDataLen, 
	unsigned long ulIndex, unsigned long & ulBeginIndex, unsigned long & ulEndIndex, 
	unsigned int & uiUnitLen)
{
	int result = 0;
	unsigned long i = ulIndex;
	size_t nHeadSize = sizeof(access_service::AppMessageHead);
	access_service::AppMessageHead msgHead;
	bool bFindValidHead = false;
	do {
		if (i >= ulDataLen) {
			break;
		}
		if (!bFindValidHead) {
			if (ulDataLen - i < nHeadSize) {
				break;
			}
			memcpy_s(&msgHead, nHeadSize, pData + i, nHeadSize);
			if (msgHead.marker[0] == 'E' && msgHead.marker[1] == 'C' && msgHead.version[0] == '1') {
				bFindValidHead = true;
				result = 1;
				i += nHeadSize;
				ulBeginIndex = i;
				uiUnitLen = msgHead.uiDataLen;
			}
			else {
				i++;
			}
		}
		else {
			if (i + msgHead.uiDataLen <= ulDataLen) {
				ulBeginIndex = i;
				ulEndIndex = i + msgHead.uiDataLen;
				result = 2;
			}
			break;
		}
	} while (1);
	return result;
}

void AccessService::decryptMessage(unsigned char * pData, unsigned long ulBeginIndex, 
	unsigned long ulEndIndex)
{
	if (ulEndIndex > ulBeginIndex && ulBeginIndex >= 0) {
		for (unsigned long i = ulBeginIndex; i < ulEndIndex; i++) {
			pData[i] ^= gSecret;
			pData[i] -= 1;
		}
	}
}

void AccessService::encryptMessage(unsigned char * pData, unsigned long ulBeginIndex, 
	unsigned long ulEndIndex)
{
	if (ulEndIndex > ulBeginIndex && ulBeginIndex >= 0) {
		for (unsigned long i = ulBeginIndex; i < ulEndIndex; i++) {
			pData[i] += 1;
			pData[i] ^= gSecret;
		}
	}
}

//action: login 
//condition:  c1-check guarder(user) wether "login" already | consider the matter <guarder(user) re-login>, how to handle 
//							 check passwd			
//
void AccessService::handleAppLogin(access_service::AppLoginInfo loginInfo, const char * pEndPoint, 
	unsigned long ulTime)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	//1.query guarderList for information check
	bool bFindGuarder = false;	
	bool bHaveTask = false;
	char szSession[20] = { 0 };
	char szCurrTaskId[12] = { 0 };
	char szReply[512] = { 0 };
	char szTaskInfo[256] = { 0 };
	EscortTask * pCurrTask = NULL;
	unsigned short usDeviceStatus = 0;
	unsigned short usDeviceBattery = 0;
	bool bReLogin = false;
	//1-1.check from g_guarderList, 1-2.check from db-prox
	pthread_mutex_lock(&g_mutex4GuarderList);
	if (zhash_size(g_guarderList)) {
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo.szUser);
		if (pGuarder) {
			bFindGuarder = true;
			if (strlen(pGuarder->szCurrentSession) > 0) { //session exists, account in use
				if (strlen(pGuarder->szLink) > 0) {
					nErr = E_ACCOUNTINUSE;
				}
				else {
					bReLogin = true;
					strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession, 
						strlen(pGuarder->szCurrentSession));
					if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
						strncpy_s(szCurrTaskId, sizeof(szCurrTaskId), pGuarder->szTaskId, 
							strlen(pGuarder->szTaskId));
						bHaveTask = true;
					}
				}
			}
			else {
				if (pGuarder->uiState == STATE_GUARDER_DEACTIVATE) {
					nErr = E_INVALIDACCOUNT;
				}
				else {
					//check passwd
					if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
						if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
							strncpy_s(szCurrTaskId, sizeof(szCurrTaskId), pGuarder->szTaskId, 
								strlen(pGuarder->szTaskId));
							bHaveTask = true;
						}
						if (!bReLogin) {
							generateSession(szSession, sizeof(szSession));
						}
						size_t nSize = strlen(szSession);
						if (nSize) {
							strncpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), 
								szSession, nSize);
						}
						strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
					}
					else { //passwd error
						nErr = E_INVALIDPASSWD;
					}
				}
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4GuarderList);
	//1-2.
	if (!bFindGuarder) {
		//query from db-buffer,real kill time
		size_t nContainerSize = sizeof(escort_db::SqlContainer);
		do {
			nErr = E_INVALIDACCOUNT;
			escort_db::SqlContainer reqContainer;
			reqContainer.uiSqlOptSeq = getNextRequestSequence();
			reqContainer.ulSqlOptTime = (unsigned long)time(NULL);
			reqContainer.usSqlOptTarget = escort_db::E_TBL_GUARDER;
			reqContainer.usSqlOptType = escort_db::E_OPT_QUERY;
			reqContainer.uiResultCount = 0;
			reqContainer.uiResultLen = 0;
			reqContainer.pStoreResult = NULL;
			strncpy_s(reqContainer.szSqlOptKey, sizeof(reqContainer.szSqlOptKey), loginInfo.szUser,
				strlen(loginInfo.szUser));
			zframe_t * frame_req = zframe_new(&reqContainer, nContainerSize);
			zmsg_t * msg_req = zmsg_new();
			zmsg_append(msg_req, &frame_req);
			zmsg_send(&msg_req, m_seeker);
			zmsg_t * msg_rep = zmsg_recv(m_seeker);
			if (!msg_rep) {
				break;
			}
			zframe_t * frame_rep = zmsg_pop(msg_rep);
			if (!frame_rep) {
				zmsg_destroy(&msg_rep);
				break;
			}
			size_t nFrameDataLen = zframe_size(frame_rep);
			unsigned char * pFrameData = zframe_data(frame_rep);
			if (pFrameData && nFrameDataLen && nFrameDataLen >= nContainerSize) {
				escort_db::SqlContainer repContainer;
				memcpy_s(&repContainer, nContainerSize, pFrameData, nContainerSize);
				if (repContainer.uiResultCount == 1 && repContainer.uiResultLen) {
					repContainer.pStoreResult = (unsigned char *)zmalloc(repContainer.uiResultLen + 1);
					memcpy_s(repContainer.pStoreResult, repContainer.uiResultLen + 1,
						pFrameData + nContainerSize, repContainer.uiResultLen);
					repContainer.pStoreResult[repContainer.uiResultLen] = '\0';
				}
				if (repContainer.uiSqlOptSeq == reqContainer.uiSqlOptSeq
					&& repContainer.ulSqlOptTime == reqContainer.ulSqlOptTime
					&& repContainer.usSqlOptTarget == reqContainer.usSqlOptTarget
					&& repContainer.usSqlOptType == reqContainer.usSqlOptType) {
					size_t nGuarderSize = sizeof(Guarder);
					Guarder * pGuarder = (Guarder *)zmalloc(nGuarderSize);
					memcpy_s(pGuarder, nGuarderSize, reqContainer.pStoreResult, nGuarderSize);
					if (strcmp(pGuarder->szId, loginInfo.szUser) == 0) {
						if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							pGuarder->uiState = STATE_GUARDER_FREE;
							generateSession(szSession, sizeof(szSession));
							if (strlen(szSession)) {
								strncpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession),
									szSession, strlen(szSession));
							}
							strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
							nErr = E_OK;
						}
						else {
							nErr = E_INVALIDPASSWD;
						}
						pthread_mutex_lock(&g_mutex4GuarderList);
						zhash_update(g_guarderList, pGuarder->szId, pGuarder);
						zhash_freefn(g_guarderList, pGuarder->szId, free);
						pthread_mutex_unlock(&g_mutex4GuarderList);
					}
					free(pGuarder);
					pGuarder = NULL;
				}
			}
			zframe_destroy(&frame_rep);
			zmsg_destroy(&msg_rep);
		} while (0);
	}
	//2.give reply for login request
	if (bHaveTask) {
		//2-1.get task information
		char szDevKey[20] = { 0 };
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrTaskId);
			if (pTask) {		
				//snprintf(szDevKey, sizeof(szDevKey), "%s_%s", pTask->szFactoryId, pTask->szDeviceId);
				strncpy_s(szDevKey, sizeof(szDevKey), pTask->szDeviceId, strlen(pTask->szDeviceId));
				size_t nTaskSize = sizeof(EscortTask);
				pCurrTask = (EscortTask *)zmalloc(nTaskSize);
				memcpy_s(pCurrTask, nTaskSize, pTask, nTaskSize);
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
		//2-2.get device information
		if (strlen(szDevKey)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDevKey);
				if (pDev) {
					usDeviceStatus = pDev->deviceBasic.nStatus;
					usDeviceBattery = pDev->deviceBasic.nBattery;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
	}		
	if (nErr != E_OK) {//login failed
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, nErr);
	}
	else {
		if (!bHaveTask) {	//login ok, without linger task
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[]}",
				access_service::E_CMD_LOGIN_REPLY, szSession);
		}
		else {	//login ok, with linger task
			snprintf(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,"
				"\"limit\":%d,\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%u,"
				"\"deviceState\":%u}", pCurrTask->szTaskId, pCurrTask->szDeviceId, pCurrTask->nTaskType,
				pCurrTask->nTaskLimitDistance, pCurrTask->szDestination, pCurrTask->szTarget, 
				pCurrTask->szTaskStartTime, usDeviceBattery, usDeviceStatus);
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}",
				access_service::E_CMD_LOGIN_REPLY, szSession, szTaskInfo);
		}
	}
	sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
	//3.update information
	if (nErr == E_OK) {
		size_t nSize = sizeof(access_service::AppLinkInfo);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zmalloc(nSize);
		strncpy_s(pLink->szGuarder, sizeof(pLink->szGuarder), loginInfo.szUser, strlen(loginInfo.szUser));
		strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndPoint, strlen(pEndPoint));
		strncpy_s(pLink->szSession, sizeof(pLink->szSession), szSession, strlen(szSession));
		if (bHaveTask && pCurrTask) {
			strncpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), pCurrTask->szDeviceId, 
				strlen(pCurrTask->szDeviceId));
			strncpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), pCurrTask->szFactoryId,
				strlen(pCurrTask->szFactoryId));
			strncpy_s(pLink->szOrg, sizeof(pLink->szOrg), pCurrTask->szOrg, strlen(pCurrTask->szOrg));
			strncpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), pCurrTask->szTaskId, 
				strlen(pCurrTask->szTaskId));
		}
		pLink->nActivated = 1;
		pLink->ulActivateTime = ulTime;
		pthread_mutex_lock(&m_mutex4LinkList);
		zhash_update(m_linkList, szSession, pLink);
		zhash_freefn(m_linkList, szSession, free);
		pthread_mutex_unlock(&m_mutex4LinkList);

		pthread_mutex_lock(&m_mutex4LinkDataList); //update linkdatalist, add Guarder to handle disconnect
		std::string strLink = pEndPoint;
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), loginInfo.szUser, strlen(loginInfo.szUser));
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	if (pCurrTask) {
		char szTopic[64] = { 0 };
		snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", pCurrTask->szOrg, pCurrTask->szFactoryId, 
			pCurrTask->szDeviceId);
		access_service::AppSubscirbeInfo * pSubInfo;
		pSubInfo = (access_service::AppSubscirbeInfo *)zmalloc(sizeof(access_service::AppSubscirbeInfo));
		strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndPoint, strlen(pEndPoint));
		strncpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pCurrTask->szGuarder, 
			strlen(pCurrTask->szGuarder));
		strncpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), szSession, strlen(szSession));
		strncpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic, strlen(szTopic));
		pthread_mutex_lock(&m_mutex4SubscribeList);
		zhash_update(m_subscribeList, szTopic, pSubInfo);
		zhash_freefn(m_subscribeList, szTopic, free);
		pthread_mutex_unlock(&m_mutex4SubscribeList);
		free(pCurrTask);
		pCurrTask = NULL;
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, datetime=%s, result=%d, "
		"session=%s\r\n", __FUNCTION__, __LINE__, loginInfo.szUser, loginInfo.szDateTime, nErr, szSession);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
	
}

void AccessService::handleAppLogout(access_service::AppLogoutInfo logoutInfo, const char * pEndPoint, 
	unsigned long ulTime)
{
	char szLog[256] = { 0 };
	int nErr = E_OK;
	bool bFindLink = false;
	char szReply[256] = { 0 };
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	//logoutInfo.szSession
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			logoutInfo.szSession);
		if (pLink) {
			pLink->ulActivateTime = ulTime;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			bFindLink = true;
		} 
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (!bFindLink) {
		nErr = E_INVALIDSESSION;	
	}
	else {
		pthread_mutex_lock(&g_mutex4GuarderList);
		if (zhash_size(g_guarderList)) {
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strlen(pGuarder->szTaskId) && pGuarder->uiState == STATE_GUARDER_DUTY) {
					nErr = E_GUARDERINDUTY;
				}
				else {
					if (strlen(pGuarder->szBindDevice) && pGuarder->uiState == STATE_GUARDER_BIND) {
						pGuarder->uiState = STATE_GUARDER_FREE;				
					}
					if (strlen(pGuarder->szBindDevice)) {
						strncpy_s(szDevKey, sizeof(szDevKey), pGuarder->szBindDevice, 
							strlen(pGuarder->szBindDevice));
					}
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->szCurrentSession[0] = '\0';
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}", 
		access_service::E_CMD_LOGOUT_REPLY, nErr, logoutInfo.szSession);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
	if (nErr == E_OK) {
		if (strlen(szDevKey)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (g_deviceList) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDevKey);
				if (pDev) {
					pDev->szBindGuard[0] = '\0';
					pDev->ulBindTime = 0;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		//delete session link
		pthread_mutex_lock(&m_mutex4LinkList);
		zhash_delete(m_linkList, logoutInfo.szSession);
		pthread_mutex_unlock(&m_mutex4LinkList);
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]logout session=%s, datetime=%s, user=%s, "
		"result=%d\r\n", __FUNCTION__, __LINE__, logoutInfo.szSession, logoutInfo.szDateTime, szGuarder,
		nErr);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppBind(access_service::AppBindInfo bindInfo, const char * pEndPoint, 
	unsigned long ulTime)
{
	char szLog[256] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	bool bFindLink = false;
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szFactory[4] = { 0 };
	char szOrg[40] = { 0 };
	unsigned short usBattery = 0;
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			bindInfo.szSesssion);
		if (pLink) {
			bFindLink = true;
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (!bFindLink) {
		nErr = E_INVALIDSESSION;
	}
	else {
		bool bValidGuarder = false;
		do {
			if (bindInfo.nMode == 0) { //execute bind
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (pGuarder->uiState == STATE_GUARDER_BIND && strlen(pGuarder->szBindDevice) > 0) {
						if (strcmp(pGuarder->szBindDevice, bindInfo.szDeviceId) != 0) {
							nErr = E_GUARDERBINDOTHERDEVICE;
						}
					}
					else if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
						nErr = E_GUARDERINDUTY;
					}
					else if (pGuarder->uiState == STATE_GUARDER_FREE) {
						bValidGuarder = true;
					}
					else {
						nErr = E_INVALIDACCOUNT;
					}
					strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
				}
				else {
					nErr = E_INVALIDACCOUNT;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (!bValidGuarder) {
					break;
				}
				bool bFindDevice = false;
				pthread_mutex_lock(&g_mutex4DevList);
				if (zhash_size(g_deviceList)) {
					WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo.szDeviceId);
					if (pDev) {
						bFindDevice = true;
						if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
							|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							nErr = E_ALREADYBINDDEVICE;
						}
						else if ((pDev->deviceBasic.nStatus & DEV_ONLINE) == DEV_ONLINE) {
							if (strlen(pDev->szBindGuard) > 0) {
								if (strcmp(pDev->szBindGuard, szGuarder) != 0) {
									nErr = E_ALREADYBINDDEVICE;
								}
							}
							else { //ok, update device here
								strncpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), szGuarder, strlen(szGuarder));
								pDev->ulBindTime = ulTime;
							}
						}
						else if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
							nErr = E_UNABLEWORKDEVICE;
						}
						usBattery = pDev->deviceBasic.nBattery;
						strncpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId,
							strlen(pDev->deviceBasic.szFactoryId));
						if (!strlen(pDev->szOrganization) && strlen(szOrg)) {
							strncpy_s(pDev->szOrganization, sizeof(pDev->szOrganization), szOrg, strlen(szOrg));
						}
					}			
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				if (!bFindDevice) {				
					nErr = E_INVALIDDEVICE;
					size_t nContainerSize = sizeof(escort_db::SqlContainer);
					do {
						escort_db::SqlContainer reqContainer;
						reqContainer.pStoreResult = NULL;
						reqContainer.uiResultCount = 0;
						reqContainer.uiResultLen = 0;
						reqContainer.uiSqlOptSeq = getNextRequestSequence();
						reqContainer.ulSqlOptTime = (unsigned long)time(NULL);
						reqContainer.usSqlOptTarget = escort_db::E_TBL_DEVICE;
						reqContainer.usSqlOptType = escort_db::E_OPT_QUERY;
						strncpy_s(reqContainer.szSqlOptKey, sizeof(reqContainer.szSqlOptKey), bindInfo.szDeviceId,
							strlen(bindInfo.szDeviceId));
						zframe_t * frame_req = zframe_new(&reqContainer, nContainerSize);
						zmsg_t * msg_req = zmsg_new();
						zmsg_append(msg_req, &frame_req);
						zmsg_send(&msg_req, m_seeker);
						zmsg_t * msg_rep = zmsg_recv(m_seeker);
						if (!msg_rep) {
							break;
						}
						zframe_t * frame_rep = zmsg_pop(msg_rep);
						if (!frame_rep) {
							zmsg_destroy(&msg_rep);
							break;
						}
						size_t nFrameDataLen = zframe_size(frame_rep);
						unsigned char * pFrameData = zframe_data(frame_rep);
						if (pFrameData && nFrameDataLen && nFrameDataLen > nContainerSize) {
							escort_db::SqlContainer repContainer;
							memcpy_s(&repContainer, nContainerSize, pFrameData, nContainerSize);
							if (repContainer.uiResultCount && repContainer.uiResultLen) {
								repContainer.pStoreResult = (unsigned char *)zmalloc(repContainer.uiResultLen + 1);
								memcpy_s(repContainer.pStoreResult, repContainer.uiResultLen + 1,
									pFrameData + nContainerSize, repContainer.uiResultLen);
								repContainer.pStoreResult[repContainer.uiResultLen] = '\0';
							}
							if (repContainer.uiSqlOptSeq == reqContainer.uiSqlOptSeq
								&& repContainer.ulSqlOptTime == reqContainer.ulSqlOptTime
								&& repContainer.usSqlOptTarget == reqContainer.usSqlOptTarget
								&& repContainer.usSqlOptType == reqContainer.usSqlOptType
								&& repContainer.uiResultCount == 1) {
								size_t nDevSize = sizeof(WristletDevice);
								WristletDevice * pDev = (WristletDevice *)zmalloc(nDevSize);
								memcpy_s(pDev, nDevSize, repContainer.pStoreResult, nDevSize);
								if (strcmp(pDev->deviceBasic.szDeviceId, bindInfo.szDeviceId) == 0) {
									pDev->ulBindTime = ulTime;
									strncpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), szGuarder, strlen(szGuarder));
									changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
									usBattery = pDev->deviceBasic.nBattery;
									strncpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId,
										strlen(pDev->deviceBasic.szFactoryId));
									if (!strlen(pDev->szOrganization) && strlen(szOrg)) {
										strncpy_s(pDev->szOrganization, sizeof(pDev->szOrganization), szOrg, strlen(szOrg));
									}
									pthread_mutex_lock(&g_mutex4DevList);
									zhash_update(g_deviceList, bindInfo.szDeviceId, pDev);
									zhash_freefn(g_deviceList, bindInfo.szDeviceId, free);
									pthread_mutex_unlock(&g_mutex4DevList);
									nErr = E_OK;
								}
								else {
									free(pDev);
									pDev = NULL;
								}
							}
						}
						zframe_destroy(&frame_rep);
						zmsg_destroy(&msg_rep);
					} while (0);					
				}		
			}
			else {//execute unbind
				pthread_mutex_lock(&g_mutex4GuarderList);
				if (zhash_size(g_guarderList)) {
					Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
							nErr = E_GUARDERINDUTY;
						}
						else if (pGuarder->uiState == STATE_GUARDER_FREE) {
							nErr = E_UNBINDDEVICE;
						}
						else if (pGuarder->uiState == STATE_GUARDER_BIND && strlen(pGuarder->szBindDevice) > 0) {
							if (strcmp(pGuarder->szBindDevice, bindInfo.szDeviceId) != 0) {
								nErr = E_DEVICENOTMATCH;
							}
							else {
								bValidGuarder = true;
							}
						}
						strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (!bValidGuarder) {
					break;
				}
				pthread_mutex_lock(&g_mutex4DevList);
				if (zhash_size(g_deviceList)) {
					WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo.szDeviceId);
					if (pDev) {
						if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
							|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							nErr = E_DEVICEINDUTY;
						}
						else if ((pDev->deviceBasic.nStatus & DEV_ONLINE) == DEV_ONLINE) {
							if (strlen(pDev->szBindGuard) == 0) {
								nErr = E_UNBINDDEVICE;
							}
							else {
								if (strcmp(pDev->szBindGuard, szGuarder) != 0) {
									nErr = E_GUARDERNOTMATCH;
								}
								else {//ok, update device here
									pDev->szBindGuard[0] = '\0';
									pDev->ulBindTime = 0;
								}
							}
						}
						strncpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId, 
							strlen(pDev->deviceBasic.szFactoryId));
					}
					else {
						nErr = E_INVALIDDEVICE;
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		} while (0);
	}
	//execute reply
	if (bindInfo.nMode == 0) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"battery\":%u}",
			access_service::E_CMD_BIND_REPLY, nErr, bindInfo.szSesssion, (nErr == E_OK) ? usBattery : 0);
	}
	else {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_UNBIND_REPLY, nErr, bindInfo.szSesssion);
	}
	sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
	if (nErr == E_OK) {
		//1.update information; 2.send to M_S
		if (bindInfo.nMode == 0) {
			//mark,version,type,sequence,datetime,report[subType,factoryId,deviceId,guarder]
			char szBody[256] = { 0 };
			snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guard"
				"er\":\"%s\"}]}", MSG_SUB_REPORT, bindInfo.uiReqSeq, bindInfo.szDateTime, SUB_REPORT_DEVICE_BIND,
				szFactory, bindInfo.szDeviceId, szGuarder);
			sendDataViaInteractor(szBody, strlen(szBody));

			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), bindInfo.szDeviceId,
					strlen(bindInfo.szDeviceId));
				pGuarder->uiState = STATE_GUARDER_BIND;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4LinkList);
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
				bindInfo.szSesssion);
			if (pLink) {
				strncpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactory, strlen(szFactory));
				strncpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), bindInfo.szDeviceId, 
					strlen(bindInfo.szDeviceId));
				strncpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg, strlen(szOrg));
			}
			pthread_mutex_unlock(&m_mutex4LinkList);

			char szTopic[64] = { 0 };
			snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, bindInfo.szDeviceId);
			access_service::AppSubscirbeInfo * pSubInfo;
			pSubInfo = (access_service::AppSubscirbeInfo *)zmalloc(sizeof(access_service::AppSubscirbeInfo));
			strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndPoint, strlen(pEndPoint));
			strncpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), szGuarder, strlen(szGuarder));
			strncpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), bindInfo.szSesssion,
				strlen(bindInfo.szSesssion));
			strncpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic, strlen(szTopic));
			pthread_mutex_lock(&m_mutex4SubscribeList);
			zhash_update(m_subscribeList, szTopic, pSubInfo);
			zhash_freefn(m_subscribeList, szTopic, free);
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
		else {
			char szBody[512] = { 0 };
			snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guard"
				"er\":\"%s\"}]}", MSG_SUB_REPORT, bindInfo.uiReqSeq, bindInfo.szDateTime, SUB_REPORT_DEVICE_UNBIND,
				szFactory, bindInfo.szDeviceId, szGuarder);
			sendDataViaInteractor(szBody, strlen(szBody));

			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				pGuarder->szBindDevice[0] = '\0';
				pGuarder->uiState = STATE_GUARDER_FREE;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4LinkList);
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
				bindInfo.szSesssion);
			if (pLink) {
				pLink->szDeviceId[0] = '\0';
				pLink->szFactoryId[0] = '\0';
				pLink->szOrg[0] = '\0';
			}
			pthread_mutex_unlock(&m_mutex4LinkList);

			char szTopic[64] = { 0 };
			snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, bindInfo.szDeviceId);
			pthread_mutex_lock(&m_mutex4SubscribeList);
			zhash_delete(m_subscribeList, szTopic);
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
	}	
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]%lu, %s device=%s, session=%s, datetime=%s, "
		"result=%d\r\n", __FUNCTION__, __LINE__, bindInfo.uiReqSeq, 
		(bindInfo.nMode == 0) ? "bind" : "unbind", bindInfo.szDeviceId, bindInfo.szSesssion, 
		bindInfo.szDateTime, nErr);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppSubmitTask(access_service::AppSubmitTaskInfo taskInfo, 
	const char * pEndpoint, unsigned long ulTime)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	bool bValidLink = false;
	bool bValidGuarder = false;
	bool bValidDevice = false;
	char szGuarder[20] = { 0 };
	char szFactoryId[4] = { 0 };
	char szDeviceId[16] = { 0 };
	char szOrg[40] = { 0 };
	char szTaskId[12] = { 0 };
	char szReply[256] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			taskInfo.szSession);
		if (pLink) {
			pLink->ulActivateTime = ulTime;
			pLink->nActivated = 1;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			strncpy_s(szFactoryId, sizeof(szFactoryId), pLink->szFactoryId, strlen(pLink->szFactoryId));
			strncpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId, strlen(pLink->szDeviceId));
			bValidLink = true;
		}
		else {
			nErr = E_INVALIDSESSION;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4GuarderList);
		if (zhash_size(g_guarderList)) {
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					nErr = E_GUARDERINDUTY;
				}
				else if (pGuarder->uiState == STATE_GUARDER_FREE) {
					nErr = E_UNBINDDEVICE;
				}
				else if (pGuarder->uiState == STATE_GUARDER_BIND) {
					bValidGuarder = true;
				}
				else {
					nErr = E_DEFAULTERROR;
				}
				strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
			}
			else {
				nErr = E_INVALIDACCOUNT;
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
	if (bValidGuarder) {
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
					nErr = E_UNABLEWORKDEVICE;
				}
				else if (((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) 
					|| ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE)) {
					nErr = E_DEVICEINDUTY;
				} 
				else if ((pDev->deviceBasic.nStatus & DEV_ONLINE) == DEV_ONLINE) {
					bValidDevice = true;
					changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bValidDevice) {
			EscortTask * pTask = (EscortTask *)zmalloc(sizeof(EscortTask));
			generateTaskId(szTaskId, sizeof(szTaskId));
			strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId, strlen(szTaskId));
			pTask->nTaskType = (uint8_t)taskInfo.usTaskType;
			pTask->nTaskState = 0;
			pTask->nTaskLimitDistance = (uint8_t)taskInfo.usTaskLimit;
			strncpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactoryId, strlen(szFactoryId));
			strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), szDeviceId, strlen(szDeviceId));
			strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg, strlen(szOrg));
			strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), szGuarder, strlen(szGuarder));
			if (strlen(taskInfo.szDestination) > 0) {
				strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), taskInfo.szDestination,
					strlen(taskInfo.szDestination));
			}
			if (strlen(taskInfo.szTarget) > 0) {
				strncpy_s(pTask->szTarget, sizeof(pTask->szTarget), taskInfo.szTarget, 
					strlen(taskInfo.szTarget));
			}
			strncpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), taskInfo.szDatetime,
				strlen(taskInfo.szDatetime));
			pthread_mutex_lock(&g_mutex4TaskList);
			zhash_update(g_taskList, szTaskId, pTask);
			zhash_freefn(g_taskList, szTaskId, free);
			pthread_mutex_unlock(&g_mutex4TaskList);
		}
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		access_service::E_CMD_TASK_REPLY, nErr, taskInfo.szSession, szTaskId);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	if (nErr == E_OK) {
		char szBody[512] = { 0 };
		snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"taskType\":%u,\"limit\":%u,"
			"\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\""
			"%s\"}]}", MSG_SUB_REPORT, taskInfo.uiReqSeq, taskInfo.szDatetime, SUB_REPORT_TASK, szTaskId,
			taskInfo.usTaskType, taskInfo.usTaskLimit, szFactoryId, szDeviceId, szGuarder,
			taskInfo.szDestination, taskInfo.szTarget);
		sendDataViaInteractor(szBody, strlen(szBody));
		bool bUpdateLink = false;
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId, strlen(szTaskId));
			strncpy_s(pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime), taskInfo.szDatetime,
				strlen(taskInfo.szDatetime));
			if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
				strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
				bUpdateLink = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		char szFilter[64] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			taskInfo.szSession);
		if (pLink) {
			strncpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), szTaskId, strlen(szTaskId));
			if (bUpdateLink) {
				strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
				snprintf(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, 
					pLink->szDeviceId);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bUpdateLink && strlen(szFilter)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task %d: session=%s, type=%u, limit=%u,"
		" target=%s, destination=%s, datetime=%s, result=%d\r\n", __FUNCTION__, __LINE__, taskInfo.uiReqSeq,
		taskInfo.szSession, taskInfo.usTaskType, taskInfo.usTaskLimit, taskInfo.szTarget, 
		taskInfo.szDestination, taskInfo.szDatetime, nErr);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppCloseTask(access_service::AppCloseTaskInfo taskInfo, const char * pEndpoint,
	unsigned long ulTime)
{
	char szLog[256] = { 0 };
	int nErr = E_OK;
	bool bValidLink = false;
	bool bValidTask = false;
	char szGuarder[20] = { 0 };
	char szReply[256] = { 0 };
	char szFilter[64] = { 0 };
	char szDeviceId[16] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			taskInfo.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			strncpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId, strlen(pLink->szDeviceId));
			if (strlen(pLink->szTaskId) > 0 && strcmp(pLink->szTaskId, taskInfo.szTaskId) == 0) {
				bValidLink = true;
				snprintf(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, 
					pLink->szDeviceId);
			}
			else {
				nErr = E_INVALIDTASK;
			}
		} 
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (!bValidLink) {
		nErr = E_INVALIDSESSION;
	}
	else {
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, taskInfo.szTaskId);
			if (pTask) {
				if (strlen(pTask->szGuarder) > 0 && strcmp(pTask->szGuarder, szGuarder) == 0) {
					bValidTask = true;
					pTask->nTaskState = (taskInfo.nCloseType == 0) ? 2 : 1;
					strncpy_s(pTask->szTaskStopTime, sizeof(pTask->szTaskStopTime), taskInfo.szDatetime,
						strlen(taskInfo.szDatetime));
				}
				else {
					nErr = E_GUARDERNOTMATCH;
				}
			}
			else {
				nErr = E_INVALIDTASK;
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		access_service::E_CMD_TAKK_CLOSE_REPLY, nErr, taskInfo.szSession, taskInfo.szTaskId);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	if (nErr == E_OK) {
		char szBody[256] = { 0 };
		snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"closeType\":%d}]}", 
			MSG_SUB_REPORT, taskInfo.uiReqSeq,taskInfo.szDatetime, SUB_REPORT_TASK_CLOSE, taskInfo.szTaskId, 
			taskInfo.nCloseType);
		sendDataViaInteractor(szBody, strlen(szBody));

		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			taskInfo.szSession);
		if (pLink) {
			if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
				strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
			}
			pLink->szTaskId[0] = '\0';
		}
		pthread_mutex_unlock(&m_mutex4LinkList);

		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
					//no change
				}
				else if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
					changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
				}
				else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
					changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		bool bUpdateLink = false;
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			if (pGuarder->uiState == STATE_GUARDER_DUTY) {
				pGuarder->uiState = STATE_GUARDER_BIND;
			}
			pGuarder->szTaskId[0] = '\0';
			pGuarder->szTaskStartTime[0] = '\0';
			if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
				strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
				bUpdateLink = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&g_mutex4TaskList);
		zhash_delete(g_taskList, taskInfo.szTaskId);
		pthread_mutex_unlock(&g_mutex4TaskList);
		if (bUpdateLink && strlen(szFilter)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit close task, session=%s, taskId=%s, "
		"datetime=%s, result=%d\r\n", __FUNCTION__, __LINE__, taskInfo.szSession, taskInfo.szTaskId, 
		taskInfo.szDatetime, nErr);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppPosition(access_service::AppPositionInfo posInfo, const char * pEndpoint, 
	unsigned long ulTime)
{
	char szLog[256] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szDevice[16] = { 0 };
	bool bUpdateLink = false;
	char szFilter[64] = { 0 };
	char szGuarder[20] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			posInfo.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime;
			if (strlen(pLink->szTaskId) > 0 && strcmp(pLink->szTaskId, posInfo.szTaskId) == 0) {
				bValidLink = true;
				snprintf(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, 
					pLink->szDeviceId);
				strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
				if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
					bUpdateLink = true;
					strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
				}
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, posInfo.szTaskId);
			if (pTask) {
				strncpy_s(szDevice, sizeof(szDevice), pTask->szDeviceId, strlen(pTask->szDeviceId));
				bValidTask = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
		if (bValidTask) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDevice);
			if (pDev) {
				if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
					|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
					pDev->guardPosition.dLatitude = posInfo.dLat;
					pDev->guardPosition.dLngitude = posInfo.dLng;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			char szBody[512] = { 0 };
			snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"lat\":%.06f,\"lng\":%.06f}]}",
				MSG_SUB_REPORT, posInfo.uiReqSeq, posInfo.szDatetime, SUB_REPORT_POSITION, posInfo.szTaskId,
				posInfo.dLat, posInfo.dLng);
			sendDataViaInteractor(szBody, strlen(szBody));
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4SubscribeList);
			access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
				m_subscribeList, szFilter);
			if (pSubInfo) {
				strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report position session=%s, taskId=%s, lat=%.06f, "
		"lng=%0.6f, datetime=%s\r\n", __FUNCTION__, __LINE__, posInfo.szSession, posInfo.szTaskId,
		posInfo.dLat, posInfo.dLng, posInfo.szDatetime);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppFlee(access_service::AppSubmitFleeInfo fleeInfo, const char * pEndpoint, 
	unsigned long ulTime)
{
	int nErr = E_OK;
	char szFactoryId[4] = { 0 };
	char szDeviceId[16] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szReply[256] = { 0 };
	bool bUpdateLink = false;
	char szGuarder[20] = { 0 };
	char szFilter[64] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			fleeInfo.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime;
			if (strlen(pLink->szTaskId) > 0 && strcmp(pLink->szTaskId, fleeInfo.szTaskId) == 0) {
				if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
					strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
					bUpdateLink = true;
					strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
				}
				bValidLink = true;
			}
			else {
				nErr = E_INVALIDTASK;
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, fleeInfo.szTaskId);
			if (pTask) {
				strncpy_s(szFactoryId, sizeof(szFactoryId), pTask->szFactoryId, strlen(pTask->szFactoryId));
				strncpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId, strlen(pTask->szDeviceId));
				snprintf(szFilter, sizeof(szFilter), "%s_%s_%s", pTask->szOrg, pTask->szFactoryId, pTask->szDeviceId);
				bValidTask = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
		if (bValidTask) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDev) {
				if (fleeInfo.nMode == 0) { //flee
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
						changeDeviceStatus(DEV_FLEE, pDev->deviceBasic.nStatus);
					}
					else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						//do nothing
					} 
					else if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						nErr = E_UNABLEWORKDEVICE;
					}
				}
				else { //flee revoke
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
						//do nothing
					}
					else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
					}
					else if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						nErr = E_UNABLEWORKDEVICE;
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);		
			if (bUpdateLink) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
						strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
			}
		}
		else {
			nErr = E_INVALIDTASK;
		}
	}
	else {
		nErr = E_INVALIDSESSION;
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		fleeInfo.nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
		nErr, fleeInfo.szSession, fleeInfo.szTaskId);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	if (nErr == E_OK) {
		char szBody[512] = { 0 };
		snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\"}]}",
			MSG_SUB_REPORT, fleeInfo.uiReqSeq, fleeInfo.szDatetime,
			fleeInfo.nMode == 0 ? SUB_REPORT_DEVICE_FLEE : SUB_REPORT_DEVICE_FLEE_REVOKE, fleeInfo.szTaskId);
		sendDataViaInteractor(szBody, strlen(szBody));
	}
	char szLog[512] = { 0 };
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report %s %d, session=%s, taskId=%s, datetime=%s"
		 ",result=%d\r\n" ,__FUNCTION__, __LINE__, fleeInfo.nMode == 0 ? "flee" : "flee revoke", 
		fleeInfo.uiReqSeq, fleeInfo.szSession, fleeInfo.szTaskId, fleeInfo.szDatetime, nErr);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
}

void AccessService::handleAppKeepAlive(access_service::AppKeepAlive keepAlive, const char * pEndpoint, 
	unsigned long ulTime)
{
	char szReply[256] = { 0 };
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu}",
		access_service::E_CMD_KEEPALIVE_REPLY, keepAlive.szSession, keepAlive.uiSeq);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			keepAlive.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
}

unsigned int AccessService::getNextRequestSequence()
{
	unsigned int result = 0;
	pthread_mutex_lock(&g_mutex4RequestSequence);
	result = ++g_uiRequestSequence;
	pthread_mutex_unlock(&g_mutex4RequestSequence);
	return result;
}

int AccessService::generateSession(char * pSession, size_t nSize)
{
	long now = (long)time(NULL);
	char szSession[16] = { 0 };
	snprintf(szSession, sizeof(szSession), "%ld", now);
	unsigned char key[crypto_generichash_KEYBYTES];
	randombytes_buf(key, sizeof(key));
	unsigned char szOut[10] = { 0 };
	crypto_shorthash(szOut, (const unsigned char *)szSession, strlen(szSession), key);
	if (nSize > 16 && pSession) {
		for (int i = 0; i < 8; i++) {
			char cell[4] = { 0 };
			snprintf(cell, sizeof(cell), "%02x", szOut[i]);
			strncat_s(pSession, nSize, cell, strlen(cell));
		}
		return 0;
	}
	else {
		return -1;
	}
}

void AccessService::generateTaskId(char * pTaskId, size_t nSize)
{
	static uint8_t n = 0;
	pthread_mutex_lock(&g_mutex4TaskId);
	unsigned int now = (unsigned int)time(NULL);
	char szTaskId[12] = { 0 };
	snprintf(szTaskId, sizeof(szTaskId), "%x%02x", now, ++n);
	pthread_mutex_unlock(&g_mutex4TaskId);
	if (strlen(szTaskId)) {
		strncpy_s(pTaskId, nSize, szTaskId, strlen(szTaskId));
	}
}

void AccessService::changeDeviceStatus(unsigned short usNewStatus, unsigned short & usDeviceStatus, 
	int nMode)
{
	if (usNewStatus == DEV_LOWPOWER || usNewStatus == DEV_LOOSE) {
		if (usDeviceStatus != DEV_OFFLINE) {
			if (nMode == 0) {
				usDeviceStatus += ((usDeviceStatus & usNewStatus) == usNewStatus) ? 0 : usNewStatus;
			}
			else {
				usDeviceStatus -= ((usDeviceStatus & usNewStatus) == usNewStatus) ? usNewStatus : 0;
			}
		}
	}
	else if (usNewStatus == DEV_OFFLINE) {
		usDeviceStatus = DEV_OFFLINE;
	}
	else if (usNewStatus == DEV_ONLINE || usNewStatus == DEV_GUARD || usNewStatus == DEV_FLEE) {
		usDeviceStatus = usDeviceStatus - (((usDeviceStatus & DEV_ONLINE) == DEV_ONLINE) ? DEV_ONLINE : 0)
			- (((usDeviceStatus & DEV_GUARD) == DEV_GUARD) ? DEV_GUARD : 0)
			- (((usDeviceStatus & DEV_FLEE) == DEV_FLEE) ? DEV_FLEE : 0)
			+ usNewStatus;
	}
}

bool AccessService::addTopicMsg(TopicMessage * pMsg)
{
	bool result = false;
	if (pMsg) {
		if (strlen(pMsg->szMsgUuid) && strlen(pMsg->szMsgBody)) {
			pthread_mutex_lock(&m_mutex4TopicMsgQueue);
			m_topicMsgQueue.push(pMsg);
			if (m_topicMsgQueue.size() == 1) {
				pthread_cond_signal(&m_cond4TopicMsgQueue);
			}
			pthread_mutex_unlock(&m_mutex4TopicMsgQueue);
			result = true;
		}
	}
	return result;
}

void AccessService::dealTopicMsg()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4TopicMsgQueue);
		while (m_nRun && m_topicMsgQueue.empty()) {
			pthread_cond_wait(&m_cond4TopicMsgQueue, &m_mutex4TopicMsgQueue);
		}
		if (!m_nRun && m_topicMsgQueue.empty()) {
			pthread_mutex_unlock(&m_mutex4TopicMsgQueue);
			break;
		}
		TopicMessage * pMsg = m_topicMsgQueue.front();
		m_topicMsgQueue.pop();
		pthread_mutex_unlock(&m_mutex4TopicMsgQueue);
		if (pMsg) { //M_S->A_S
			bool bNeedStore = false;
			switch (pMsg->usMsgType) {
				case MSG_DEVICE_ALIVE: {
					//parse pMsg->szMsgBody,
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicAliveMessage aliveMsg;
						//aliveMsg.usSequence = pMsg->usMsgSequence;
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szFactoryId, sizeof(aliveMsg.szFactoryId), 
										doc["factoryId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szDeviceId, sizeof(aliveMsg.szDeviceId), 
										doc["deviceId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szOrg, sizeof(aliveMsg.szOrg), doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								aliveMsg.usBattery = (unsigned short)doc["battery"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									aliveMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicAliveMsg(&aliveMsg, pMsg->szMsgMark) == E_OK) {
							bNeedStore = true;
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alive message error\r\n",
							__FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_LOCATE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case LOCATE_GPS: {
								TopicLocateMessageGps gpsLocateMsg;
								//gpsLocateMsg.usSequence = pMsg->usMsgSequence;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szFactoryId, sizeof(gpsLocateMsg.szFactoryId), 
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szDeviceId, sizeof(gpsLocateMsg.szDeviceId), 
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szOrg, sizeof(gpsLocateMsg.szOrg), 
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										gpsLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										gpsLocateMsg.dLat = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngtitude")) {
									if (doc["lngitude"].IsDouble()) {
										gpsLocateMsg.dLng = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										gpsLocateMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										gpsLocateMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("sattelite")) {
									if (doc["sattelite"].IsInt()) {
										gpsLocateMsg.usStattelite = (unsigned short)doc["sattelite"].GetInt();
									}
								}
								if (doc.HasMember("intensity")) {
									if (doc["intensity"].IsInt()) {
										gpsLocateMsg.usIntensity = (unsigned short)doc["intensity"].GetInt();
									}
								}
								if (doc.HasMember("speed")) {
									if (doc["speed"].IsDouble()) {
										gpsLocateMsg.dSpeed = doc["speed"].GetDouble();
									}
								}
								if (doc.HasMember("direction")) {
									if (doc["direction"].IsInt()) {
										gpsLocateMsg.nDirection = (short)doc["direction"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											gpsLocateMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
										}
									}
								}
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										gpsLocateMsg.usFlag = (unsigned short)doc["locateFlag"].GetInt();
									}
								}
								if (handleTopicLocateGpsMsg(&gpsLocateMsg, pMsg->szMsgMark) == E_OK) {
									bNeedStore = true;
								}
								break;
							}
							case LOCATE_LBS: {
								TopicLocateMessageLbs lbsLocateMsg;
								//lbsLocateMsg.usSequence = pMsg->usMsgSequence;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szFactoryId, sizeof(lbsLocateMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szDeviceId, sizeof(lbsLocateMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szOrg, sizeof(lbsLocateMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lbsLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										lbsLocateMsg.dLat = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngtitude")) {
									if (doc["lngitude"].IsDouble()) {
										lbsLocateMsg.dLng = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										lbsLocateMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										lbsLocateMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("precision")) {
									if (doc["precision"].IsInt()) {
										lbsLocateMsg.nPrecision = doc["precision"].GetInt();
									}
								}
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										lbsLocateMsg.usFlag = (unsigned short)doc["locateFlag"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											lbsLocateMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
										}
									}
								}
								if (handleTopicLocateLbsMsg(&lbsLocateMsg, pMsg->szMsgMark) == E_OK) {
									bNeedStore = true;
								}
								break;
							}
							case LOCATE_BT: { //ignore
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic locate message "
									"error, not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic locate message "
							"error\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_ALARM: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case ALARM_DEVICE_LOWPOWER: {
								TopicAlarmMessageLowpower lpAlarmMsg;
								//lpAlarmMsg.usSequence = pMsg->usMsgSequence;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(lpAlarmMsg.szFactoryId, sizeof(lpAlarmMsg.szFactoryId), 
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(lpAlarmMsg.szDeviceId, sizeof(lpAlarmMsg.szDeviceId), 
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(lpAlarmMsg.szOrg, sizeof(lpAlarmMsg.szOrg), 
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lpAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										lpAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											lpAlarmMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
										}
									}
								}
								if (handleTopicAlarmLowpowerMsg(&lpAlarmMsg, pMsg->szMsgMark) == E_OK) {
									bNeedStore = true;
								}
								break;
							}
							case ALARM_DEVICE_LOOSE: {
								TopicAlarmMessageLoose lsAlarmMsg;
								lsAlarmMsg.szFactoryId[0] = '\0';
								lsAlarmMsg.szDeviceId[0] = '\0';
								lsAlarmMsg.szOrg[0] = '\0';
								lsAlarmMsg.usMode = 0;
								lsAlarmMsg.ulMessageTime = 0;
								//lsAlarmMsg.usSequence = pMsg->usMsgSequence;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(lsAlarmMsg.szFactoryId, sizeof(lsAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(lsAlarmMsg.szDeviceId, sizeof(lsAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(lsAlarmMsg.szOrg, sizeof(lsAlarmMsg.szOrg), 
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										lsAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lsAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											lsAlarmMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
										}
									}
								}
								if (handleTopicAlarmLooseMsg(&lsAlarmMsg, pMsg->szMsgMark) == E_OK) {
									bNeedStore = true;
								}
								break;
							}
							case ALARM_DEVICE_FLEE: {
								break;
							}
							//case ALARM_DEVICE_FLEE_REVOKE: {
							//	break;
							//}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error,"
									" not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error\r\n",
							__FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_BIND: { //ignore
					break;
				}
				case MSG_DEVICE_ONLINE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicOnlineMessage onlineMsg;
						onlineMsg.szFactoryId[0] = '\0';
						onlineMsg.szDeviceId[0] = '\0';
						onlineMsg.szOrg[0] = '\0';
						onlineMsg.usBattery = 0;
						onlineMsg.ulMessageTime = 0;
						//onlineMsg.usSequence = pMsg->usMsgSequence;
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szFactoryId, sizeof(onlineMsg.szFactoryId), 
										doc["factoryId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szDeviceId, sizeof(onlineMsg.szDeviceId), 
										doc["deviceId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szOrg, sizeof(onlineMsg.szOrg), 
										doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								onlineMsg.usBattery = (unsigned short)doc["battery"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									onlineMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicOnlineMsg(&onlineMsg, pMsg->szMsgMark) == 0) {
							bNeedStore = true;
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic online message "
							"error\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_OFFLINE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicOfflineMessage offlineMsg;
						offlineMsg.szFactoryId[0] = '\0';
						offlineMsg.szDeviceId[0] = '\0';
						offlineMsg.szOrg[0] = '\0';
						offlineMsg.ulMessageTime = 0;
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szFactoryId, sizeof(offlineMsg.szFactoryId), 
										doc["factoryId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szDeviceId, sizeof(offlineMsg.szDeviceId), 
										doc["deviceId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szOrg, sizeof(offlineMsg.szOrg), 
										doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									offlineMsg.ulMessageTime = strdatetime2time(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicOfflineMsg(&offlineMsg, pMsg->szMsgMark) == 0) {
							bNeedStore = true;
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic offline message "
							"error\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_TASK: { //ignore
					break;
				}
				case MSG_BUFFER_MODIFY: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nObject = 0;
						int nOperate = 0;
						if (doc.HasMember("object")) {
							if (doc["object"].IsInt()) {
								nObject = doc["object"].GetInt();
							}
						}
						if (doc.HasMember("operate")) {
							if (doc["operate"].IsInt()) {
								nOperate = doc["operate"].GetInt();
							}
						}
						switch (nObject) {
							case BUFFER_GUARDER: {
								size_t nGuarderSize = sizeof(Guarder);
								Guarder guarder;
								memset(&guarder, 0, nGuarderSize);
								bool bValidGuarder = false;
								bool bValidOrg = false;
								bool bValidDateTime = false;
								char szDateTime[20] = { 0 };
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strncpy_s(guarder.szId, sizeof(guarder.szId), doc["guarder"].GetString(), nSize);
											bValidGuarder = true;
										}
									}								
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(guarder.szOrg, sizeof(guarder.szOrg), doc["orgId"].GetString(), nSize);
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDateTime, sizeof(szDateTime), doc["datetime"].GetString(), nSize);
											bValidDateTime = true;
										}
									}
								}
								if (nOperate == BUFFER_OPERATE_DELETE) {
									if (bValidGuarder && bValidOrg && bValidDateTime) {
										pthread_mutex_lock(&g_mutex4GuarderList);
										zhash_delete(g_guarderList, guarder.szId);
										pthread_mutex_unlock(&g_mutex4GuarderList);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic Buffer Modify,"
											" object=%u, operate=%d, guarder=%s, org=%s, datetime=%s\r\n", __FUNCTION__, 
											__LINE__, BUFFER_GUARDER, nOperate, bValidGuarder ? guarder.szId : "",
											bValidOrg ? guarder.szOrg : "", bValidDateTime ? szDateTime : "");
										writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									}
								}
								else {
									bool bValidName = false;
									bool bValidPasswd = false;
									if (doc.HasMember("name")) {
										if (doc["name"].IsString()) {
											size_t nSize = doc["name"].GetStringLength();
											if (nSize) {
												strncpy_s(guarder.szTagName, sizeof(guarder.szTagName), 
													doc["name"].GetString(), nSize);
												bValidName = true;
											}
										}
									}
									if (doc.HasMember("passwd")) {
										if (doc["passwd"].IsString()) {
											size_t nSize = doc["passwd"].GetStringLength();
											if (nSize) {
												strncpy_s(guarder.szPassword, sizeof(guarder.szPassword), 
													doc["passwd"].GetString(), nSize);
												bValidPasswd = true;
											}
										}
									}
									if (bValidGuarder && bValidOrg && bValidDateTime && bValidName && bValidPasswd) {
										if (nOperate == BUFFER_OPERATE_NEW) {
											pthread_mutex_lock(&g_mutex4GuarderList);
											Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, guarder.szId);
											if (pGuarder) {
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify "
													"message, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s,"
													"already exists in the guarderList\r\n", __FUNCTION__, __LINE__, 
													BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId, guarder.szTagName, 
													guarder.szPassword, guarder.szOrg);
												writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
											}
											else {
												pGuarder = (Guarder *)zmalloc(nGuarderSize);
												memset(pGuarder, 0, nGuarderSize);
												strncpy_s(pGuarder->szId, sizeof(pGuarder->szId), guarder.szId, 
													strlen(guarder.szId));
												strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
													guarder.szTagName, strlen(guarder.szTagName));
												strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), guarder.szOrg,
													strlen(guarder.szOrg));
												strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
													guarder.szPassword, strlen(guarder.szPassword));
												pGuarder->szLink[0] = '\0';
												pGuarder->szTaskId[0] = '\0';
												pGuarder->szBindDevice[0] = '\0';
												pGuarder->szCurrentSession[0] = '\0';
												pGuarder->szTaskStartTime[0] = '\0';
												zhash_update(g_guarderList, pGuarder->szId, pGuarder);
												zhash_freefn(g_guarderList, pGuarder->szId, free);
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify mes"
													"sage, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId, 
													guarder.szTagName, guarder.szPassword, guarder.szOrg);
												writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
											}
											pthread_mutex_unlock(&g_mutex4GuarderList);
										}
										else if (nOperate == BUFFER_OPERATE_UPDATE) {
											pthread_mutex_lock(&g_mutex4GuarderList);
											Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, guarder.szId);
											if (pGuarder) {
												if (strcmp(pGuarder->szTagName, guarder.szTagName) != 0) {
													strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
														guarder.szTagName, strlen(guarder.szTagName));
												}
												if (strcmp(pGuarder->szPassword, guarder.szPassword) != 0) {
													strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
														guarder.szPassword, strlen(guarder.szPassword));
												}
												if (strcmp(pGuarder->szOrg, guarder.szOrg) != 0) {
													strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), guarder.szOrg, 
														strlen(guarder.szOrg));
												}
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify mess"
													"age, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE, guarder.szId,
													guarder.szTagName, guarder.szPassword, guarder.szOrg);
												writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
											}
											else {
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify mess"
													"age, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, not "
													"found in guarderList\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER,
													BUFFER_OPERATE_UPDATE, guarder.szId, guarder.szTagName, guarder.szPassword,
													guarder.szOrg);
												writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
											}
											pthread_mutex_unlock(&g_mutex4GuarderList);
										}
									}
								}
								break;
							}
							case BUFFER_DEVICE: {
								size_t nDeviceSize = sizeof(WristletDevice);
								WristletDevice device;
								bool bValidDeviceId = false;
								bool bValidFactoryId = false;
								bool bValidOrgId = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(device.deviceBasic.szDeviceId, sizeof(device.deviceBasic.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDeviceId = true;
										}
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(device.deviceBasic.szFactoryId, 
												sizeof(device.deviceBasic.szFactoryId), 
												doc["factoryId"].GetString(), nSize);
											bValidFactoryId = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(device.szOrganization, sizeof(device.szOrganization),
												doc["orgId"].GetString(), nSize);
											bValidOrgId = true;
										}
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											bValidDatetime = true;
										}
									}
								}
								if (bValidDeviceId && bValidFactoryId && bValidOrgId && bValidDatetime) {
									char szDevKey[20] = { 0 };
									snprintf(szDevKey, sizeof(szDevKey), "%s_%s", device.deviceBasic.szFactoryId,
										device.deviceBasic.szDeviceId);
									if (nOperate == BUFFER_OPERATE_NEW) {
										pthread_mutex_lock(&g_mutex4DevList);
										WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDevKey);
										if (pDevice) {
											if (strlen(pDevice->szOrganization) == 0) {
												strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
													device.szOrganization, strlen(device.szOrganization));
											}
											if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
												strncpy_s(pDevice->deviceBasic.szFactoryId, 
													sizeof(pDevice->deviceBasic.szFactoryId),
													device.deviceBasic.szFactoryId,
													strlen(device.deviceBasic.szFactoryId));
											}
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]topic Buffer modify mess"
												"age, object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s"
												", already found in the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, 
												nOperate, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId, 
												device.szOrganization, szDatetime);
											writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
										}
										else {
											pDevice = (WristletDevice *)zmalloc(nDeviceSize);
											memset(pDevice, 0, nDeviceSize);
											strncpy_s(pDevice->deviceBasic.szDeviceId, 
												sizeof(pDevice->deviceBasic.szDeviceId),
												device.deviceBasic.szDeviceId, 
												strlen(device.deviceBasic.szDeviceId));
											strncpy_s(pDevice->deviceBasic.szFactoryId, 
												sizeof(pDevice->deviceBasic.szFactoryId),
												device.deviceBasic.szFactoryId, 
												strlen(device.deviceBasic.szFactoryId));
											strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
												device.szOrganization, strlen(device.szOrganization));
											pDevice->deviceBasic.nBattery = 0;
											pDevice->deviceBasic.nStatus = DEV_OFFLINE;
											pDevice->deviceBasic.ulLastActiveTime = 0;
											pDevice->nLastLocateType = 0;
											pDevice->szBindGuard[0] = '\0';
											pDevice->szLinkId[0] = '\0';
											pDevice->ulBindTime = 0;
											pDevice->ulLastFleeAlertTime = 0;
											pDevice->ulLastLocateTime = 0;
											pDevice->ulLastLooseAlertTime = 0;
											pDevice->ulLastLowPowerAlertTime = 0;
											pDevice->devicePosition.dLatitude = pDevice->devicePosition.dLngitude = 0.000000;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = 0;
											pDevice->guardPosition.dLatitude = pDevice->guardPosition.dLngitude = 0.000000;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->guardPosition.nPrecision = 0;
											zhash_update(g_deviceList, szDevKey, pDevice);
											zhash_freefn(g_deviceList, szDevKey, free);
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]topic Buffer modify message,"
												" object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
												__FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate, device.deviceBasic.szDeviceId,
												device.deviceBasic.szFactoryId, device.szOrganization, szDatetime);
											writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
										}
										pthread_mutex_unlock(&g_mutex4DevList);
									}
									else if (nOperate == BUFFER_OPERATE_UPDATE) {
										pthread_mutex_lock(&g_mutex4DevList);
										WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDevKey);
										if (pDevice) {
											if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
												strncpy_s(pDevice->deviceBasic.szFactoryId, 
													sizeof(pDevice->deviceBasic.szFactoryId),
													device.deviceBasic.szFactoryId, 
													strlen(device.deviceBasic.szFactoryId));
											}
											if (strcmp(pDevice->szOrganization, device.szOrganization) != 0) {
												strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
													device.szOrganization, strlen(device.szOrganization));
											}
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message, "
												"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
												__FUNCTION__, __LINE__, BUFFER_DEVICE, BUFFER_OPERATE_UPDATE,
												device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
												device.szOrganization, szDatetime);
											writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
										}
										else {
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
												"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s, not "
												"find int the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, 
												BUFFER_OPERATE_UPDATE, device.deviceBasic.szDeviceId, 
												device.deviceBasic.szFactoryId, device.szOrganization, szDatetime);
											writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
										}
										pthread_mutex_unlock(&g_mutex4DevList);
									}
									else if (nOperate == BUFFER_OPERATE_DELETE) {
										pthread_mutex_lock(&g_mutex4DevList);
										zhash_delete(g_deviceList, szDevKey);
										pthread_mutex_unlock(&g_mutex4DevList);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
										" object=%u, operate=%d, parameter miss, deviceId=%s, factoryId=%s, orgId=%s, "
										"datetime=%s\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate,
										bValidDeviceId ? device.deviceBasic.szDeviceId : "",
										bValidFactoryId ? device.deviceBasic.szFactoryId : "",
										bValidOrgId ? device.szOrganization : "", bValidDatetime ? szDatetime : "");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
									"object=%d not support, seq=%u\r\n", __FUNCTION__, __LINE__, nObject, 
									pMsg->usMsgSequence);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic buffer modify "
							"message error, seq=%u, \r\n", __FUNCTION__, __LINE__, pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
			}
			if (bNeedStore && m_nRun) {
				storeTopicMsg(pMsg);
			}
		}
	} while (1);
}

void AccessService::storeTopicMsg(const TopicMessage * pMsg)
{
	if (strlen(pMsg->szMsgBody) && strlen(pMsg->szMsgUuid) && pMsg->usMsgType > 0) {
		size_t nSize = sizeof(TopicMessage);
		TopicMessage * pMsgCopy = (TopicMessage *)zmalloc(nSize);
		memcpy_s(pMsgCopy, nSize, pMsg, nSize);
		pthread_mutex_lock(&m_mutex4LocalTopicMsgList);
		size_t nListSize = zlist_size(m_localTopicMsgList);
		if (nListSize < LOCAL_KEEP_MESSAGE) {
			zlist_push(m_localTopicMsgList, pMsgCopy);
			zlist_freefn(m_localTopicMsgList, pMsgCopy, free, false);
		}
		else {
			TopicMessage * pMsgLast = (TopicMessage *)zlist_last(m_localTopicMsgList);
			zlist_push(m_localTopicMsgList, pMsgCopy);
			zlist_freefn(m_localTopicMsgList, pMsgCopy, free, false);
		}
		pthread_mutex_unlock(&m_mutex4LocalTopicMsgList);
	}
}

bool AccessService::addInteractionMsg(InteractionMessage * pMsg)
{
	bool result = false;
	if (pMsg) {
		if (pMsg->pMsgContents && pMsg->uiContentCount > 0) {
			pthread_mutex_lock(&m_mutex4InteractionMsgQueue);
			m_interactionMsgQueue.push(pMsg);
			if (m_interactionMsgQueue.size() == 1) {
				pthread_cond_signal(&m_cond4InteractionMsgQueue);
			}
			pthread_mutex_unlock(&m_mutex4InteractionMsgQueue);
			result = true;
		}
	}
	return result;
}

void AccessService::dealInteractionMsg()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4InteractionMsgQueue);
		while (m_nRun && m_interactionMsgQueue.empty()) {
			pthread_cond_wait(&m_cond4InteractionMsgQueue, &m_mutex4InteractionMsgQueue);
		}
		if (!m_nRun && m_interactionMsgQueue.empty()) {
			pthread_mutex_unlock(&m_mutex4InteractionMsgQueue);
			break;
		}
		InteractionMessage * pMsg = m_interactionMsgQueue.front();
		m_interactionMsgQueue.pop();
		pthread_mutex_unlock(&m_mutex4InteractionMsgQueue);
		if (pMsg && pMsg->pMsgContents) {
			for (unsigned int i = 0; i < pMsg->uiContentCount; i++) {
				rapidjson::Document doc;
				if (!doc.Parse(pMsg->pMsgContents[i]).HasParseError()) {
					bool bValidMsg = false;
					bool bValidType = false;
					bool bValidSeq = false;
					bool bValidTime = false;
					if (doc.HasMember("mark") && doc.HasMember("version")) {
						if (doc["mark"].IsString() && doc["version"].IsString()) {
							size_t nSize1 = doc["mark"].GetStringLength();
							size_t nSize2 = doc["version"].GetStringLength();
							if (nSize1 && nSize2) {
								if (strcmp(doc["mark"].GetString(), "EC") == 0) {
									bValidMsg = true;
								}
							}
						}
					}
					if (bValidMsg) {
						int nType = 0;
						int nSequence = 0;
						char szDatetime[16] = { 0 };
						if (doc.HasMember("type")) {
							if (doc["type"].IsInt()) {
								nType = doc["type"].GetInt();
								bValidType = true;
							}
						}
						if (doc.HasMember("sequence")) {
							if (doc["sequence"].IsInt()) {
								nSequence = doc["sequence"].GetInt();
								bValidSeq = true;
							}
						}			
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize && nSize <= sizeof(szDatetime)) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									bValidTime = true;
								}
							}
						}
						if (bValidType && bValidSeq && bValidTime) {
							switch (nType) {
								case MSG_SUB_ALIVE: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									break;
								}
								case MSG_SUB_SNAPSHOT: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									int nContinue = -1;
									if (doc.HasMember("continue")) {
										if (doc["continue"].IsInt()) {
											nContinue = doc["continue"].GetInt();
										}
										if (nContinue == 1) {
											if (doc.HasMember("msg")) {
												TopicMessage topicMsg;
												bool bValidUuid = false;
												bool bValidMsg = false;
												if (doc["msg"][0].HasMember("msgMark")) {
													if (doc["msg"][0]["msgMark"].IsString()) {
														size_t nSize = doc["msg"][0]["msgMark"].GetStringLength();
														if (nSize) {
															strncpy_s(topicMsg.szMsgMark, sizeof(topicMsg.szMsgMark),
																doc["msg"][0]["msgMark"].GetString(), nSize);
														}
													}
												}
												if (doc["msg"][0].HasMember("msgSequence")) {
													if (doc["msg"][0]["msgSequence"].IsInt()) {
														topicMsg.usMsgSequence = (unsigned short)doc["msg"][0]["msgSequence"].GetInt();
													}
												}
												if (doc["msg"][0].HasMember("msgType")) {
													if (doc["msg"][0]["msgType"].IsInt()) {
														topicMsg.usMsgType = (unsigned short)doc["msg"][0]["msgType"].GetInt();
													}
												}
												if (doc["msg"][0].HasMember("msgUuid")) {
													if (doc["msg"][0]["msgType"].IsString()) {
														size_t nSize = doc["msg"][0]["msgMark"].GetStringLength();
														if (nSize) {
															strncpy_s(topicMsg.szMsgUuid, sizeof(topicMsg.szMsgUuid),
																doc["msg"][0]["msgUuid"].GetString(), nSize);
															bValidUuid = true;
														}
													}
												}
												if (doc["msg"][0].HasMember("msgBody")) {
													if (doc["msg"][0]["msgBody"].IsString()) {
														size_t nSize = doc["msg"][0]["msgBody"].GetStringLength();
														if (nSize) {
															strncpy_s(topicMsg.szMsgBody, sizeof(topicMsg.szMsgBody),
																doc["msg"][0]["msgBody"].GetString(), nSize);
															bValidMsg = true;
														}
													}
												}
												if (bValidMsg && bValidUuid) {
													storeTopicMsg(&topicMsg);
												}
											}
										}
									}									
									break;
								}
								case MSG_SUB_REPORT: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									break;
								}
								default: {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message "
										"failed, unsupport type: %d\r\n", __FUNCTION__, __LINE__, nType);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									break;
								}
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, "
								"json data miss parameter, type=%d, sequence=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, bValidType ? nType : -1, bValidSeq ? nSequence : -1, 
								bValidTime ? szDatetime : "null");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, "
							"verify message head failed\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
				}
				else {
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, JSON"
						" data parse error: %s\r\n", __FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
					writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
				}
			}
			free(pMsg->pMsgContents);
			pMsg->pMsgContents = NULL;
			free(pMsg->uiContentLens);
			pMsg->uiContentLens = NULL;
			free(pMsg);
			pMsg = NULL;
		}
	} while (1);
}

int AccessService::handleTopicOnlineMsg(TopicOnlineMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg) {
		bool bValidMsg = false;
		unsigned short usBattery = 0;
		if (strlen(pMsg->szDeviceId) > 0) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
						bValidMsg = true;
						result = E_OK;
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
			char szDatetime[16] = { 0 };
			format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
			bool bFindSub = false;
			char szSession[20] = { 0 };
			char szEndpoint[40] = { 0 };
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = NULL;
				pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
				if (pSubInfo) {
					bFindSub = true;
					strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
					strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szSession));
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
					"\"battery\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
					access_service::E_NOTIFY_DEVICE_ONLINE, pMsg->szDeviceId, usBattery, szDatetime);
				if (strlen(szMsg)) {
					sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicOfflineMsg(TopicOfflineMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg) {
		if (pMsg->szDeviceId) {
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					changeDeviceStatus(DEV_OFFLINE, pDev->deviceBasic.nStatus);
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					pDev->deviceBasic.nBattery = 0;
					bValidMsg = true;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
				bool bNotifyMsg = false;
				char szSession[20] = { 0 };
				char szEndpoint[40] = { 0 };
				char szDatetime[16] = { 0 };
				format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscirbeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
						bNotifyMsg = true;
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bNotifyMsg) {
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
						access_service::E_NOTIFY_DEVICE_OFFLINE, pMsg->szDeviceId, szDatetime);
					if (strlen(szMsg)) {
						sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAliveMsg(TopicAliveMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg) {
		bool bValidMsg = false;
		unsigned short usBattery = 0;
		if (strlen(pMsg->szDeviceId) > 0) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					bValidMsg = true;
					result = E_OK;
				}		
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
			char szDatetime[16] = { 0 };
			format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
			char szSession[20] = { 0 };
			char szEndpoint[40] = { 0 };
			bool bFindSub = false;
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = NULL;
				pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
				if (pSubInfo) {
					bFindSub = true;
					strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
					strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));				
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\""
					":\"%s\",\"battery\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
					access_service::E_ALARM_DEVICE_LOWPOWER, pMsg->szDeviceId, usBattery, szDatetime);
				if (strlen(szMsg)) {
					sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicLocateGpsMsg(TopicLocateMessageGps * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg && pMsg->dLat > 0 && pMsg->dLng > 0 && pMsg->usFlag == 1) {
		if (strlen(pMsg->szDeviceId)) {
			bool bValidMsg = false;
			unsigned short usBattery = 0;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
					}
					if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						bValidMsg = true;
					}	
					pDev->devicePosition.dLatitude = pMsg->dLat;
					pDev->devicePosition.dLngitude = pMsg->dLng;
					pDev->devicePosition.usLatType = pMsg->usLatType;
					pDev->devicePosition.usLngType = pMsg->usLngType;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[16] = { 0 };
				format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[40] = { 0 };
				bool bNotifyMsg = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscirbeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bNotifyMsg = true;
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bNotifyMsg) {
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery,
						pMsg->dLat, pMsg->dLng, szDatetime);
					if (strlen(szMsg)) {
						sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicLocateLbsMsg(TopicLocateMessageLbs * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg && pMsg->usFlag == 1 && pMsg->dLat > 0 && pMsg->dLng) {
		if (strlen(pMsg->szDeviceId) > 0) {
			unsigned short usBattery = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
					}
					if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						//bValidMsg = true; 
					}
					//this code would open if LBS position is correct 
					//pDev->devicePosition.dLatitude = pMsg->dLat;
					//pDev->devicePosition.dLngitude = pMsg->dLng;
					//pDev->devicePosition.usLatType = pMsg->usLatType;
					//pDev->devicePosition.usLngType = pMsg->usLngType;
					//pDev->devicePosition.nPrecision = pMsg->nPrecision;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szSession[20] = { 0 };
				char szEndpoint[20] = { 0 };
				char szDatetime[16] = { 0 };
				format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				bool bNotifyMsg = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscirbeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bNotifyMsg = true;
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bValidMsg) {
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery,
						pMsg->dLat, pMsg->dLng, szDatetime);
					sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAlarmLowpowerMsg(TopicAlarmMessageLowpower * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			unsigned short usBattery = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
						pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
						if (pMsg->usBattery > 0) {
							pDev->deviceBasic.nBattery = pMsg->usBattery;
						}
						usBattery = pDev->deviceBasic.nBattery;
						bool bLowpower = ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER);
						if (!bLowpower) {
							if (pMsg->usMode == 0) {
								changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus);
								bValidMsg = true;
							}
						}
						else {
							if (pMsg->usMode == 1) {
								changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus, 1);
								bValidMsg = true;
							}
						}
						result = E_OK;
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[16] = { 0 };
				format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[40] = { 0 };
				bool bNotifyMsg = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscirbeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bNotifyMsg = true;
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bNotifyMsg) {
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_ALARM_DEVICE_LOWPOWER, pMsg->szDeviceId, pMsg->usBattery,
						pMsg->usMode, szDatetime);
					if (strlen(szMsg)) {
						sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAlarmLooseMsg(TopicAlarmMessageLoose * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			unsigned short usBattery = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
						pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
						result = E_OK;
						if (pMsg->usBattery > 0) {
							pDev->deviceBasic.nBattery = pMsg->usBattery;
						}
						usBattery = pDev->deviceBasic.nBattery;
						bool bLoose = ((pDev->deviceBasic.nStatus & DEV_LOOSE) == DEV_LOOSE);
						if (bLoose) {
							if (pMsg->usMode == 1) {
								changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus, 1);
								bValidMsg = true;
							}
						}
						else {
							if (pMsg->usMode == 0) {
								changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus);
								bValidMsg = true;
							}
						}
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[16] = { 0 };
				format_datetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[40] = { 0 };
				bool bNotifyMsg = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscirbeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bNotifyMsg = true;
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bNotifyMsg) {
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"mode\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
						access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, pMsg->usBattery, pMsg->usMode, szDatetime);
					if (strlen(szMsg)) {
						sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
					}
				}
			}
		}
	}
	return result;
}

void AccessService::initZookeeper()
{
	int nTimeout = 30000;
	if (!m_zkHandle) {
		m_zkHandle = zookeeper_init(m_szHost, zk_server_watcher, nTimeout, NULL, this, 0);
	}
	if (m_zkHandle) {
		zoo_acreate(m_zkHandle, "/escort", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, zk_escort_create_completion, this);
		zoo_acreate(m_zkHandle, "/escort/access", "", 1024, &ZOO_OPEN_ACL_UNSAFE, 0, zk_access_create_completion,
			this);
	}
}

int AccessService::competeForMaster()
{
	if (m_bConnectZk) {
		char * path = make_zkpath(2, ESCORT_ACCESS_PATH, "master");
		int ret = zoo_acreate(m_zkHandle, path, "", 1024, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
			zk_access_master_create_completion, this);
		free(path);
		if (ret == 0) {
			return 0;
		}
	}
	return -1;
}

void AccessService::masterExist()
{
	if (m_bConnectZk) {
		char * path = make_zkpath(2, ESCORT_ACCESS_PATH, "master");
		zoo_awexists(m_zkHandle, path, zk_access_master_exists_watcher, this,
			zk_access_master_exists_completion, this);
		free(path);
	}
}

int AccessService::setAccessData(const char * path, void * data, size_t data_size)
{
	if (m_bConnectZk) {
		char szBuf[1024] = { 0 };
		memcpy_s(szBuf, 1024, data, data_size);
		int ret = zoo_aset(m_zkHandle, path, szBuf, data_size, -1, zk_access_set_completion, this);
		return ret;
	}
	return -1;
}

int AccessService::runAsSlaver()
{
	if (m_bConnectZk) {
		char * path = make_zkpath(2, ESCORT_ACCESS_PATH, "slaver_");
		int ret = zoo_acreate(m_zkHandle, path, "", 1024, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL + ZOO_SEQUENCE,
			zk_access_slaver_create_completion, this);
		free(path);
		return ret;
	}
	return -1;
}

void AccessService::removeSlaver()
{
	if (m_bConnectZk) {
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		m_zkNodePath[0] = '\0';
	}
}

int AccessService::getMidwareData(const char * pPath, ZkMidware * pMidware)
{
	if (m_bConnectZk) {
		char * pMidwarePath = make_zkpath(2, ESCORT_MIDWARE_PATH, "master");
		size_t nSize = sizeof(ZkMidware);
		char * pBuffer = (char *)malloc(nSize + 1);
		int nBufLen = nSize;
		Stat stat;
		int rc = zoo_get(m_zkHandle, pMidwarePath, 0, pBuffer, &nBufLen, &stat);
		if (rc == ZOK) {
			if (pMidware) {
				memcpy_s(pMidware, nSize, pBuffer, nSize);
			}
			return 0;
		}
		else if (rc == ZNONODE) {

		}
	}
	return -1;
}

int AccessService::sendDatatoEndpoint(const char * pData, size_t nDataLen, const char * pEndpoint)
{
	int result = E_DEFAULTERROR;
	if (pData && nDataLen && pEndpoint) {
		size_t nHeadSize = sizeof(access_service::AppMessageHead);
		access_service::AppMessageHead head;
		MAKE_APPMSGHEAD(head);
		head.uiDataLen = nDataLen;
		unsigned long ulBufLen = nHeadSize + nDataLen;
		unsigned char * pMsgBuf = (unsigned char *)zmalloc(ulBufLen + 1);
		memcpy_s(pMsgBuf, ulBufLen, &head, nHeadSize);
		unsigned long ulOffset = nHeadSize;
		memcpy_s(pMsgBuf + ulOffset, nDataLen + 1, pData, nDataLen);
		encryptMessage(pMsgBuf, ulOffset, ulOffset + nDataLen);
		result = TS_SendData(m_uiSrvInst, pEndpoint, (const char *)pMsgBuf, ulBufLen);
		free(pMsgBuf);
		pMsgBuf = NULL;
		result = 0;
	}
	return result;
}

int AccessService::sendDataViaInteractor(const char *pData, size_t nDataLen)
{
	int result = E_DEFAULTERROR;
	if (pData && nDataLen) {
		unsigned char * pFrameData = (unsigned char *)zmalloc(nDataLen + 1);
		memcpy_s(pFrameData, nDataLen, pData, nDataLen);
		pFrameData[nDataLen] = '\0';
		zmsg_t * msg = zmsg_new();
		zmsg_addmem(msg, pFrameData, nDataLen);
		zmsg_send(&msg, m_interactor);
		free(pFrameData);
		pFrameData = NULL;
		result = E_OK;
	}
	return result;
}

void AccessService::dealNetwork()
{
	zmq_pollitem_t items[] = { {m_subscriber, 0, ZMQ_POLLIN, 0}, {m_interactor, 0, ZMQ_POLLIN, 0}};
	while (m_nRun) {
		int rc = zmq_poll(items, 2, 1000 * ZMQ_POLL_MSEC);
		if (rc == -1 && errno == ETERM) {
			break;
		}
		if (items[0].revents & ZMQ_POLLIN) { //subscirbe
			zmsg_t * subMsg = zmsg_recv(items[0].socket);
			if (subMsg) {
				zframe_t * frame_mark = zmsg_pop(subMsg);
				zframe_t * frame_seq = zmsg_pop(subMsg);
				zframe_t * frame_type = zmsg_pop(subMsg);
				zframe_t * frame_uuid = zmsg_pop(subMsg);
				zframe_t * frame_body = zmsg_pop(subMsg);
				char * strMark = zframe_strdup(frame_mark);
				char * strSeq = zframe_strdup(frame_seq);
				char * strType = zframe_strdup(frame_type);
				char * strUuid = zframe_strdup(frame_uuid);
				char * strBody = zframe_strdup(frame_body);
				TopicMessage * pMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
				if (pMsg) {
					strncpy_s(pMsg->szMsgMark, sizeof(pMsg->szMsgMark), strMark, strlen(strMark));
					pMsg->usMsgSequence = (unsigned short)atoi(strSeq);
					pMsg->usMsgType = (unsigned short)atoi(strType);
					strncpy_s(pMsg->szMsgUuid, sizeof(pMsg->szMsgUuid), strUuid, strlen(strUuid));
					strncpy_s(pMsg->szMsgBody, sizeof(pMsg->szMsgBody), strBody, strlen(strBody));
					if (!addTopicMsg(pMsg)) {
						free(pMsg);
					}
				}
				zframe_destroy(&frame_mark);
				zframe_destroy(&frame_seq);
				zframe_destroy(&frame_type);
				zframe_destroy(&frame_uuid);
				zframe_destroy(&frame_body);
				zmsg_destroy(&subMsg);
				free(strMark);
				free(strSeq);
				free(strType);
				free(strUuid);
				free(strBody);
			}
		}
		if (items[1].revents & ZMQ_POLLIN) { //interactor
			zmsg_t * reply_msg = zmsg_recv(items[1].socket);
			if (!reply_msg) {
				break;
			}
			size_t nCount =	zmsg_size(reply_msg);
			if (nCount > 0) {
				zframe_t ** frame_replys = (zframe_t **)zmalloc(nCount * sizeof(zframe_t *));
				InteractionMessage * pMsg = (InteractionMessage *)zmalloc(sizeof(InteractionMessage));
				pMsg->uiContentCount = nCount;
				pMsg->pMsgContents = (char **)zmalloc(sizeof(char *) * nCount);
				pMsg->uiContentLens = (unsigned int *)zmalloc(sizeof(unsigned int) * nCount); 
				for (size_t i = 0; i < nCount; i++) {
					frame_replys[i] = zmsg_pop(reply_msg);
					size_t nFrameLen = zframe_size(frame_replys[i]);
					pMsg->uiContentLens[i] = nFrameLen;
					pMsg->pMsgContents[i] = zframe_strdup(frame_replys[i]);
					zframe_destroy(&frame_replys[i]);
				}
				if (!addInteractionMsg(pMsg)) {
					for (size_t i = 0; i < nCount; i++) {
						free(pMsg->pMsgContents[i]);
					}
					free(pMsg->pMsgContents);
					free(pMsg->uiContentLens);
					free(pMsg);
				}
				free(frame_replys);
				frame_replys = NULL;
			}
			zmsg_destroy(&reply_msg);
		}
	}
}

void AccessService::handleLinkDisconnect(const char * pLink)
{
	if (pLink) {
		std::string strLink = pLink;
		char szGuarder[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(pLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				pLinkData->nLinkState = 1;
				if (strlen(pLinkData->szUser)) {
					strncpy_s(szGuarder, sizeof(szGuarder), pLinkData->szUser, strlen(pLinkData->szUser));
				}
				if (pLinkData->pLingeData && pLinkData->ulTotalDataLen) {
					free(pLinkData->pLingeData);
					pLinkData->pLingeData = NULL;
					pLinkData->ulTotalDataLen = 0;
				}		
				delete pLinkData;
				pLinkData = NULL;
			}
			m_linkDataList.erase(iter);
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (strlen(szGuarder) > 0) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				pGuarder->szLink[0] = '\0';
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
}

void AccessService::readDataBuffer()
{
	//pthread_mutex_lock(&g_mutex4DevList);
	//WristletDevice * pDev = (WristletDevice *)malloc(sizeof(WristletDevice));
	//if (pDev) {
	//	pDev->deviceBasic.nBattery = 100;
	//	pDev->deviceBasic.nStatus = 0;
	//	sprintf_s(pDev->deviceBasic.szDeviceId, sizeof(pDev->deviceBasic.szDeviceId), "devId01");
	//	sprintf_s(pDev->deviceBasic.szFactoryId, sizeof(pDev->deviceBasic.szFactoryId), "01");
	//	pDev->deviceBasic.ulLastActiveTime = (unsigned long)time(NULL);
	//	sprintf_s(pDev->szOrganization, sizeof(pDev->szOrganization), "org01");
	//	pDev->deviceBasic.nStatus = DEV_ONLINE;
	//	pDev->ulBindTime = 0;
	//	pDev->ulLastFleeAlertTime = 0;
	//	pDev->ulLastLocateTime = 0;
	//	pDev->ulLastLooseAlertTime = 0;
	//	pDev->ulLastLowPowerAlertTime = 0;
	//	pDev->szBindGuard[0] = '\0';
	//	pDev->szLinkId[0] = '\0';
	//	zhash_update(g_deviceList, pDev->deviceBasic.szDeviceId, pDev);
	//	zhash_freefn(g_deviceList, pDev->deviceBasic.szDeviceId, free);
	//}
	//pthread_mutex_unlock(&g_mutex4DevList);
	//pthread_mutex_lock(&g_mutex4GuarderList);
	//Guarder * pGuarder = (Guarder *)malloc(sizeof(Guarder));
	//if (pGuarder) {
	//	sprintf_s(pGuarder->szId, sizeof(pGuarder->szId), "test");
	//	sprintf_s(pGuarder->szPassword, sizeof(pGuarder->szPassword), "test123");
	//	sprintf_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), "org01");
	//	sprintf_s(pGuarder->szTagName, sizeof(pGuarder->szTagName), "test");
	//	pGuarder->szBindDevice[0] = '\0';
	//	pGuarder->szCurrentSession[0] = '\0';
	//	pGuarder->szLink[0] = '\0';
	//	pGuarder->szTaskStartTime[0] = '\0';
	//	pGuarder->szTaskId[0] = '\0';
	//	pGuarder->uiState = 0;
	//	zhash_update(g_guarderList, pGuarder->szId, pGuarder);
	//	zhash_freefn(g_guarderList, pGuarder->szId, free);
	//}
	//pthread_mutex_unlock(&g_mutex4GuarderList);	
	escort_db::SqlContainer guarderContainer;
	escort_db::SqlContainer deviceContainer;
	escort_db::SqlContainer taskContainer;
	size_t nContainerSize = sizeof(escort_db::SqlContainer);
	guarderContainer.usSqlOptTarget = escort_db::E_TBL_GUARDER;
	guarderContainer.usSqlOptType = escort_db::E_OPT_QUERY;
	guarderContainer.uiSqlOptSeq = getNextRequestSequence();
	guarderContainer.ulSqlOptTime = (unsigned long)time(NULL);
	guarderContainer.uiResultCount = 0;
	guarderContainer.uiResultLen = 0;
	guarderContainer.szSqlOptKey[0] = '\0';
	guarderContainer.pStoreResult = NULL;
	deviceContainer.usSqlOptTarget = escort_db::E_TBL_DEVICE;
	deviceContainer.usSqlOptType = escort_db::E_OPT_QUERY;
	deviceContainer.uiSqlOptSeq = getNextRequestSequence();
	deviceContainer.ulSqlOptTime = (unsigned long)time(NULL);
	deviceContainer.uiResultCount = 0;
	deviceContainer.uiResultLen = 0;
	deviceContainer.pStoreResult = NULL;
	deviceContainer.szSqlOptKey[0] = '\0';
	taskContainer.usSqlOptTarget = escort_db::E_TBL_TASK;
	taskContainer.usSqlOptType = escort_db::E_OPT_QUERY;
	taskContainer.uiSqlOptSeq = getNextRequestSequence();
	taskContainer.ulSqlOptTime = (unsigned long)time(NULL);
	taskContainer.uiResultCount = 0;
	taskContainer.uiResultLen = 0;
	taskContainer.pStoreResult = NULL;
	taskContainer.szSqlOptKey[0] = '\0';
	do {
		zmsg_t * msg_guarder_req = zmsg_new();
		zframe_t * frame_guarder_req = zframe_new(&guarderContainer, nContainerSize);
		zmsg_append(msg_guarder_req, &frame_guarder_req);
		zmsg_send(&msg_guarder_req, m_seeker);
		zmsg_t * msg_guarder_rep = zmsg_recv(m_seeker);
		if (!msg_guarder_rep) {
			break;
		}
		zframe_t * frame_guarder_rep = zmsg_pop(msg_guarder_rep);
		if (!frame_guarder_rep) {
			zmsg_destroy(&msg_guarder_rep);
			break;
		}
		size_t nFrameGuarderRepSize = zframe_size(frame_guarder_rep);
		unsigned char * pFrameGuarderRepData = zframe_data(frame_guarder_rep);
		if (pFrameGuarderRepData && nFrameGuarderRepSize >= nContainerSize) {
			escort_db::SqlContainer guarderRepContainer;
			memcpy_s(&guarderRepContainer, nContainerSize, pFrameGuarderRepData, nContainerSize);
			if (guarderRepContainer.uiResultCount && guarderRepContainer.uiResultLen
				&& guarderRepContainer.uiResultLen <= nFrameGuarderRepSize - nContainerSize) {
				guarderRepContainer.pStoreResult = (unsigned char *)zmalloc(
					guarderRepContainer.uiResultLen + 1);
				memcpy_s(guarderRepContainer.pStoreResult, guarderRepContainer.uiResultLen + 1,
					pFrameGuarderRepData + nContainerSize, guarderRepContainer.uiResultLen);
				guarderRepContainer.pStoreResult[guarderRepContainer.uiResultLen] = '\0';
			}
			if (guarderRepContainer.usSqlOptTarget == guarderContainer.usSqlOptTarget
				&& guarderRepContainer.usSqlOptType == guarderContainer.usSqlOptType
				&& guarderRepContainer.uiSqlOptSeq == guarderContainer.uiSqlOptSeq
				&& guarderRepContainer.ulSqlOptTime == guarderContainer.ulSqlOptTime) {
				size_t nGuarderSize = sizeof(Guarder);
				size_t nListLen = guarderRepContainer.uiResultCount * nGuarderSize;
				Guarder * pGuarderList = (Guarder *)zmalloc(nListLen);
				memcpy_s(pGuarderList, nListLen, guarderRepContainer.pStoreResult, nListLen);
				for (unsigned int i = 0; i < guarderRepContainer.uiResultCount; i++) {
					Guarder * pGuarder = (Guarder *)zmalloc(nGuarderSize);
					memcpy_s(pGuarder, nGuarderSize, &pGuarderList[i], nGuarderSize);
					pthread_mutex_lock(&g_mutex4GuarderList);
					zhash_update(g_guarderList, pGuarder->szId, pGuarder);
					zhash_freefn(g_guarderList, pGuarder->szId, free);
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}
				free(pGuarderList);
				pGuarderList = NULL;
			}
		}
		zframe_destroy(&frame_guarder_rep);
		zmsg_destroy(&msg_guarder_rep);
	} while (0);
	do {
		zmsg_t * msg_device_req = zmsg_new();
		zframe_t * frame_device_req = zframe_new(&deviceContainer, nContainerSize);
		zmsg_append(msg_device_req, &frame_device_req);
		zmsg_send(&msg_device_req, m_seeker);
		zmsg_t * msg_device_rep = zmsg_recv(m_seeker);
		if (!msg_device_rep) {
			break;
		}
		zframe_t * frame_device_rep = zmsg_pop(msg_device_rep);
		if (!frame_device_rep) {
			zmsg_destroy(&msg_device_rep);
			break;
		}
		size_t nFrameDeviceRepSize = zframe_size(frame_device_rep);
		unsigned char * pFrameDeviceRepData = zframe_data(frame_device_rep);
		if (pFrameDeviceRepData && nFrameDeviceRepSize >= nContainerSize) {
			escort_db::SqlContainer deviceContainerRep;
			memcpy_s(&deviceContainerRep, nContainerSize, pFrameDeviceRepData, nContainerSize);
			if (deviceContainerRep.uiResultLen && deviceContainerRep.uiResultCount
				&& deviceContainerRep.uiResultLen <= nFrameDeviceRepSize - nContainerSize) {
				deviceContainerRep.pStoreResult = (unsigned char *)zmalloc(deviceContainerRep.uiResultLen + 1);
				memcpy_s(deviceContainerRep.pStoreResult, deviceContainerRep.uiResultLen + 1,
					pFrameDeviceRepData + nContainerSize, deviceContainerRep.uiResultLen);
				deviceContainerRep.pStoreResult[deviceContainerRep.uiResultLen] = '\0';
			}
			if (deviceContainerRep.uiSqlOptSeq == deviceContainer.uiSqlOptSeq 
				&& deviceContainerRep.ulSqlOptTime == deviceContainer.ulSqlOptTime
				&& deviceContainerRep.usSqlOptTarget == deviceContainer.usSqlOptTarget
				&& deviceContainerRep.usSqlOptType == deviceContainer.usSqlOptType) {
				size_t nDeviceSize = sizeof(WristletDevice);
				size_t nListLen = nDeviceSize * deviceContainerRep.uiResultCount;
				WristletDevice * pDevList = (WristletDevice *)zmalloc(nListLen);
				memcpy_s(pDevList, nListLen, deviceContainerRep.pStoreResult, nListLen);
				for (unsigned int i = 0; i < deviceContainerRep.uiResultCount; i++) {
					WristletDevice * pDev = (WristletDevice *)zmalloc(nDeviceSize);
					memcpy_s(pDev, nDeviceSize, &pDevList[i], nDeviceSize);
					pthread_mutex_lock(&g_mutex4DevList);
					zhash_update(g_deviceList, pDev->deviceBasic.szDeviceId, pDev);
					zhash_freefn(g_deviceList, pDev->deviceBasic.szDeviceId, free);
					pthread_mutex_unlock(&g_mutex4DevList);
				}
				free(pDevList);
				pDevList = NULL;
			}
		}
		zframe_destroy(&frame_device_rep);
		zmsg_destroy(&msg_device_rep);
	} while (0);
	do {
		zmsg_t * msg_task_req = zmsg_new();
		zframe_t * frame_task_req = zframe_new(&taskContainer, nContainerSize);
		zmsg_append(msg_task_req, &frame_task_req);
		zmsg_send(&msg_task_req, m_seeker);
		zmsg_t * msg_task_rep = zmsg_recv(m_seeker);
		if (!msg_task_rep) {
			break;
		}
		zframe_t * frame_task_rep = zmsg_pop(msg_task_rep);
		if (!frame_task_rep) {
			zmsg_destroy(&msg_task_rep);
			break;
		}
		size_t nFramTaskRepDataLen = zframe_size(frame_task_rep);
		unsigned char * pFrameTaskRepData = zframe_data(frame_task_rep);
		if (pFrameTaskRepData && nFramTaskRepDataLen >= nContainerSize) {
			escort_db::SqlContainer taskRepContainer;
			memcpy_s(&taskRepContainer, nContainerSize, pFrameTaskRepData, nContainerSize);
			if (taskRepContainer.uiResultCount && taskRepContainer.uiResultLen) {
				taskRepContainer.pStoreResult = (unsigned char *)zmalloc(taskRepContainer.uiResultLen + 1);
				memcpy_s(taskRepContainer.pStoreResult, taskRepContainer.uiResultLen + 1,
					pFrameTaskRepData + nContainerSize, taskRepContainer.uiResultLen);
				taskRepContainer.pStoreResult[taskRepContainer.uiResultLen] = '\0';
			}
			if (taskRepContainer.uiSqlOptSeq == taskContainer.uiSqlOptSeq
				&& taskRepContainer.ulSqlOptTime == taskContainer.ulSqlOptTime
				&& taskRepContainer.usSqlOptTarget == taskContainer.usSqlOptTarget
				&& taskRepContainer.usSqlOptType == taskContainer.usSqlOptType) {
				size_t nTaskSize = sizeof(EscortTask);
				size_t nListLen = nTaskSize * taskRepContainer.uiResultCount;
				EscortTask * pTaskList = (EscortTask *)zmalloc(nListLen);
				memcpy_s(pTaskList, nListLen, taskRepContainer.pStoreResult, nListLen);
				for (unsigned int i = 0; i < taskRepContainer.uiResultCount; i++) {
					EscortTask * pTask = (EscortTask *)zmalloc(nTaskSize);
					memcpy_s(pTask, nTaskSize, &pTaskList[i], nTaskSize);
					pthread_mutex_lock(&g_mutex4TaskList);
					zhash_update(g_taskList, pTask->szTaskId, pTask);
					zhash_freefn(g_taskList, pTask->szTaskId, free);
					pthread_mutex_unlock(&g_mutex4TaskList);
				}
				free(pTaskList);
				pTaskList = NULL;
			}
		}
		zframe_destroy(&frame_task_rep);
		zmsg_destroy(&msg_task_rep);
	} while (0);
}

void * dealNetworkThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		pService->dealNetwork();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealLogThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		pService->dealLog();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealAppMsgThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		pService->dealAppMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealTopicMsgThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		pService->dealTopicMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealInteractionMsgThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		pService->dealInteractionMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * superviseThread(void * param)
{
	AccessService * pService = (AccessService *)param;
	if (pService) {
		zloop_start(pService->m_loop);
	}
	pthread_exit(NULL);
	return NULL;
}

int supervise(zloop_t * loop, int timer_id, void * arg)
{
	int result = 0;
	AccessService * pService = (AccessService *)arg;
	if (pService) {
		if (!pService->m_nRun) {
			result = -1;
		}
		else {
			if (pService->m_nTimerTickCount % 6 == 0) { //1min
				bool bActived = false;
				pthread_mutex_lock(&pService->m_mutex4RemoteLink);
				if (pService->m_remoteMsgSrvLink.nActive) {
					bActived = true;
				}
				pthread_mutex_unlock(&pService->m_mutex4RemoteLink);
				if (bActived) {
					unsigned long now = (unsigned long)time(NULL);
					char szDatetime[16] = { 0 };
					format_datetime(now, szDatetime, sizeof(szDatetime));
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
						"\"datetime\":\"%s\"}", MSG_SUB_ALIVE, pService->getNextRequestSequence(), szDatetime);
					if (strlen(szMsg)) {
						pService->sendDataViaInteractor(szMsg, strlen(szMsg));
					}
				}
			}
			else if (pService->m_nTimerTickCount % 18 == 0) { //3min
				pthread_mutex_lock(&pService->m_mutex4RemoteLink);
				if (pService->m_remoteMsgSrvLink.nActive) {
					time_t now = time(NULL);
					time_t lastActiveTime = pService->m_remoteMsgSrvLink.ulLastActiveTime;
					if (difftime(now, lastActiveTime) > 360.00) { //6min
						pService->m_remoteMsgSrvLink.nActive = 0;
					}
				}
				pthread_mutex_unlock(&pService->m_mutex4RemoteLink);
			}
			pService->m_nTimerTickCount++;
			if (pService->m_nTimerTickCount == 360) {//reset when 1 hour
				pService->m_nTimerTickCount = 0;
			}
		}
	}
	return result;
}

