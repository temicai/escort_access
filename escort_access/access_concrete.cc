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

#define MAKE_APPMSGHEAD(x) {x.marker[0] = 'E'; x.marker[1] = 'C'; x.version[0] = '1'; x.version[1] = '0';}

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

std::string Utf8ToAnsi(LPCSTR utf8)
{
	int WLength = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, NULL);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_UTF8, 0, utf8, -1, pszW, WLength);
	pszW[WLength] = '\0';
	int ALength = WideCharToMultiByte(CP_ACP, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca((ALength + 1) * sizeof(char));
	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
	WideCharToMultiByte(CP_ACP, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr = pszA;
	return retStr;
}

std::string AnsiToUtf8(LPCSTR Ansi)
{
	int WLength = MultiByteToWideChar(CP_ACP, 0, Ansi, -1, NULL, 0);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_ACP, 0, Ansi, -1, pszW, WLength);
	int ALength = WideCharToMultiByte(CP_UTF8, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca(ALength + 1);
	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
	WideCharToMultiByte(CP_UTF8, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr(pszA);
	return retStr;
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
				pLinkData->szUser[0] = '\0';

				pthread_mutex_lock(&pService->m_mutex4LinkDataList);
				pService->m_linkDataList.insert(std::make_pair(strLink, pLinkData));
				pthread_mutex_unlock(&pService->m_mutex4LinkDataList);
				char szLog[256] = { 0 };
				snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]link=%s connect\r\n", __FUNCTION__, __LINE__, 
					strLink.c_str());
				pService->writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, pService->m_usLogType);
				break;
			}
			case MSG_LINK_DISCONNECT: {
				const char * pStrLink = (char *)pMsg;
				pService->handleLinkDisconnect(pStrLink, NULL, true);
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
			if (pInst) {
				pInst->m_bConnectZk = true;
			}
		}
		else if (state == ZOO_EXPIRED_SESSION_STATE) {
			if (pInst) {
				pInst->m_bConnectZk = false;
				zookeeper_close(pInst->m_zkHandle);
				pInst->m_zkHandle = NULL;
				pInst->initZookeeper();
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
				if (!pService->getLoadSessionFlag()) {
					pService->loadSessionList();
				}
			}
			break;
		}
		case ZNODEEXISTS: {
			if (pService && pService->m_nRun) {
				pService->masterExist();
				pService->runAsSlaver();
				pService->setLoadSessionFlag(true);
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
				if (!pService->getLoadSessionFlag()) {
					pService->loadSessionList();
				}
			}
			break;
		}
	}
}

void zk_session_create_completion(int rc, const char * name, const void * data)
{
	AccessService * pInst = (AccessService *)data;
	if (rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT) {
		if (pInst && pInst->m_nRun) {
			zoo_acreate(pInst->m_zkHandle, "/escort/session", NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0,
				zk_session_create_completion, data);
		}
	}
}

void zk_session_get_children_watcher(zhandle_t * zh, int type, int state, const char * path,
	void * watcherCtx)
{

}

void zk_session_child_delete_completion(int rc, const void * data)
{
	ZKSessionNode * pSessionNode = (ZKSessionNode *)data;
	if (rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT) {
		if (pSessionNode) {
			AccessService * pService = (AccessService *)pSessionNode->data;
			if (pService && pService->m_nRun) {
				if (pService->m_zkHandle) {
					char szSessionFullPath[128] = { 0 };
					snprintf(szSessionFullPath, sizeof(szSessionFullPath), "/escort/session/%s", szSessionFullPath);
					zoo_adelete(pService->m_zkHandle, szSessionFullPath, -1, zk_session_child_delete_completion, data);
				}
			}
		}
	}
}

void zk_session_child_create_completion(int rc, const char * name, const void * data)
{
	ZKSessionNode * pNode = (ZKSessionNode *)data;
	if (rc == ZOPERATIONTIMEOUT || rc == ZCONNECTIONLOSS) {
		AccessService * pService = (AccessService *)pNode->data;
		if (pService && pService->m_nRun) {
			char szBuf[1024] = { 0 };
			access_service::AppLinkInfo * pLinkInfo = NULL;
			size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
			pthread_mutex_lock(&pService->m_mutex4LinkList);
			if (zhash_size(pService->m_linkList)) {
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(pService->m_linkList, 
					pNode->szSession);
				if (pLink) {
					pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
					memcpy_s(pLink, nLinkInfoSize, pLinkInfo, nLinkInfoSize);
				}
			}
			pthread_mutex_unlock(&pService->m_mutex4LinkList);
			if (pLinkInfo) {
				if (pService->m_zkHandle) {
					char szFullPath[128] = { 0 };
					sprintf_s(szFullPath, sizeof(szFullPath), "/escort/session/%s", pNode->szSession);
					const char * pBuf = szBuf;
					zoo_acreate(pService->m_zkHandle, szFullPath, pBuf, 1024, &ZOO_OPEN_ACL_UNSAFE, 0,
						zk_session_child_create_completion, data);
				}
				free(pLinkInfo);
				pLinkInfo = NULL;
			}
		}
	}
}

void zk_session_child_set_completion(int rc, const struct Stat * stat, const void * data)
{
	ZKSessionNode * pNode = (ZKSessionNode *)data;
	if (rc == ZOPERATIONTIMEOUT || rc == ZCONNECTIONLOSS) {
		if (pNode) {
			AccessService * pService = (AccessService *)pNode->data;
			if (pService && pService->m_nRun && pService->m_zkHandle) {
				access_service::AppLinkInfo * pLinkInfo = NULL;
				size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
				pthread_mutex_lock(&pService->m_mutex4LinkList);
				if (zhash_size(pService->m_linkList)) {
					access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
						pService->m_linkList, pNode->szSession);
					if (pLink) {
						pLinkInfo = new access_service::AppLinkInfo();
						memcpy_s(pLinkInfo, nLinkInfoSize, pLink, nLinkInfoSize);
					}
				}
				pthread_mutex_unlock(&pService->m_mutex4LinkList);
				if (pLinkInfo) {
					char szBuf[1024] = { 0 };
					memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
					const char * pBuf = szBuf;
					char szPath[128] = { 0 };
					sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", pNode->szSession);
					zoo_aset(pService->m_zkHandle, szPath, pBuf, 1024, -1, zk_session_child_set_completion, data);
					delete pLinkInfo;
					pLinkInfo = NULL;
				}
			}
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
	pthread_mutex_init(&m_mutex4LoadSession, NULL);
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
	m_seeker = NULL;
	m_szInteractorIdentity[0] = '\0';
	m_szSeekerIdentity[0] = '\0';

	m_bLoadSession = false;

	m_linkList = zhash_new();
	m_subscribeList = zhash_new();
	m_localTopicMsgList = zlist_new();

	m_szHost[0] = '\0';
	m_szLogRoot[0] = '\0';
	m_uiLogInst = 0;
	m_usLogType = pf_logger::eLOGTYPE_FILE;

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
	if (m_uiLogInst) {
		LOG_Release(m_uiLogInst);
		m_uiLogInst = 0;
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
	pthread_mutex_destroy(&m_mutex4LoadSession);
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
	strcpy_s(m_szInteractorIdentity, sizeof(m_szInteractorIdentity), szUuid);
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
	unsigned int uiSrvInst = TS_StartServer(usServicePort, (int)m_usLogType, fMsgCb, this);
	if (uiSrvInst > 0) {
		m_bLoadSession = false;
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
		char szDatetime[20] = { 0 };
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

		if (g_bInitBuffer == FALSE) {
			readDataBuffer();
			g_bInitBuffer = TRUE;
		}

		loadSessionList();
		competeForMaster();

		char szLog[256] = { 0 };
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]access service start %u\r\n", __FUNCTION__, 
			__LINE__, usServicePort);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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

	m_szInteractorIdentity[0] = '\0';
	m_szSeekerIdentity[0] = '\0';

	//删除zookeeper自身节点
	if (m_bConnectZk) {
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		m_zkNodePath[0] = '\0';
		m_bConnectZk = false;
	}
	return E_OK;
}

void AccessService::SetLogType(unsigned short usLogType)
{
	if (m_uiLogInst) {
		if (m_usLogType != usLogType) {
			pf_logger::LogConfig logConf;
			LOG_GetConfig(m_uiLogInst, &logConf);
			if (logConf.usLogType != usLogType) {
				logConf.usLogType = usLogType;
				LOG_SetConfig(m_uiLogInst, logConf);
				m_usLogType = usLogType;
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
	if (m_uiLogInst == 0) {
		m_uiLogInst = LOG_Init();
		if (m_uiLogInst) {
			pf_logger::LogConfig logInfo;
			logInfo.usLogType = m_usLogType;
			char szLogDir[256] = { 0 };
			snprintf(szLogDir, 256, "%slog\\", m_szLogRoot);
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strcat_s(szLogDir, 256, "escort_accesss\\");
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strncpy_s(logInfo.szLogPath, sizeof(logInfo.szLogPath), szLogDir, strlen(szLogDir));
			LOG_SetConfig(m_uiLogInst, logInfo);
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

void AccessService::writeLog(const char * pLogContent, unsigned short usLogCategoryType, 
	unsigned short usLogType)
{
	if (pLogContent && strlen(pLogContent)) {
		access_service::LogContext * pLog = new access_service::LogContext();
		pLog->usLogCategory = usLogCategoryType;
		pLog->usLogType = usLogType;
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
				if (m_uiLogInst) {
					LOG_Log(m_uiLogInst, pLog->pLogData, pLog->usLogCategory, pLog->usLogType);
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
			writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
					std::string ansiStr = Utf8ToAnsi(pContent);
					size_t nAnsiStrLen = ansiStr.size();
					char * pContent2 = (char *)malloc(nAnsiStrLen + 1);
					memcpy_s(pContent2, nAnsiStrLen, ansiStr.c_str(), nAnsiStrLen);
					pContent2[nAnsiStrLen] = '\0';
					//printf("pContent: %s\n", pContent);
					printf("pContent2: %s\n", pContent2);
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]pContent2=%s\r\n", __FUNCTION__,
						__LINE__, pContent2);
					writeLog(szLog, pf_logger::eLOGCATEGORY_DEFAULT, m_usLogType);
					rapidjson::Document doc;
					if (!doc.Parse(pContent2).HasParseError()) {
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
										if (nSize) {
											strncpy_s(loginInfo.szUser, sizeof(loginInfo.szUser), doc["account"].GetString(), nSize);
											bValidAccount = true;
										}
									}
									if (doc["passwd"].IsString()) {
										size_t nSize = doc["passwd"].GetStringLength();
										if (nSize) {
											strncpy_s(loginInfo.szPasswd, sizeof(loginInfo.szPasswd), doc["passwd"].GetString(), nSize);
											bValidPasswd = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(loginInfo.szDateTime, sizeof(loginInfo.szDateTime), 
												doc["datetime"].GetString(), nSize);
											bValidDatetime = true;
										}
									}
									if (doc.HasMember("handset")) {
										if (doc["handset"].IsString()) {
											size_t nSize = doc["handset"].GetStringLength();
											if (nSize) {
												strncpy_s(loginInfo.szHandset, sizeof(loginInfo.szHandset), doc["handset"].GetString(), 
													nSize);
											}
										}
									}
									if (bValidAccount && bValidPasswd && bValidDatetime) {
										loginInfo.uiReqSeq = 0;	//loginInfo.uiReqSeq = getNextRequestSequence();
										handleAppLoginEx(loginInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, one or more"
											" parameter needed, account=%s, passwd=%s, datetime=%s, handset=%s\r\n", __FUNCTION__, 
											__LINE__, pMsg->szEndPoint, bValidAccount ? loginInfo.szUser : "null", 
											bValidPasswd ? loginInfo.szPasswd : "null", bValidDatetime ? loginInfo.szDateTime : "null", 
											loginInfo.szHandset);
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = {};
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
											"\"taskInfo\":[]}", access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskInfo\":[]}", access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? logoutInfo.szSession : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"battery\":0}",
											access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER, bValidSession ? bindInfo.szSesssion : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"battery\":0}",
										access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? bindInfo.szSesssion : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
									bool bValidTarget = false;
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
										if (nSize) {
											size_t nFieldSize = sizeof(taskInfo.szDestination);
											strncpy_s(taskInfo.szDestination, nFieldSize, doc["destination"].GetString(), 
												(nSize < nFieldSize) ? nSize : nFieldSize - 1);
										}
									}
									if (doc["target"].GetString()) {
										size_t nSize = doc["target"].GetStringLength();
										if (nSize) {
											size_t nFieldSize = sizeof(taskInfo.szTarget);
											strncpy_s(taskInfo.szTarget, nFieldSize, doc["target"].GetString(),
												(nSize < nFieldSize) ? nSize : nFieldSize - 1);
											if (taskInfo.szTarget[0] != '&') {
												bValidTarget = true;
											}
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
									if (bValidSession && bValidType && bValidLimit && bValidDatetime && bValidTarget) {
										taskInfo.uiReqSeq = getNextRequestSequence();
										handleAppSubmitTask(taskInfo, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, one or more"
											" parameter needed, session=%s, type=%d, limit=%d, target=%s, datetime=%s\r\n", __FUNCTION__, 
											__LINE__, pMsg->szEndPoint, bValidSession ? taskInfo.szSession : "null",
											bValidType ? taskInfo.usTaskType : -1, bValidLimit ? taskInfo.usTaskLimit : -1,
											bValidTarget ? taskInfo.szTarget : "null", bValidDatetime ? taskInfo.szDatetime : "null");
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":"
											"\"\"}", access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER,
											bValidSession ? taskInfo.szSession : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\":\"\"}",
										access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\""
											":\"%s\"}", access_service::E_CMD_TAKK_CLOSE_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? taskInfo.szSession : "", bValidTask ? taskInfo.szTaskId : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\""
										":\"\"}", access_service::E_CMD_TAKK_CLOSE_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"%s\"}", access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? fleeInfo.szSession : "", bValidTask ? fleeInfo.szTaskId : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, JSON data "
										"format error=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, pMsg->pMsgData);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\""
										":\"\"}", access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\""
											":\"%s\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER,
											bValidSession ? fleeInfo.szSession : "", bValidTask ? fleeInfo.szTaskId : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s, JSON data"
										" format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\""
										":\"\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, JSON data format "
										"error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_MODIFY_PASSWD: {
								if (doc.HasMember("session") && doc.HasMember("currPasswd") && doc.HasMember("newPasswd")
									&& doc.HasMember("datetime")) {
									access_service::AppModifyPassword modifyPasswd;
									bool bValidSession = false;
									bool bValidCurrPasswd = false;
									bool bValidNewPasswd = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strncpy_s(modifyPasswd.szSession, sizeof(modifyPasswd.szSession),
												doc["session"].GetString(), nSize);
											bValidSession = true;
										}
									}
									if (doc["currPasswd"].IsString()) {
										size_t nSize = doc["currPasswd"].GetStringLength();
										if (nSize) {
											strncpy_s(modifyPasswd.szCurrPassword, sizeof(modifyPasswd.szCurrPassword),
												doc["currPasswd"].GetString(), nSize);
											bValidCurrPasswd = true;
										}
									}
									if (doc["newPasswd"].IsString()) {
										size_t nSize = doc["newPasswd"].GetStringLength();
										if (nSize) {
											strncpy_s(modifyPasswd.szNewPassword, sizeof(modifyPasswd.szNewPassword),
												doc["newPasswd"].GetString(), nSize);
											bValidNewPasswd = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(modifyPasswd.szDatetime, sizeof(modifyPasswd.szDatetime),
												doc["datetime"].GetString(), nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidCurrPasswd && bValidNewPasswd && bValidDatetime) {
										modifyPasswd.uiSeq = getNextRequestSequence();
										handleAppModifyAccountPassword(modifyPasswd, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, one or more "
											"parameter needed, session=%s, currPasswd=%s, newPasswd=%s, datetime=%s\r\n",
											__FUNCTION__, __LINE__, pMsg->szEndPoint, bValidSession ? modifyPasswd.szSession : "null",
											bValidCurrPasswd ? modifyPasswd.szCurrPassword : "null",
											bValidNewPasswd ? modifyPasswd.szNewPassword : "null",
											bValidDatetime ? modifyPasswd.szDatetime : "null");
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\""
											":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, E_INVALIDPARAMETER,
											bValidSession ? modifyPasswd.szSession : "", bValidDatetime ? modifyPasswd.szDatetime : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]modify password from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\",\"retcode\":%d,\"datetime\":"
										"\"\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("datetime")) {
									access_service::AppQueryTask queryTask;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strncpy_s(queryTask.szSession, sizeof(queryTask.szSession), doc["session"].GetString(),
												nSize);
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(queryTask.szTaskId, sizeof(queryTask.szTaskId), doc["taskId"].GetString(),
												nSize);
											bValidTask = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(queryTask.szDatetime, sizeof(queryTask.szDatetime), doc["datetime"].GetString(),
												nSize);
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidDatetime) {
										queryTask.uiQuerySeq = getNextRequestSequence();
										handleAppQueryTask(queryTask, pMsg->szEndPoint, pMsg->ulMsgTime);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, one or more param"
											"eter miss, session=%s, task=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint,
											bValidSession ? queryTask.szSession : "null", bValidTask ? queryTask.szTaskId : "null",
											bValidDatetime ? queryTask.szDatetime : "null");
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\""
											":\"%s\",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, E_INVALIDPARAMETER,
											bValidSession ? queryTask.szSession : "", bValidDatetime ? queryTask.szDatetime : "");
										sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
									}
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, JSON data format "
										"error=%d\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, doc.GetParseError());
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"datetime\":\"\""
										",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
								}
								break;
							}
							case access_service::E_CMD_DEVICE_COMMAND: {
								if (doc.HasMember("session") && doc.HasMember("deviceId") && doc.HasMember("seq")
									&& doc.HasMember("datetime") && doc.HasMember("param1") && doc.HasMember("param2")) {
									access_service::AppDeviceCommandInfo devCmdInfo;
									memset(&devCmdInfo, 0, sizeof(devCmdInfo));
									sprintf_s(devCmdInfo.szFactoryId, sizeof(devCmdInfo.szFactoryId), "01");
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strncpy_s(devCmdInfo.szSession, sizeof(devCmdInfo.szSession),
												doc["session"].GetString(), nSize);
										}
									}
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(devCmdInfo.szDeviceId, sizeof(devCmdInfo.szDeviceId),
												doc["deviceId"].GetString(), nSize);
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(devCmdInfo.szDatetime, sizeof(devCmdInfo.szDatetime),
												doc["datetime"].GetString(), nSize);
										}
									}
									if (doc["seq"].IsInt()) {
										devCmdInfo.nSeq = doc["seq"].GetInt();
									}
									if (doc["param1"].IsInt()) {
										devCmdInfo.nParam1 = doc["param1"].GetInt();
									}
									if (doc["param2"].IsInt()) {
										devCmdInfo.nParam2 = doc["param2"].GetInt();
									}
									handleAppDeviceCommand(devCmdInfo, pMsg->szEndPoint);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, JSON data "
										"format error=%d\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, doc.GetParseError());
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"device\""
										":\"\",\"seq\":0,\"datetime\":\"\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY,
										E_INVALIDPARAMETER);
									sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]can't recognise command: %d\r\n", 
									__FUNCTION__, __LINE__, nCmd);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								char szReply[256] = { 0 };
								snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}", 
									access_service::E_CMD_DEFAULT_REPLY, E_INVALIDCOMMAND);
								sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]can't parse JSON content: %s\r\n", 
							__FUNCTION__, __LINE__, pContent);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						char szReply[256] = { 0 };
						snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}", 
							access_service::E_CMD_DEFAULT_REPLY, E_INVALIDCOMMAND);
						sendDatatoEndpoint(szReply, strlen(szReply), pMsg->szEndPoint);
					}
					free(pContent);
					pContent = NULL;
					free(pContent2);
					pContent2 = NULL;
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
//condition:  c1-check guarder(user) wether "login" already | consider the matter <guarder(user) re-login>,
//how to handle check passwd
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
	char szReply[1024] = { 0 };
	char szTaskInfo[512] = { 0 };
	EscortTask * pCurrTask = NULL;
	unsigned short usDeviceStatus = 0;
	unsigned char ucDeviceBattery = 0;
	unsigned char ucDeviceLooseState = 0;
	bool bReLogin = false;
	//1-1.check from g_guarderList, 1-2.check from db-prox
	bool bNeedReloadData = false;
	pthread_mutex_lock(&g_mutex4GuarderList);
	if (zhash_size(g_guarderList)) {
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo.szUser);
		if (pGuarder) {
			if ((pGuarder->usRoleType & USER_ROLE_OPERATOR) != 0) {
				bFindGuarder = true;
				if (strlen(pGuarder->szCurrentSession) > 0) { //session exists, account in use
					if (strlen(pGuarder->szLink) > 0) {
						if (strcmp(pGuarder->szLink, pEndPoint) == 0) {
							if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
								bReLogin = true;
								strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession,
									strlen(pGuarder->szCurrentSession));
								if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
									strncpy_s(szCurrTaskId, sizeof(szCurrTaskId), pGuarder->szTaskId,
										strlen(pGuarder->szTaskId));
									bHaveTask = true;
								}
							}
							else {
								nErr = E_INVALIDPASSWD;
								snprintf(szLog, sizeof(szLog), "user=%s, link=%s, incorrected password\n",
									loginInfo.szUser, pEndPoint);
								printf(szLog);
							}
						}
						else {
							if (verifyLink(pGuarder->szLink)) {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, already have link=%s in use,"
									" from=%s\r\n", __FUNCTION__, __LINE__, loginInfo.szUser, pGuarder->szLink, pEndPoint);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								nErr = E_ACCOUNTINUSE;
							}
							else {
								TS_CloseEndpoint(m_uiSrvInst, pGuarder->szLink);
								if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
									bReLogin = true;
									strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession,
										strlen(pGuarder->szCurrentSession));
									if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
										strncpy_s(szCurrTaskId, sizeof(szCurrTaskId), pGuarder->szTaskId,
											strlen(pGuarder->szTaskId));
										bHaveTask = true;
									}
									strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint);
								}
								else {
									nErr = E_INVALIDPASSWD;
									snprintf(szLog, sizeof(szLog), "user=%s, link=%s, incorrected password\n",
										loginInfo.szUser, pEndPoint);
									printf(szLog);
								}
							}
						}
					}
					else { //link empty
						if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							bReLogin = true;
							strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession,
								strlen(pGuarder->szCurrentSession));
							if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
								strncpy_s(szCurrTaskId, sizeof(szCurrTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
								bHaveTask = true;
							}
							strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
						}
						else {
							snprintf(szLog, sizeof(szLog), "user=%s, link=%s, incorrected password\n",
								loginInfo.szUser, pEndPoint);
							printf(szLog);
							nErr = E_INVALIDPASSWD;
						}
					}
				} 
				else {  //not exists session
					if (pGuarder->usState == STATE_GUARDER_DEACTIVATE) {
						nErr = E_INVALIDACCOUNT;
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s deactivate, from=%s\r\n",
							__FUNCTION__, __LINE__, loginInfo.szUser, pEndPoint);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					else {
						//check passwd
						if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
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
			else {
				nErr = E_INVALIDACCOUNT;
			}
		}
		else {
			nErr = E_INVALIDACCOUNT;
		}
	}
	else {
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not load guarderlist\r\n", __FUNCTION__, __LINE__);
		writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
		nErr = E_DEFAULTERROR;
		bNeedReloadData = true;
	}
	pthread_mutex_unlock(&g_mutex4GuarderList);
	//1-2.
	//if (!bFindGuarder) {
	//	//query from db-buffer,real kill time
	//	size_t nContainerSize = sizeof(escort_db::SqlContainer);
	//	do {
	//		nErr = E_INVALIDACCOUNT;
	//		escort_db::SqlContainer reqContainer;
	//		reqContainer.uiSqlOptSeq = getNextRequestSequence();
	//		reqContainer.ulSqlOptTime = (unsigned long)time(NULL);
	//		reqContainer.usSqlOptTarget = escort_db::E_TBL_GUARDER;
	//		reqContainer.usSqlOptType = escort_db::E_OPT_QUERY;
	//		reqContainer.uiResultCount = 0;
	//		reqContainer.uiResultLen = 0;
	//		reqContainer.pStoreResult = NULL;
	//		strncpy_s(reqContainer.szSqlOptKey, sizeof(reqContainer.szSqlOptKey), loginInfo.szUser,
	//			strlen(loginInfo.szUser));
	//		zframe_t * frame_req = zframe_new(&reqContainer, nContainerSize);
	//		zmsg_t * msg_req = zmsg_new();
	//		zmsg_append(msg_req, &frame_req);
	//		zmsg_send(&msg_req, m_seeker);
	//		zmsg_t * msg_rep = zmsg_recv(m_seeker);
	//		if (!msg_rep) {
	//			break;
	//		}
	//		zframe_t * frame_rep = zmsg_pop(msg_rep);
	//		if (!frame_rep) {
	//			zmsg_destroy(&msg_rep);
	//			break;
	//		}
	//		size_t nFrameDataLen = zframe_size(frame_rep);
	//		unsigned char * pFrameData = zframe_data(frame_rep);
	//		if (pFrameData && nFrameDataLen && nFrameDataLen >= nContainerSize) {
	//			escort_db::SqlContainer repContainer;
	//			memcpy_s(&repContainer, nContainerSize, pFrameData, nContainerSize);
	//			if (repContainer.uiResultCount == 1 && repContainer.uiResultLen) {
	//				repContainer.pStoreResult = (unsigned char *)zmalloc(repContainer.uiResultLen + 1);
	//				memcpy_s(repContainer.pStoreResult, repContainer.uiResultLen + 1,
	//					pFrameData + nContainerSize, repContainer.uiResultLen);
	//				repContainer.pStoreResult[repContainer.uiResultLen] = '\0';
	//			}
	//			if (repContainer.uiSqlOptSeq == reqContainer.uiSqlOptSeq
	//				&& repContainer.ulSqlOptTime == reqContainer.ulSqlOptTime
	//				&& repContainer.usSqlOptTarget == reqContainer.usSqlOptTarget
	//				&& repContainer.usSqlOptType == reqContainer.usSqlOptType) {
	//				size_t nGuarderSize = sizeof(Guarder);
	//				Guarder * pGuarder = (Guarder *)zmalloc(nGuarderSize);
	//				memcpy_s(pGuarder, nGuarderSize, repContainer.pStoreResult, nGuarderSize);
	//				if (strcmp(pGuarder->szId, loginInfo.szUser) == 0) {
	//					if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
	//						pGuarder->usState = STATE_GUARDER_FREE;
	//						generateSession(szSession, sizeof(szSession));
	//						if (strlen(szSession)) {
	//							strncpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession),
	//								szSession, strlen(szSession));
	//						}
	//						strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
	//						nErr = E_OK;
	//					}
	//					else {
	//						nErr = E_INVALIDPASSWD;
	//					}
	//					pthread_mutex_lock(&g_mutex4GuarderList);
	//					zhash_update(g_guarderList, pGuarder->szId, pGuarder);
	//					zhash_freefn(g_guarderList, pGuarder->szId, free);
	//					pthread_mutex_unlock(&g_mutex4GuarderList);
	//				}
	//				free(pGuarder);
	//				pGuarder = NULL;
	//			}
	//		}
	//		zframe_destroy(&frame_rep);
	//		zmsg_destroy(&msg_rep);
	//	} while (0);
	//}
	//2.give reply for login request
	if (bHaveTask) {
		//2-1.get task information
		char szDevKey[20] = { 0 };
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrTaskId);
			if (pTask) {
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
					ucDeviceBattery = pDev->deviceBasic.nBattery;
					ucDeviceLooseState = pDev->deviceBasic.nLooseStatus; 
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
		if (!bHaveTask) {//login ok, without linger task
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[]}",
				access_service::E_CMD_LOGIN_REPLY, szSession);
		}
		else {//login ok, with linger task
			if (pCurrTask) {
				snprintf(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,"
					"\"limit\":%d,\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%u,"
					"\"deviceState\":%u, \"handset\":\"%s\"}", pCurrTask->szTaskId, pCurrTask->szDeviceId, 
					pCurrTask->nTaskType + 1, pCurrTask->nTaskLimitDistance, pCurrTask->szDestination, 
					pCurrTask->szTarget, pCurrTask->szTaskStartTime, ucDeviceBattery, usDeviceStatus, loginInfo.szHandset);
				snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}",
					access_service::E_CMD_LOGIN_REPLY, szSession, szTaskInfo);
			}
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
		if (strlen(loginInfo.szHandset) > 0) {
			strncpy_s(pLink->szHandset, sizeof(pLink->szHandset), loginInfo.szHandset, strlen(loginInfo.szHandset));
		}
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

		if (bHaveTask) {
			bool bModifyTask = false;
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrTaskId);
				if (pTask) {
					if (pTask->nTaskMode == 0) {
						if (strlen(loginInfo.szHandset)) {
							pTask->nTaskMode = 1;
							strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset, 
								strlen(loginInfo.szHandset));
							bModifyTask = true;
						}
					}
					else if (pTask->nTaskMode == 1) {
						if (strlen(loginInfo.szHandset)) { //current login with handset
							if (strlen(pTask->szHandset)) { //task last handset is not empty
								if (strcmp(pTask->szHandset, loginInfo.szHandset) != 0) { //change task handset
									bModifyTask = true;
									strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset, 
										strlen(loginInfo.szHandset));
								}
							}
							else { //task last handset is empty
								bModifyTask = true;
								strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset, strlen(loginInfo.szHandset));
							}
						}
						else { //current login without handset
							if (strlen(pTask->szHandset)) { //if task has handset, switch to no handset
								bModifyTask = true;
								pTask->szHandset[0] = '\0';
							}
						}
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
			if (bModifyTask) {
				char szBody[512] = { 0 };
				snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"handset\":\"%s\"}]}",
					MSG_SUB_REPORT, loginInfo.uiReqSeq, loginInfo.szDateTime, SUB_REPORT_TASK_MODIFY, 
					pCurrTask->szTaskId, loginInfo.szHandset);
				sendDataViaInteractor(szBody, strlen(szBody));
			}
		}
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
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]add new subscriber for %s, endpoint=%s\r\n",
			__FUNCTION__, __LINE__, szTopic, pEndPoint);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		free(pCurrTask);
		pCurrTask = NULL;
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, datetime=%s, handset=%s, result=%d,"
		" session=%s, from=%s\r\n", __FUNCTION__, __LINE__, loginInfo.szUser, loginInfo.szDateTime, 
		loginInfo.szHandset, nErr, szSession, pEndPoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	if (bNeedReloadData) {
		readDataBuffer();
	}
}

void AccessService::handleAppLoginEx(access_service::AppLoginInfo loginInfo, const char * pEndpoint,
	unsigned long ulTime)
{
	char szLog[512] = { 0 };
	if (!getLoadSessionFlag()) {
		char szReply[256] = { 0 };
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, E_DEFAULTERROR);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login request received from %s, user=%s, server not"
			" ready yet, wait load session information\r\n", __FUNCTION__, __LINE__, pEndpoint, loginInfo.szUser);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	int nErr = E_OK;
	bool bNeedReloadData = false;
	bool bHaveTask = false;
	char szCurrentTaskId[20] = { 0 };
	char szCurrentBindDeviceId[20] = { 0 };
	char szCurrentSession[20] = { 0 };
	char szLastestSession[20] = { 0 };
	bool bNeedGenerateSession = false; 
	unsigned short usDeviceBattery = 0;
	unsigned short usDeviceStatus = 0;
	pthread_mutex_lock(&g_mutex4GuarderList);
	if (zhash_size(g_guarderList)) {
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo.szUser);
		if (pGuarder) {
			if ((pGuarder->usRoleType & USER_ROLE_OPERATOR) != 0) {
				if (strlen(pGuarder->szCurrentSession) > 0) {
					strncpy_s(szCurrentSession, sizeof(szCurrentSession), pGuarder->szCurrentSession,
						strlen(pGuarder->szCurrentSession));
					if (strlen(pGuarder->szLink) > 0) {
						if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, already have link=%s"
								" in user, from=%s\r\n", __FUNCTION__, __LINE__, loginInfo.szUser, pGuarder->szLink,
								pEndpoint);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							nErr = E_ACCOUNTINUSE;
							//if (verifyLink(pGuarder->szLink)) {
							//	nErr = E_ACCOUNTINUSE;
							//}
							//else {
							//	if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							//		if (pGuarder->usState == STATE_GUARDER_BIND) {
							//			if (strlen(pGuarder->szBindDevice)) {
							//				strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
							//					strlen(pGuarder->szBindDevice));
							//			}
							//		}
							//		else if (pGuarder->usState == STATE_GUARDER_DUTY) {
							//			if (strlen(pGuarder->szTaskId)) {
							//				strncpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId, 
							//					strlen(pGuarder->szTaskId));
							//				bHaveTask = true;
							//			}
							//			if (strlen(pGuarder->szBindDevice)) {
							//				strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
							//					strlen(pGuarder->szBindDevice));
							//			}
							//		}
							//		bNeedGenerateSession = true;
							//	}
							//	else {
							//		nErr = E_INVALIDPASSWD;
							//	}
							//}
						}
						else { //same link
							if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
								if (pGuarder->usState == STATE_GUARDER_BIND) {
									if (strlen(pGuarder->szBindDevice)) {
										strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
											strlen(pGuarder->szBindDevice));
									}
								}
								else if (pGuarder->usState == STATE_GUARDER_DUTY) {
									if (strlen(pGuarder->szTaskId) > 0) {
										strncpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId, 
											strlen(pGuarder->szTaskId));
										bHaveTask = true;
									}
									if (strlen(pGuarder->szBindDevice)) {
										strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
											strlen(pGuarder->szBindDevice));
									}
								}
								bNeedGenerateSession = false;
							}
							else {
								nErr = E_INVALIDPASSWD;
							}
						}
					}
					else { //link disconnect
						if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							if (pGuarder->usState == STATE_GUARDER_BIND) {
								if (strlen(pGuarder->szBindDevice)) {
									strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
										strlen(pGuarder->szBindDevice));
								}
							}
							else if (pGuarder->usState == STATE_GUARDER_DUTY) {
								if (strlen(pGuarder->szTaskId)) {
									strncpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId,
										strlen(pGuarder->szTaskId));
									bHaveTask = true;
								}
								if (strlen(pGuarder->szBindDevice)) {
									strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
										strlen(pGuarder->szBindDevice));
								}
							}
							bNeedGenerateSession = true;
						}
						else {
							nErr = E_INVALIDPASSWD;
						}
					}
				}
				else { //no session
					if (pGuarder->usState == STATE_GUARDER_DEACTIVATE) {
						nErr = E_UNAUTHORIZEDACCOUNT;
					}
					else {
						if (strcmp(pGuarder->szPassword, loginInfo.szPasswd) == 0) {
							if (pGuarder->usState == STATE_GUARDER_BIND) {
								strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
									strlen(pGuarder->szBindDevice));
							}
							else if (pGuarder->usState == STATE_GUARDER_DUTY) {
								if (strlen(pGuarder->szTaskId) > 0) {
									strncpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId, 
										strlen(pGuarder->szTaskId));
									strncpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice,
										strlen(pGuarder->szBindDevice));
									bHaveTask = true;
								}
							}
							bNeedGenerateSession = true;
						}
						else {
							nErr = E_INVALIDPASSWD;
						}
					}
				}
			}
			else {
				nErr = E_UNAUTHORIZEDACCOUNT;
			}
		}
		else {
			nErr = E_INVALIDACCOUNT;
		}
	}
	else {
		nErr = E_DEFAULTERROR;
		bNeedReloadData = true;
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]guarder list is empty, need reload data\r\n",
			__FUNCTION__, __LINE__);
		writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
	}
	pthread_mutex_unlock(&g_mutex4GuarderList);
	if (nErr == E_OK) {
		char szFactoryId[4] = { 0 };
		char szOrg[40] = { 0 };
		char szTaskInfo[512] = { 0 };
		char szReply[1024] = { 0 };
		if (bNeedGenerateSession) {
			generateSession(szLastestSession, sizeof(szLastestSession));
		}
		else {
			if (strlen(szCurrentSession)) {
				strncpy_s(szLastestSession, sizeof(szLastestSession), szCurrentSession, strlen(szCurrentSession));
			}
		}
		if (strlen(szCurrentBindDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szCurrentBindDeviceId);
				if (pDevice) {
					usDeviceStatus = pDevice->deviceBasic.nStatus;
					usDeviceBattery = (unsigned short)pDevice->deviceBasic.nBattery;
					strcpy_s(szFactoryId, sizeof(szFactoryId), pDevice->deviceBasic.szFactoryId);
					strcpy_s(szOrg, sizeof(szOrg), pDevice->szOrganization);
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		EscortTask * pCurrentTask = NULL;
		bool bModifyTask = false;
		if (strlen(szCurrentTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrentTaskId);
				if (pTask) {
					if (pTask->nTaskMode == 0) {
						if (strlen(loginInfo.szHandset)) {
							pTask->nTaskMode = 1;
							strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset,
								strlen(loginInfo.szHandset));
							bModifyTask = true;
						}
					}
					else if (pTask->nTaskMode == 1) {
						if (strlen(loginInfo.szHandset)) {//current login with handset
							if (strlen(pTask->szHandset)) { //task last handset is not empty
								if (strcmp(pTask->szHandset, loginInfo.szHandset) != 0) { //change task handset
									bModifyTask = true;
									strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset,
										strlen(loginInfo.szHandset));
								}
							}
							else { //task last handset is empty
								bModifyTask = true;
								strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo.szHandset,
									strlen(loginInfo.szHandset));
							}
						} 
						else {
							if (strlen(pTask->szHandset)) {
								bModifyTask = true;
								pTask->szHandset[0] = '\0';
							}
						}
					}
					size_t nTaskSize = sizeof(EscortTask);
					pCurrentTask = (EscortTask *)zmalloc(nTaskSize);
					memcpy_s(pCurrentTask, nTaskSize, pTask, nTaskSize);
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
		}
		if (bHaveTask && pCurrentTask) {
			snprintf(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,"
				"\"limit\":%d,\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%hu"
				",\"deviceState\":%hu,\"handset\":\"%s\"}", pCurrentTask->szTaskId, pCurrentTask->szDeviceId,
				pCurrentTask->nTaskType + 1, pCurrentTask->nTaskLimitDistance, pCurrentTask->szDestination,
				pCurrentTask->szTarget, pCurrentTask->szTaskStartTime, usDeviceBattery, usDeviceStatus, 
				loginInfo.szHandset);
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\""
				":[%s]}", access_service::E_CMD_LOGIN_REPLY, szLastestSession, szTaskInfo);
			sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
		}
		else {
			if (strlen(szCurrentBindDeviceId)) {
				snprintf(szTaskInfo, sizeof(szTaskInfo), "{\"deviceId\":\"%s\",\"battery\":%hu,\"deviceState\":"
					"\"%hu\"}", szCurrentBindDeviceId, usDeviceBattery, usDeviceStatus);
				snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\""
					":[%s]}", access_service::E_CMD_LOGIN_REPLY, szLastestSession, ""/*szTaskInfo*/);
				sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
			}
			else {
				snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[]}",
					access_service::E_CMD_LOGIN_REPLY, szLastestSession);
				sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
			}
		}
		if (bModifyTask) {
			char szBody[512] = { 0 };
			snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":"
				"%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"handset\":\"%s\"}]}",
				MSG_SUB_REPORT, loginInfo.uiReqSeq, loginInfo.szDateTime, SUB_REPORT_TASK_MODIFY,
				pCurrentTask->szTaskId, loginInfo.szHandset);
			sendDataViaInteractor(szBody, strlen(szBody));
		}
		//update guarder
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo.szUser);
		if (pGuarder) {
			strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szLastestSession);
			strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint);
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		//update link
		size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
		strcpy_s(pLink->szGuarder, sizeof(pLink->szGuarder), loginInfo.szUser);
		strcpy_s(pLink->szSession, sizeof(pLink->szSession), szLastestSession);
		strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint);
		if (strlen(loginInfo.szHandset)) {
			strcpy_s(pLink->szHandset, sizeof(pLink->szHandset), loginInfo.szHandset);
		}
		if (strlen(szCurrentBindDeviceId)) {
			strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactoryId);
			strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), szCurrentBindDeviceId);
			strcpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg);
		}
		if (bHaveTask && pCurrentTask) {
			strcpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), pCurrentTask->szTaskId);
		}
		pLink->nActivated = 1;
		pLink->ulActivateTime = ulTime;
		
		pthread_mutex_lock(&m_mutex4LinkList);
		if (bNeedGenerateSession && strlen(szCurrentSession)) {
			zhash_delete(m_linkList, szCurrentSession);
		}
		zhash_update(m_linkList, szLastestSession, pLink);
		zhash_freefn(m_linkList, szLastestSession, free);
		pthread_mutex_unlock(&m_mutex4LinkList);

		if (bNeedGenerateSession && strlen(szCurrentSession)) {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]remove session=%s\r\n", __FUNCTION__,
				__LINE__, szCurrentSession);
			writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			zkRemoveSession(szCurrentSession);
		}
		zkAddSession(szLastestSession);
		char szUploadMsg[512] = { 0 };
		snprintf(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\""
			":%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\""
			":\"%s\"}]}",MSG_SUB_REPORT, getNextRequestSequence(), loginInfo.szDateTime, SUB_REPORT_GUARDER_LOGIN, 
			loginInfo.szUser, szLastestSession, loginInfo.szHandset);
		sendDataViaInteractor(szUploadMsg, strlen(szUploadMsg));
		
		pthread_mutex_lock(&m_mutex4LinkDataList); //update linkdatalist, add Guarder to handle disconnect
		std::string strLink = pEndpoint;
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), loginInfo.szUser, strlen(loginInfo.szUser));
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);

		if (pCurrentTask) {
			char szTopic[64] = { 0 };
			snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", pCurrentTask->szOrg, pCurrentTask->szFactoryId,
				pCurrentTask->szDeviceId);
			access_service::AppSubscirbeInfo * pSubInfo;
			pSubInfo = (access_service::AppSubscirbeInfo *)zmalloc(sizeof(access_service::AppSubscirbeInfo));
			strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint);
			strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pCurrentTask->szGuarder);
			strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), szLastestSession);
			strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
			pthread_mutex_lock(&m_mutex4SubscribeList);
			zhash_update(m_subscribeList, szTopic, pSubInfo);
			zhash_freefn(m_subscribeList, szTopic, free);
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			free(pCurrentTask);
			pCurrentTask = NULL;
		}
	}
	else {
		char szReply[256] = { 0 };
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, nErr);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, datetime=%s, handset=%s, result=%d,"
		" session=%s, from=%s\r\n", __FUNCTION__, __LINE__, loginInfo.szUser, loginInfo.szDateTime,
		loginInfo.szHandset, nErr, szLastestSession, pEndpoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	if (bNeedReloadData) {
		readDataBuffer();
	}
}

void AccessService::handleAppLogout(access_service::AppLogoutInfo logoutInfo, const char * pEndPoint, 
	unsigned long ulTime)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	bool bFindLink = false;
	char szReply[256] = { 0 };
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szHandset[64] = { 0 };
	bool bUpdateLink = false;
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_LOGOUT_REPLY, E_DEFAULTERROR, logoutInfo.szSession);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive logout from %s, session=%s, server "
			"not ready\r\n", __FUNCTION__, __LINE__, pEndPoint, logoutInfo.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	//logoutInfo.szSession
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			logoutInfo.szSession);
		if (pLink) {
			pLink->ulActivateTime = ulTime;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			if (strcmp(pLink->szEndpoint, pEndPoint) != 0) {
				strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndPoint, strlen(pEndPoint));
				bUpdateLink = true;
			}
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
				if (strlen(pGuarder->szTaskId) && pGuarder->usState == STATE_GUARDER_DUTY) {
					nErr = E_GUARDERINDUTY;
				}
				else {
					if (strlen(pGuarder->szBindDevice) && pGuarder->usState == STATE_GUARDER_BIND) {
						pGuarder->usState = STATE_GUARDER_FREE;
					}
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->szCurrentSession[0] = '\0';
				}
				if (strcmp(pGuarder->szLink, pEndPoint) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}", 
		access_service::E_CMD_LOGOUT_REPLY, nErr, logoutInfo.szSession);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
	if (nErr == E_OK) {
		//delete session link
		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			logoutInfo.szSession);
		if (pLink) {
			strncpy_s(szDevKey, sizeof(szDevKey), pLink->szDeviceId, strlen(pLink->szDeviceId));
			strncpy_s(szHandset, sizeof(szHandset), pLink->szHandset, strlen(pLink->szHandset));
		}
		zhash_delete(m_linkList, logoutInfo.szSession);
		pthread_mutex_unlock(&m_mutex4LinkList);

		zkRemoveSession(logoutInfo.szSession);

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
		char szUpdateMsg[512] = { 0 };
		snprintf(szUpdateMsg, sizeof(szUpdateMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,"
			"\"sequence\":%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\","
			"\"session\":\"%s\",\"handset\":\"%s\"}]}", MSG_SUB_REPORT, getNextRequestSequence(), 
			logoutInfo.szDateTime, SUB_REPORT_GUARDER_LOGOUT, szGuarder, logoutInfo.szSession, szHandset);
		sendDataViaInteractor(szUpdateMsg, strlen(szUpdateMsg));
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::string strLink = pEndPoint;
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				if (nErr == E_OK) {
					pLinkData->szUser[0] = '\0';
				}
				else {
					strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]logout session=%s, datetime=%s, user=%s, "
		"result=%d, from=%s\r\n", __FUNCTION__, __LINE__, logoutInfo.szSession, logoutInfo.szDateTime, 
		szGuarder, nErr, pEndPoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppBind(access_service::AppBindInfo bindInfo, const char * pEndPoint, 
	unsigned long ulTime)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_UNBIND_REPLY, E_DEFAULTERROR, bindInfo.szSesssion);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndPoint);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive bind request from %s, deviceId=%s, session"
			"=%s, server not ready yet, still wait for session information\r\n", __FUNCTION__, __LINE__, pEndPoint,
			bindInfo.szDeviceId, bindInfo.szSesssion);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	bool bFindLink = false;
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szFactory[4] = { 0 };
	char szOrg[40] = { 0 };
	unsigned short usBattery = 0;
	bool bBindAlready = false;
	bool bUpdateLink = false;
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
					if (pGuarder->usState == STATE_GUARDER_BIND) {
						if (strlen(pGuarder->szBindDevice) > 0) {
							if (strcmp(pGuarder->szBindDevice, bindInfo.szDeviceId) != 0) {
								nErr = E_GUARDERBINDOTHERDEVICE;
							}
							else {
								bBindAlready = true;
								bValidGuarder = true;
							}
						}
						else {
							bValidGuarder = true;
						}
					}
					else if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
						nErr = E_GUARDERINDUTY;
					}
					else if (pGuarder->usState == STATE_GUARDER_FREE) {
						bValidGuarder = true;
						if (strcmp(pGuarder->szLink, pEndPoint) != 0) {
							strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
							bUpdateLink = true;
						}
					}
					else {
						nErr = E_INVALIDACCOUNT;
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]invalid guarder=%s state=%u\r\n", 
							__FUNCTION__, __LINE__, szGuarder, pGuarder->usState);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
				}
				else {
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not find guarder, guarder=%s\r\n",
						__FUNCTION__, __LINE__, szGuarder);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
						if (strlen(pDev->szOrganization)) {
							if (strcmp(pDev->szOrganization, szOrg) != 0) {
								strncpy_s(szOrg, sizeof(szOrg), pDev->szOrganization, strlen(pDev->szOrganization));
							}
						}
						else {
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
			else {//execute unbind, mode = 1
				pthread_mutex_lock(&g_mutex4GuarderList);
				if (zhash_size(g_guarderList)) {
					Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId) > 0) {
							nErr = E_GUARDERINDUTY;
						}
						else if (pGuarder->usState == STATE_GUARDER_FREE) {
							nErr = E_UNBINDDEVICE;
						}
						else if (pGuarder->usState == STATE_GUARDER_BIND && strlen(pGuarder->szBindDevice) > 0) {
							if (strcmp(pGuarder->szBindDevice, bindInfo.szDeviceId) != 0) {
								nErr = E_DEVICENOTMATCH;
							}
							else {
								bValidGuarder = true;
								//if (strcmp(pGuarder->szLink, pEndPoint) != 0) {
								//	strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint, strlen(pEndPoint));
								//	bUpdateLink = true;
								//}
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
									snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]device already bind guarder=%s, wanna"
										" bind guarder=%s, not match\r\n", __FUNCTION__, __LINE__, pDev->szBindGuard, szGuarder);
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									nErr = E_GUARDERNOTMATCH;
								}
							}
						}
						strncpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId, 
							strlen(pDev->deviceBasic.szFactoryId));
						if (strlen(pDev->szOrganization)) {
							strncpy_s(szOrg, sizeof(szOrg), pDev->szOrganization, strlen(pDev->szOrganization));
						}
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
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			std::string strLink = pEndPoint;
			std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	if (nErr == E_OK) {
		if (!bBindAlready) {
			//1.update information; 2.send to M_S
			if (bindInfo.nMode == 0) {
				//mark,version,type,sequence,datetime,report[subType,factoryId,deviceId,guarder]
				char szBody[512] = { 0 };
				snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guard"
					"er\":\"%s\"}]}", MSG_SUB_REPORT, bindInfo.uiReqSeq, bindInfo.szDateTime, SUB_REPORT_DEVICE_BIND,
					strlen(szFactory) ? szFactory : "01", bindInfo.szDeviceId, szGuarder);
				sendDataViaInteractor(szBody, strlen(szBody));
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), bindInfo.szDeviceId,
						strlen(bindInfo.szDeviceId));
					pGuarder->usState = STATE_GUARDER_BIND;
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
				zkSetSession(bindInfo.szSesssion);
				snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]add new subscriber for %s, link=%s\r\n", 
					__FUNCTION__, __LINE__, szTopic, pEndPoint);
				writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
			else {
				char szBody[512] = { 0 };
				snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guard"
					"er\":\"%s\"}]}", MSG_SUB_REPORT, bindInfo.uiReqSeq, bindInfo.szDateTime, SUB_REPORT_DEVICE_UNBIND,
					strlen(szFactory) ? szFactory : "01", bindInfo.szDeviceId, szGuarder);
				sendDataViaInteractor(szBody, strlen(szBody));

				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->usState = STATE_GUARDER_FREE;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo.szDeviceId);
				if (pDev) {
					pDev->szBindGuard[0] = '\0';
					pDev->ulBindTime = 0;
				}
				pthread_mutex_unlock(&g_mutex4DevList);

				pthread_mutex_lock(&m_mutex4LinkList);
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
					bindInfo.szSesssion);
				if (pLink) {
					pLink->szDeviceId[0] = '\0';
					pLink->szFactoryId[0] = '\0';
					pLink->szOrg[0] = '\0';
					pLink->nNotifyBattery = 0;
					pLink->nNotifyOnline = 0;
					pLink->nNotifyPosition = 0;
					pLink->nNotifyStatus = 0;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				//remove subsciber
				char szTopic[64] = { 0 };
				snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, bindInfo.szDeviceId);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				zhash_delete(m_subscribeList, szTopic);
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				
				zkSetSession(bindInfo.szSesssion);
			}
		}
		else {
			//already bind
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
			zkSetSession(bindInfo.szSesssion);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]seq=%lu, %s, device=%s, factoryId=%s, session=%s,"
		" guarder=%s, datetime=%s, org=%s, result=%d, from=%s\r\n", __FUNCTION__, __LINE__, bindInfo.uiReqSeq, 
		(bindInfo.nMode == 0) ? "bind" : "unbind", bindInfo.szDeviceId, strlen(szFactory) ? szFactory : "01",
		bindInfo.szSesssion, szGuarder, bindInfo.szDateTime, szOrg, nErr, pEndPoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
	char szHandset[64] = { 0 };
	char szFactoryId[4] = { 0 };
	char szDeviceId[16] = { 0 };
	char szOrg[40] = { 0 };
	char szTaskId[12] = { 0 };
	char szReply[256] = { 0 };
	bool bUpdateLink = false;
	char szFilter[64] = { 0 };
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}",
			access_service::E_CMD_TASK_REPLY, E_DEFAULTERROR, taskInfo.szSession) ;
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive submit task from %s, session=%s, server "
			"not ready\r\n", __FUNCTION__, __LINE__, pEndpoint, taskInfo.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
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
			if (strlen(pLink->szHandset)) {
				strncpy_s(szHandset, sizeof(szHandset), pLink->szHandset, strlen(pLink->szHandset));
			}
			if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
				strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
				snprintf(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
				bUpdateLink = true;
			}
			bValidLink = true;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4GuarderList);
		if (zhash_size(g_guarderList)) {
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					nErr = E_GUARDERINDUTY;
				}
				else if (pGuarder->usState == STATE_GUARDER_FREE) {
					nErr = E_UNBINDDEVICE;
				}
				else if (pGuarder->usState == STATE_GUARDER_BIND) {
					if (strlen(szDeviceId) == 0) {
						strncpy_s(szDeviceId, sizeof(szDeviceId), pGuarder->szBindDevice, strlen(pGuarder->szBindDevice));
					}
					bValidGuarder = true;
				}
				else {
					nErr = E_DEFAULTERROR;
				}
				strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
				if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
				}
			}
			else {
				nErr = E_INVALIDACCOUNT;
			}
		}
		else {
			nErr = E_INVALIDACCOUNT;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
	else {
		nErr = E_INVALIDSESSION;
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
				else {
					nErr = E_DEFAULTERROR;
				}
				if (strlen(szFactoryId) == 0) {
					strncpy_s(szFactoryId, sizeof(szFactoryId), pDev->deviceBasic.szFactoryId,
						strlen(pDev->deviceBasic.szFactoryId));
				}
				if (strlen(pDev->szOrganization)) {
					strncpy_s(szOrg, sizeof(szOrg), pDev->szOrganization, strlen(pDev->szOrganization));
				}
			}
			else {
				snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not find deviceId=%s in the list\r\n", 
					__FUNCTION__, __LINE__, szDeviceId);
				writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
				nErr = E_INVALIDDEVICE;
			}
		}
		else {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not find deviceId=%s in the list, list empty\r\n",
				__FUNCTION__, __LINE__, szDeviceId);
			writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
			nErr = E_INVALIDDEVICE;
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bValidDevice) {
			EscortTask * pTask = (EscortTask *)zmalloc(sizeof(EscortTask));
			while (1) {
				generateTaskId(szTaskId, sizeof(szTaskId));
				if (strlen(szTaskId) > 0) {
					break;
				}
			}
			printf("generate task=%s\n", szTaskId);
			strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId, strlen(szTaskId));
			pTask->nTaskType = (uint8_t)taskInfo.usTaskType - 1;
			pTask->nTaskState = 0;
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = (uint8_t)taskInfo.usTaskLimit;
			strncpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactoryId, strlen(szFactoryId));
			strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), szDeviceId, strlen(szDeviceId));
			strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg, strlen(szOrg));
			strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), szGuarder, strlen(szGuarder));
			if (strlen(szHandset)) {
				strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), szHandset, strlen(szHandset));
				pTask->nTaskMode = 1;
			}
			else {
				pTask->nTaskMode = 0;
			}
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
	if (bUpdateLink) {
		if (strlen(szFilter)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint) != 0) {
						strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::string strLink = pEndpoint;
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		access_service::E_CMD_TASK_REPLY, nErr, taskInfo.szSession, szTaskId);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
	if (nErr == E_OK) {
		char szBody[512] = { 0 };
		snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"taskType\":%u,\"limit\":%u,"
			"\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\""
			"%s\",\"handset\":\"%s\"}]}", MSG_SUB_REPORT, taskInfo.uiReqSeq, taskInfo.szDatetime, 
			SUB_REPORT_TASK, szTaskId, taskInfo.usTaskType - 1, taskInfo.usTaskLimit, szFactoryId, szDeviceId,
			szGuarder, taskInfo.szDestination, taskInfo.szTarget, szHandset);
		sendDataViaInteractor(szBody, strlen(szBody));
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId, strlen(szTaskId));
			strncpy_s(pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime), taskInfo.szDatetime,
				strlen(taskInfo.szDatetime));
			pGuarder->usState = STATE_GUARDER_DUTY;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			taskInfo.szSession);
		if (pLink) {
			strncpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), szTaskId, strlen(szTaskId));
			//if guarder have bind device already, but connection disconnect, 
			//then guarder have relogin, create new AppLinkInfo which has session,guarder,information by login,
			//but miss bind device, here fill the missing device information
			if (strlen(pLink->szDeviceId) == 0) {
				strncpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), szDeviceId, strlen(szDeviceId));
			}
			if (strlen(pLink->szFactoryId) == 0) {
				strncpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactoryId, strlen(szFactoryId));
			}
			if (strlen(pLink->szOrg) == 0) {
				strncpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg, strlen(szOrg));
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		zkSetSession(taskInfo.szSession);
	}
	//publishNotifyMessage(taskInfo.szSession, pEndpoint, szDeviceId);
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit task %d: session=%s, type=%u, limit=%u,"
		" target=%s, destination=%s, datetime=%s, result=%d, taskId=%s, handset=%s, from=%s\r\n", __FUNCTION__,
		__LINE__, taskInfo.uiReqSeq, taskInfo.szSession, taskInfo.usTaskType - 1, taskInfo.usTaskLimit, 
		taskInfo.szTarget, taskInfo.szDestination, taskInfo.szDatetime, nErr, szTaskId, szHandset, pEndpoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppCloseTask(access_service::AppCloseTaskInfo taskInfo, const char * pEndpoint,
	unsigned long ulTime)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	bool bValidLink = false;
	bool bValidTask = false;
	char szGuarder[20] = { 0 };
	char szReply[256] = { 0 };
	char szFilter[64] = { 0 };
	char szDeviceId[16] = { 0 };
	bool bUpdateLink = false;
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}",
			access_service::E_CMD_TAKK_CLOSE_REPLY, E_DEFAULTERROR, taskInfo.szSession);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive close task from %s, session=%s, server "
			"not ready\r\n", __FUNCTION__, __LINE__, pEndpoint, taskInfo.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}

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
				if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
					strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
					bUpdateLink = true;
				}
			}
			else {
				nErr = E_INVALIDTASK;
			}
		}
		else {
			nErr = E_INVALIDSESSION;
		}
	}
	else {
		nErr = E_INVALIDSESSION;
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
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
		else {
			nErr = E_INVALIDTASK;
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			if (pGuarder->usState != STATE_GUARDER_DEACTIVATE) {
				if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		if (strlen(szFilter)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint) != 0) {
						strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			std::string strLink = pEndpoint;
			std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
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

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			if (pGuarder->usState == STATE_GUARDER_DUTY) {
				pGuarder->usState = STATE_GUARDER_BIND;
			}
			pGuarder->szTaskId[0] = '\0';
			pGuarder->szTaskStartTime[0] = '\0';
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&g_mutex4TaskList);
		zhash_delete(g_taskList, taskInfo.szTaskId);
		pthread_mutex_unlock(&g_mutex4TaskList);

		zkSetSession(taskInfo.szSession);
	}
	//publishNotifyMessage(taskInfo.szSession, pEndpoint, szDeviceId);
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]submit close task, session=%s, taskId=%s, "
		"datetime=%s, result=%d, from=%s\r\n", __FUNCTION__, __LINE__, taskInfo.szSession, taskInfo.szTaskId, 
		taskInfo.szDatetime, nErr, pEndpoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppPosition(access_service::AppPositionInfo posInfo, const char * pEndpoint, 
	unsigned long ulTime)
{
	char szLog[256] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szDevice[16] = { 0 };
	char szFilter[64] = { 0 };
	char szGuarder[20] = { 0 };
	bool bUpdateLink = false;
	bool bNotifyBattery = false;
	bool bNotifyOffline = false;
	bool bNotifyPosition = false;
	bool bNotifyLoose = false;
	unsigned short usBattery = 0;
	unsigned short usLoose = 0;
	double dLat = 0.00, dLng = 0.00;
	char szDatetime[20] = { 0 };
	format_datetime(ulTime, szDatetime, sizeof(szDatetime));
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
				strncpy_s(szDevice, sizeof(szDevice), pLink->szDeviceId, strlen(pLink->szDeviceId));
				if (strcmp(pLink->szEndpoint, pEndpoint) != 0) {
					strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint, strlen(pEndpoint));
					bUpdateLink = true;
				}
				if (pLink->nNotifyOnline) {
					bNotifyOffline = true;
					pLink->nNotifyOnline = 0;
					pLink->nNotifyBattery = 0;
					bNotifyBattery = false;
				}
				else {
					if (pLink->nNotifyBattery) {
						bNotifyBattery = true;
						pLink->nNotifyBattery = 0;
						pLink->nNotifyOnline = 0;
						bNotifyOffline = false;
					}
				}
				if (pLink->nNotifyStatus) {
					bNotifyLoose = true;
					pLink->nNotifyStatus = 0;
				}
				if (pLink->nNotifyPosition) {
					bNotifyPosition = true;
					pLink->nNotifyPosition = 0;
				}
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDevice);
		if (pDev) {
			if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
				|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
				pDev->guardPosition.dLatitude = posInfo.dLat;
				pDev->guardPosition.dLngitude = posInfo.dLng;
			}
			usBattery = (unsigned short)pDev->deviceBasic.nBattery;
			usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
			dLat = pDev->devicePosition.dLatitude;
			dLng = pDev->devicePosition.dLngitude;
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
			if (strcmp(pGuarder->szLink, pEndpoint) != 0) {
				strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint, strlen(pEndpoint));
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&m_mutex4SubscribeList);
		access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
			m_subscribeList, szFilter);
		if (pSubInfo) {
			if (strcmp(pSubInfo->szEndpoint, pEndpoint) != 0) {
				strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
				snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]update subscriber topic=%s, endpoint=%s\r\n",
					__FUNCTION__, __LINE__, szFilter, pEndpoint);
				writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
		}
		else {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not find subscriber topic=%s\r\n",
				__FUNCTION__, __LINE__, szFilter);
			writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
		}
		pthread_mutex_unlock(&m_mutex4SubscribeList);
		if (bUpdateLink) {
			pthread_mutex_lock(&m_mutex4LinkDataList);
			if (!m_linkDataList.empty()) {
				std::string strLink = pEndpoint;
				std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);



		}	
		if (bNotifyOffline) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":0,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, posInfo.szSession,
				access_service::E_NOTIFY_DEVICE_OFFLINE, szDevice, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint);
		}
		if (bNotifyBattery) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, posInfo.szSession,
				access_service::E_NOTIFY_DEVICE_BATTERY, szDevice, usBattery, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint);
		}
		if (bNotifyLoose) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				posInfo.szSession, access_service::E_ALARM_DEVICE_LOOSE, szDevice, usBattery,
				(usLoose == 1) ? 0 : 1, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint);
		}
		if (bNotifyPosition) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				posInfo.szSession, access_service::E_NOTIFY_DEVICE_POSITION, szDevice, usBattery, dLat, dLng,
				szDatetime);
			sendDatatoEndpoint(szMsg, sizeof(szMsg), pEndpoint);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report position session=%s, taskId=%s, lat=%.06f, "
		"lng=%0.6f, datetime=%s, from=%s\r\n", __FUNCTION__, __LINE__, posInfo.szSession, posInfo.szTaskId,
		posInfo.dLat, posInfo.dLng, posInfo.szDatetime, pEndpoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppFlee(access_service::AppSubmitFleeInfo fleeInfo, const char * pEndpoint, 
	unsigned long ulTime)
{
	int nErr = E_OK;
	char szLog[512] = { 0 };
	char szFactoryId[4] = { 0 };
	char szDeviceId[16] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szReply[256] = { 0 };
	bool bUpdateLink = false;
	char szGuarder[20] = { 0 };
	char szFilter[64] = { 0 };
	bool bNotify = true;
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
			fleeInfo.nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
			E_DEFAULTERROR, fleeInfo.szSession, fleeInfo.szTaskId);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive flee from %s, mode=%d, session=%s, server"
			" not ready\r\n", __FUNCTION__, __LINE__, pEndpoint, fleeInfo.nMode, fleeInfo.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
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
				if (fleeInfo.nMode == 0) {
					pTask->nTaskFlee = 1;
				}
				else {
					pTask->nTaskFlee = 0;
				}
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
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]change status guard->flee, deviceId=%s, "
							"session=%s\r\n", __FUNCTION__, __LINE__, szDeviceId, fleeInfo.szSession);
						writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
					else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						//do nothing
						bNotify = false;
					} 
					else if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						nErr = E_UNABLEWORKDEVICE;
					}
				}
				else { //flee revoke
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
						//do nothing
						bNotify = false;
					}
					else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]change status flee->guard, deviceId=%s, "
							"session=%s\r\n", __FUNCTION__, __LINE__, szDeviceId, fleeInfo.szSession);
						writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
					if (strcmp(pSubInfo->szEndpoint, pEndpoint) != 0) {
						strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint, strlen(pEndpoint));
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]update subscriber for %s, endpoint=%s\r\n",
							__FUNCTION__, __LINE__, szFilter, pEndpoint);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				pthread_mutex_lock(&m_mutex4LinkDataList);
				std::string strLink = pEndpoint;
				std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkDataList);
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
		if (bNotify) {
			char szBody[512] = { 0 };
			snprintf(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\"}]}",
				MSG_SUB_REPORT, fleeInfo.uiReqSeq, fleeInfo.szDatetime,
				fleeInfo.nMode == 0 ? SUB_REPORT_DEVICE_FLEE : SUB_REPORT_DEVICE_FLEE_REVOKE, fleeInfo.szTaskId);
			sendDataViaInteractor(szBody, strlen(szBody));
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report %s %d, session=%s, taskId=%s, datetime=%s"
		 ", result=%d, from=%s\r\n" ,__FUNCTION__, __LINE__, fleeInfo.nMode == 0 ? "flee" : "flee revoke", 
		fleeInfo.uiReqSeq, fleeInfo.szSession, fleeInfo.szTaskId, fleeInfo.szDatetime, nErr, pEndpoint);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppKeepAlive(access_service::AppKeepAlive keepAlive_, const char * pEndpoint_, 
	unsigned long ulTime_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d}",
			access_service::E_CMD_KEEPALIVE_REPLY, keepAlive_.szSession, keepAlive_.uiSeq, nErr);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive alive from %s, session=%s, server not ready, "
			"wait to load session\r\n", __FUNCTION__, __LINE__, pEndpoint_, keepAlive_.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			keepAlive_.szSession);
		if (!pLink) {
			nErr = E_INVALIDSESSION;
		}
	}
	else {
		nErr = E_INVALIDSESSION;
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d}",
		access_service::E_CMD_KEEPALIVE_REPLY, keepAlive_.szSession, keepAlive_.uiSeq, nErr);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
	if (nErr == E_OK) {
		bool bUpdateLink = false;
		char szGuarder[20] = { 0 };
		bool bHaveSubscribe = false;
		char szSubTopic[64] = { 0 };
		char szDeviceId[20] = { 0 };
		bool bNotifyOffline = false;
		bool bNotifyBattery = false;
		bool bNotifyLoose = false;
		bool bNotifyPosition = false;
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
				keepAlive_.szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = ulTime_;
				if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
					bUpdateLink = true;
					strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_, strlen(pEndpoint_));
					strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
					if (strlen(pLink->szDeviceId) && strlen(pLink->szFactoryId) && strlen(pLink->szOrg)) {
						snprintf(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId,
							pLink->szDeviceId);
						strncpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId, strlen(pLink->szDeviceId));
						bHaveSubscribe = true;
					}
				}
				if (pLink->nNotifyOnline) {
					pLink->nNotifyOnline = 0;
					pLink->nNotifyBattery = 0;
					bNotifyOffline = true;
					bNotifyBattery = false;
				}
				else {
					if (pLink->nNotifyBattery) {
						pLink->nNotifyBattery = 0;
						pLink->nNotifyOnline = 0;
						bNotifyOffline = false;
						bNotifyBattery = true;
					}
				}
				if (pLink->nNotifyPosition) {
					pLink->nNotifyPosition = 0;
					bNotifyPosition = true;
				}
				if (pLink->nNotifyStatus) {
					pLink->nNotifyStatus = 0;
					bNotifyLoose = true;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bUpdateLink && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_, strlen(pEndpoint_));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::string strLink = pEndpoint_;
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (bHaveSubscribe) {
			if (strlen(szSubTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szSubTopic);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_, strlen(pEndpoint_));
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]update subscriber=%s, endpoint=%s\r\n",
							__FUNCTION__, __LINE__, szSubTopic, pEndpoint_);
						writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
			}
		}
		unsigned short usBattery = 0;
		unsigned short usLoose = 0;
		double dLat = 0.00, dLng = 0.00;
		if (strlen(szDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDev) {
					usBattery = (unsigned short)pDev->deviceBasic.nBattery;
					usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
					dLat = pDev->devicePosition.dLatitude;
					dLng = pDev->devicePosition.dLngitude;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		char szDatetime[20] = { 0 };
		format_datetime(ulTime_, szDatetime, sizeof(szDatetime));
		if (bNotifyOffline) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":0,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, keepAlive_.szSession,
				access_service::E_NOTIFY_DEVICE_OFFLINE, szDeviceId, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint_);
		}
		if (bNotifyBattery) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, keepAlive_.szSession,
				access_service::E_NOTIFY_DEVICE_BATTERY, szDeviceId, usBattery, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint_);
		}
		if (bNotifyLoose) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				keepAlive_.szSession, access_service::E_ALARM_DEVICE_LOOSE, szDeviceId, usBattery,
				(usLoose == 1) ? 0 : 1, szDatetime);
			sendDatatoEndpoint(szMsg, strlen(szMsg), pEndpoint_);
		}
		if (bNotifyPosition) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				keepAlive_.szSession, access_service::E_NOTIFY_DEVICE_POSITION, szDeviceId, usBattery, dLat, dLng,
				szDatetime);
			sendDatatoEndpoint(szMsg, sizeof(szMsg), pEndpoint_);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report keep alive, from=%s, session=%s, seq=%u,"
		" datetime=%s, retcode=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_, keepAlive_.szSession, 
		keepAlive_.uiSeq, keepAlive_.szDatetime, nErr);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppModifyAccountPassword(access_service::AppModifyPassword modifyPasswd_,
	const char * pEndpoint_, unsigned long ulTime_)
{
	char szLog[512] = { 0 };
	char szReply[256] = { 0 };
	int nErr = E_OK;
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"datetime\""
			":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, modifyPasswd_.szSession, E_DEFAULTERROR,
			modifyPasswd_.szDatetime);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive modify password from %s, session=%s, "
			"server not ready\r\n", __FUNCTION__, __LINE__, pEndpoint_, modifyPasswd_.szSession);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	bool bValidLink = false;
	bool bUpdateLink = false;
	char szGuarder[20] = { 0 };
	char szSubTopic[64] = { 0 };
	bool bHaveSubscribe = false;
	char szDeviceId[20] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			modifyPasswd_.szSession);
		if (pLink) {
			if (pLink->nActivated == 0) {
				pLink->nActivated = 1;
			}
			pLink->ulActivateTime = ulTime_;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
				strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_, strlen(pEndpoint_));
				bUpdateLink = true;
				if (strlen(pLink->szDeviceId) && strlen(pLink->szFactoryId) && strlen(pLink->szOrg)) {
					bHaveSubscribe = true;
					snprintf(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
					strncpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId, strlen(pLink->szDeviceId));
				}
			}
			bValidLink = true;
		}
		else {
			nErr = E_INVALIDSESSION;
		}
	}
	else {
		nErr = E_INVALIDSESSION;
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		if (strcmp(modifyPasswd_.szCurrPassword, modifyPasswd_.szNewPassword) != 0) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strcmp(pGuarder->szPassword, modifyPasswd_.szCurrPassword) == 0) {
					strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword), modifyPasswd_.szNewPassword,
						strlen(modifyPasswd_.szNewPassword));
				}
				else {
					nErr = E_INVALIDPASSWD;
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s,"
						" datetime=%s, current password is incorrect, can not execute modify, input=%s\r\n", 
						__FUNCTION__, __LINE__, pEndpoint_, szGuarder, modifyPasswd_.szDatetime, 
						modifyPasswd_.szCurrPassword);
					writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
				}
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_, strlen(pEndpoint_));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		else {
			nErr = E_INVALIDPASSWD;
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s, "
				"datetime=%s, new password is same with the current password, nothing to modify\r\n",
				__FUNCTION__, __LINE__, pEndpoint_, szGuarder, modifyPasswd_.szDatetime);
			writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
		}
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"datetime\""
		":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, modifyPasswd_.szSession, nErr, 
		modifyPasswd_.szDatetime);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
	if (nErr == E_OK) {
		char szMsgBody[512] = { 0 };
		snprintf(szMsgBody, sizeof(szMsgBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,"
			"\"sequence\":%u,\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\","
			"\"currPasswd\":\"%s\",\"newPasswd\":\"%s\"}]}", MSG_SUB_REPORT, modifyPasswd_.uiSeq,
			modifyPasswd_.szDatetime, SUB_REPORT_MODIFY_USER_PASSWD, szGuarder,
			modifyPasswd_.szCurrPassword, modifyPasswd_.szNewPassword);
		sendDataViaInteractor(szMsgBody, strlen(szMsgBody));
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			std::string strLink = pEndpoint_;
			std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (bHaveSubscribe && strlen(szSubTopic)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
				m_subscribeList, szSubTopic);
			if (pSubInfo) {
				if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
					strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_, strlen(pEndpoint_));
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			//publishNotifyMessage(modifyPasswd_.szSession, pEndpoint_, szDeviceId);
		}
	}
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]report modify password from %s, session=%s,"
		" seq=%u, datetime=%s, result=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_,
		modifyPasswd_.szSession, modifyPasswd_.uiSeq, modifyPasswd_.szDatetime, nErr);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppQueryTask(access_service::AppQueryTask queryTask_, 
	const char * pEndpoint_, unsigned long ulTime_)
{
	char szLog[512] = { 0 };
	bool bValidTask = false;
	bool bValidLink = false;
	int nErr = E_OK;
	EscortTask * pCurrTask = NULL;
	char szDeviceId[20] = { 0 };
	char szGuarder[20] = { 0 };
	bool bUpdateLink = false;
	bool bHaveSubscribe = false;
	char szSubTopic[64] = { 0 };
	unsigned short usDeviceStatus = 0;
	unsigned short usDeviceBattery = 0;
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
			queryTask_.szSession);
		if (pLink) {
			if (!pLink->nActivated) {
				pLink->nActivated = 1;
			}
			pLink->ulActivateTime = ulTime_;
			if (strlen(pLink->szDeviceId) && strlen(pLink->szTaskId)) {
				if (strcmp(pLink->szTaskId, queryTask_.szTaskId) == 0) {
					strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
					if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
						strncpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_, strlen(pEndpoint_));
						bUpdateLink = true;
					}
					bValidLink = true;
					bHaveSubscribe = true;
					snprintf(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
				}
				else {
					nErr = E_INVALIDTASK;
				}
			}
			else {
				nErr = E_UNBINDDEVICE;
			}
		}
		else {
			nErr = E_INVALIDSESSION;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, queryTask_.szTaskId);
			if (pTask) {
				bValidTask = true;
				size_t nTaskSize = sizeof(EscortTask);
				pCurrTask = (EscortTask *)zmalloc(nTaskSize);
				memcpy_s(pCurrTask, nTaskSize, pTask, nTaskSize);
				strncpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId, strlen(pTask->szDeviceId));
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
		if (bUpdateLink) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			if (zhash_size(g_guarderList)) {
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
						strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_, strlen(pEndpoint_));
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4LinkDataList);
			if (!m_linkDataList.empty()) {
				std::string strLink = pEndpoint_;
				std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData && strlen(szGuarder)) {
						strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
			if (bHaveSubscribe && strlen(szSubTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
					m_subscribeList, szSubTopic);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]change subsciber for %s link, from=%s to %s\r\n",
							__FUNCTION__, __LINE__, szSubTopic, pSubInfo->szEndpoint, pEndpoint_);
						writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						strncpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_, strlen(pEndpoint_));
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				//publishNotifyMessage(queryTask_.szSession, pEndpoint_, szDeviceId);
			}
		}
	}
	if (bValidTask) {
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDevice) {
				usDeviceBattery = pDevice->deviceBasic.nBattery;
				usDeviceStatus = pDevice->deviceBasic.nStatus;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
	}
	char szReply[512] = { 0 };
	if (nErr == E_OK) {
		if (bValidTask && pCurrTask) {
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":"
				"\"%s\",\"taskInfo\":[{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,\"limit\":%d,"
				"\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%u,\"deviceState\""
				":%u,\"handset\":\"%s\"}]}", access_service::E_CMD_QUERY_TASK_REPLY, nErr, queryTask_.szSession, 
				queryTask_.szDatetime, pCurrTask->szTaskId, pCurrTask->szDeviceId, pCurrTask->nTaskType + 1, 
				pCurrTask->nTaskLimitDistance, pCurrTask->szDestination, pCurrTask->szTarget, 
				pCurrTask->szTaskStartTime, usDeviceBattery, usDeviceStatus, pCurrTask->szHandset);
		}
		else {
			snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":"
				"\"%s\",\"taskInfo\":\"[]\"}", access_service::E_CMD_QUERY_TASK_REPLY, nErr,
				queryTask_.szSession, queryTask_.szDatetime);
		}
	}
	else {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":"
			"\"%s\",\"taskInfo\":\"[]\"}", access_service::E_CMD_QUERY_TASK_REPLY, nErr, 
			queryTask_.szSession, queryTask_.szDatetime);
	}
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, session=%s, taskId=%s, "
		"datetime=%s, result=%d, deviceState=%u\r\n", __FUNCTION__, __LINE__, pEndpoint_, queryTask_.szSession, 
		queryTask_.szTaskId, queryTask_.szDatetime, nErr, (nErr == 0) ? usDeviceStatus : 0);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	if (pCurrTask) {
		free(pCurrTask);
		pCurrTask = NULL;
	}
}

void AccessService::handleAppDeviceCommand(access_service::AppDeviceCommandInfo cmdInfo_,
	const char * pEndpoint_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d,"
			"\"datetime\":\"%s\",\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY, 
			cmdInfo_.szSession, cmdInfo_.nSeq, E_DEFAULTERROR, cmdInfo_.szDatetime, cmdInfo_.szDeviceId);
		sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]receive device command from %s, session=%s, "
			"seq=%d, datetime=%s, deviceId=%s, server not ready, wait to load session\r\n", __FUNCTION__, 
			__LINE__, pEndpoint_, cmdInfo_.szSession, cmdInfo_.nSeq, cmdInfo_.szDatetime, cmdInfo_.szDeviceId);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	if (strlen(cmdInfo_.szSession) > 0 && strlen(cmdInfo_.szDeviceId) && cmdInfo_.nParam1 > 0) {
		bool bValidLink = false;
		bool bValidDevice = false;
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
				cmdInfo_.szSession);
			if (pLink) {
				if (strcmp(pLink->szDeviceId, cmdInfo_.szDeviceId) != 0) {
					nErr = E_DEVICENOTMATCH;
				}
				else {
					bValidLink = true;
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bValidLink) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, cmdInfo_.szDeviceId);
			if (pDevice) {
				if (strlen(pDevice->szBindGuard)) {
					bValidDevice = true;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bValidDevice) {
			int nUploadCmd = 0;
			if (cmdInfo_.nParam1 == access_service::E_DEV_CMD_ALARM) {
				nUploadCmd = PROXY_NOTIFY_DEVICE_FLEE;
			}
			else if (cmdInfo_.nParam1 == access_service::E_DEV_CMD_RESET) {
				nUploadCmd = PROXY_CONF_DEVICE_REBOOT;
			}

			char szUploadMsg[512] = { 0 };
			sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,"
				"\"sequence\":%d,\"datetime\":\"%s\",\"request\":[{\"subType\":%d,\"factoryId\":\"%s\","
				"\"deviceId\":\"%s\",\"param\":%d}]}", MSG_SUB_REQUEST, getNextRequestSequence(), 
				cmdInfo_.szDatetime, nUploadCmd, cmdInfo_.szFactoryId, cmdInfo_.szDeviceId,
				cmdInfo_.nParam2);
			sendDataViaInteractor(szUploadMsg, strlen(szUploadMsg));
		}
	}
	snprintf(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d,"
		"\"datetime\":\"%s\",\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY,
		cmdInfo_.szSession, cmdInfo_.nSeq, nErr, cmdInfo_.szDatetime, cmdInfo_.szDeviceId);
	sendDatatoEndpoint(szReply, strlen(szReply), pEndpoint_);
	snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, deviceId=%s, "
		"session=%s, param1=%d, param2=%d, seq=%d, datetime=%s, retcode=%d\r\n", __FUNCTION__, __LINE__,
		pEndpoint_, cmdInfo_.szDeviceId, cmdInfo_.szSession, cmdInfo_.nParam1, cmdInfo_.nParam2, 
		cmdInfo_.nSeq, cmdInfo_.szDatetime, nErr);
	writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
	sprintf_s(szSession, sizeof(szSession), "%ld", now);
	unsigned char key[crypto_generichash_KEYBYTES];
	randombytes_buf(key, sizeof(key));
	unsigned char szOut[10] = { 0 };
	crypto_shorthash(szOut, (const unsigned char *)szSession, strlen(szSession), key);
	if (nSize > 16 && pSession) {
		for (int i = 0; i < 8; i++) {
			char cell[4] = { 0 };
			sprintf_s(cell, sizeof(cell), "%02x", szOut[i]);
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
				case PUBMSG_DEVICE_ALIVE: {
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
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_LOCATE: {
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
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										gpsLocateMsg.dLng = doc["lngitude"].GetDouble();
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
										else {
											strcpy_s(lbsLocateMsg.szFactoryId, sizeof(lbsLocateMsg.szFactoryId), "01");
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
											strncpy_s(lbsLocateMsg.szOrg, sizeof(lbsLocateMsg.szOrg), doc["orgId"].GetString(), nSize);
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
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										lbsLocateMsg.dLng = doc["lngitude"].GetDouble();
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
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic locate message "
							"error\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_ALARM: {
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
											strncpy_s(lpAlarmMsg.szOrg, sizeof(lpAlarmMsg.szOrg), doc["orgId"].GetString(), nSize);
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
								if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
									TopicAlarmMessageFlee fleeAlarmMsg;
									memset(&fleeAlarmMsg, 0, sizeof(TopicAlarmMessageFlee));
									char szDatetime[20] = { 0 };
									bool bValidDevice = false;
									bool bValidGuarder = false;
									bool bValidTask = false;
									bool bValidMode = false;
									bool bValidDatetime = false;
									if (doc.HasMember("factoryId")) {
										if (doc["factoryId"].IsString()) {
											size_t nSize = doc["factoryId"].GetStringLength();
											if (nSize) {
												strncpy_s(fleeAlarmMsg.szFactoryId, sizeof(fleeAlarmMsg.szFactoryId),
													doc["factoryId"].GetString(), nSize);
											}
										}
									}
									if (doc.HasMember("deviceId")) {
										if (doc["deviceId"].IsString()) {
											size_t nSize = doc["deviceId"].GetStringLength();
											if (nSize) {
												strncpy_s(fleeAlarmMsg.szDeviceId, sizeof(fleeAlarmMsg.szDeviceId),
													doc["deviceId"].GetString(), nSize);
												bValidDevice = true;
											}
										}
									}
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strncpy_s(fleeAlarmMsg.szOrg, sizeof(fleeAlarmMsg.szOrg),
													doc["orgId"].GetString(), nSize);
											}
										}
									}
									if (doc.HasMember("guarder")) {
										if (doc["guarder"].IsString()) {
											size_t nSize = doc["guarder"].GetStringLength();
											if (nSize) {
												strncpy_s(fleeAlarmMsg.szGuarder, sizeof(fleeAlarmMsg.szGuarder),
													doc["guarder"].GetString(), nSize);
												bValidGuarder = true;
											}
										}
									}
									if (doc.HasMember("taskId")) {
										if (doc["taskId"].IsString()) {
											size_t nSize = doc["taskId"].GetStringLength();
											if (nSize) {
												strncpy_s(fleeAlarmMsg.szTaskId, sizeof(fleeAlarmMsg.szTaskId),
													doc["taskId"].GetString(), nSize);
												bValidTask = true;
											}
										}
									}
									if (doc.HasMember("battery")) {
										if (doc["battery"].IsInt()) {
											fleeAlarmMsg.usBattery = (unsigned short)doc["battery"].GetInt();
										}
									}
									if (doc.HasMember("mode")) {
										if (doc["mode"].IsInt()) {
											fleeAlarmMsg.usMode = (unsigned short)doc["mode"].GetInt();
											bValidMode = true;
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
												fleeAlarmMsg.ulMessageTime = strdatetime2time(szDatetime);
												bValidDatetime = true;
											}
										}
									}
									if (bValidDevice && bValidGuarder && bValidTask && bValidTask && bValidMode 
										&& bValidDatetime) {
										handleTopicAlarmFleeMsg(&fleeAlarmMsg, pMsg->szMsgMark);
									}
									else {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse flee message data miss, "
											"factoryId=%s, deviceId=%s, orgId=%s, guarder=%s, taskId=%s, mode=%hu, battery=%hu, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, fleeAlarmMsg.szFactoryId,
											fleeAlarmMsg.szDeviceId, fleeAlarmMsg.szOrg, fleeAlarmMsg.szGuarder,
											fleeAlarmMsg.szTaskId, fleeAlarmMsg.usMode, fleeAlarmMsg.usBattery, szDatetime);
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								break;
							}
							case ALARM_LOCATE_LOST: {
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error,"
									" not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error\r\n",
							__FUNCTION__, __LINE__);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_BIND: { 
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicBindMessage devBindMsg;
							bool bValidDevice = false;
							bool bValidGuarder = false;
							bool bValidMode = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("factoryId")) {
								if (doc["factoryId"].IsString()) {
									size_t nSize = doc["factoryId"].GetStringLength();
									if (nSize) {
										strncpy_s(devBindMsg.szFactoryId, sizeof(devBindMsg.szFactoryId), 
											doc["factoryId"].GetString(), nSize);
									}
								}
							}
							if (doc.HasMember("deviceId")) {
								if (doc["deviceId"].IsString()) {
									size_t nSize = doc["deviceId"].GetStringLength();
									if (nSize) {
										strncpy_s(devBindMsg.szDeviceId, sizeof(devBindMsg.szDeviceId),
											doc["deviceId"].GetString(), nSize);
										bValidDevice = true;
									}
								}
							}
							if (doc.HasMember("orgId")) {
								if (doc["orgId"].IsString()) {
									size_t nSize = doc["orgId"].GetStringLength();
									if (nSize) {
										strncpy_s(devBindMsg.szOrg, sizeof(devBindMsg.szOrg), doc["orgId"].GetString(), nSize);
									}
								}
							}
							if (doc.HasMember("guarder")) {
								if (doc["guarder"].IsString()) {
									size_t nSize = doc["guarder"].GetStringLength();
									if (nSize) {
										strncpy_s(devBindMsg.szGuarder, sizeof(devBindMsg.szGuarder),
											doc["guarder"].GetString(), nSize);
										bValidGuarder = true;
									}
								}
							}
							if (doc.HasMember("mode")) {
								if (doc["mode"].IsInt()) {
									devBindMsg.usMode = (unsigned short)doc["mode"].GetInt();
									bValidMode = true;
								}
							}
							if (doc.HasMember("battery")) {
								if (doc["battery"].IsInt()) {
									devBindMsg.usBattery = (unsigned short)doc["battery"].GetInt();
								}
							}
							if (doc.HasMember("datetime")) {
								if (doc["datetime"].IsString()) {
									size_t nSize = doc["datetime"].GetStringLength();
									if (nSize) {
										strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
										bValidDatetime = true;
										devBindMsg.ulMessageTime = strdatetime2time(szDatetime);
									}
								}
							}
							if (bValidDevice && bValidGuarder && bValidMode && bValidDatetime) {
								handleTopicDeviceBindMsg(&devBindMsg, pMsg->szMsgMark);
							}
							else {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic device bind message data "
									"miss, factoryId=%s, deviceId=%s, org=%s, guarder=%s, mode=%hu, battery=%hu, datetime=%s,"
									"topic=%s, sequence=%u, uuid=%s, from=%s\r\n", __FUNCTION__, __LINE__, devBindMsg.szFactoryId,
									devBindMsg.szDeviceId, devBindMsg.szOrg, devBindMsg.szGuarder, devBindMsg.usMode,
									devBindMsg.usBattery, szDatetime, pMsg->szMsgMark, pMsg->usMsgSequence, pMsg->szMsgUuid,
									pMsg->szMsgFrom);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic device bind message error\r\n",
								__FUNCTION__, __LINE__);
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					break;
				}
				case PUBMSG_DEVICE_ONLINE: {
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
									strncpy_s(onlineMsg.szOrg, sizeof(onlineMsg.szOrg), doc["orgId"].GetString(), nSize);
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
							"error=%d, \r\n", __FUNCTION__, __LINE__, doc.GetParseError());
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_OFFLINE: {
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
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_TASK: { 
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							int nSubType = -1;
							if (doc.HasMember("subType")) {
								if (doc["subType"].IsInt()) {
									nSubType = doc["subType"].GetInt();
								}
								switch (nSubType) {
									case TASK_OPT_SUBMIT: {
										TopicTaskMessage taskMsg;
										bool bValidTask = false;
										bool bValidDevice = false;
										bool bValidGuarder = false;
										bool bValidDatetime = false;
										char szDatetime[20] = { 0 };
										if (doc.HasMember("taskId")) {
											if (doc["taskId"].IsString()) {
												size_t nSize = doc["taskId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szTaskId, sizeof(taskMsg.szTaskId), doc["taskId"].GetString(), nSize);
													bValidTask = true;
												}
											}
										}
										if (doc.HasMember("factoryId")) {
											if (doc["factoryId"].IsString()) {
												size_t nSize = doc["factoryId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szFactoryId, sizeof(taskMsg.szFactoryId), 
														doc["factoryId"].GetString(), nSize);
												}
											}
										}
										if (doc.HasMember("deviceId")) {
											if (doc["deviceId"].IsString()) {
												size_t nSize = doc["deviceId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szDeviceId, sizeof(taskMsg.szDeviceId), doc["deviceId"].GetString(), nSize);
													bValidDevice = true;
												}
											}
										}
										if (doc.HasMember("orgId")) {
											if (doc["orgId"].IsString()) {
												size_t nSize = doc["orgId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szOrg, sizeof(taskMsg.szOrg), doc["orgId"].GetString(), nSize);
												}
											}
										}
										if (doc.HasMember("guarder")) {
											if (doc["guarder"].IsString()) {
												size_t nSize = doc["guarder"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szGuarder, sizeof(taskMsg.szGuarder), doc["guarder"].GetString(), nSize);
													bValidGuarder = true;
												}
											}
										}
										if (doc.HasMember("taskType")) {
											if (doc["taskType"].IsInt()) {
												taskMsg.usTaskType = (unsigned short)doc["taskType"].GetInt();
											}
										}
										if (doc.HasMember("limit")) {
											if (doc["limit"].IsInt()) {
												taskMsg.usTaskLimit = (unsigned short)doc["limit"].GetInt();
											}
										}
										if (doc.HasMember("destination")) {
											if (doc["destination"].IsString()) {
												size_t nSize = doc["destination"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szDestination, sizeof(taskMsg.szDestination), 
														doc["destination"].GetString(), nSize);
												}
											}
										}
										if (doc.HasMember("target")) {
											if (doc["target"].IsString()) {
												size_t nSize = doc["target"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szTarget, sizeof(taskMsg.szTarget), doc["target"].GetString(), nSize);
													bValidTask = true;
												}
											}
										}
										if (doc.HasMember("handset")) {
											if (doc["handset"].IsString()) {
												size_t nSize = doc["handset"].GetStringLength();
												if (nSize) {
													strncpy_s(taskMsg.szHandset, sizeof(taskMsg.szHandset), doc["handset"].GetString(), nSize);
												}
											}
										}
										if (doc.HasMember("datetime")) {
											if (doc["datetime"].IsString()) {
												size_t nSize = doc["datetime"].GetStringLength();
												if (nSize) {
													strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
													taskMsg.ulMessageTime = strdatetime2time(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidTask && bValidDevice && bValidGuarder && bValidDatetime) {
											handleTopicTaskSubmitMsg(&taskMsg, pMsg->szMsgMark);
										}
										else {
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic task message data miss, "
												"taskId=%s, guarder=%s, deviceId=%s, factoryId=%s, datetime=%s, msgTopic=%s, msgSeq=%hu, "
												"msgUuid=%s, msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskMsg.szTaskId, taskMsg.szGuarder,
												taskMsg.szDeviceId, taskMsg.szFactoryId, szDatetime, pMsg->szMsgMark, pMsg->usMsgSequence,
												pMsg->szMsgUuid, pMsg->szMsgFrom);
											writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										}
										break;
									}
									case TASK_OPT_MODIFY: {
										TopicTaskModifyMessage taskModifyMsg;
										memset(&taskModifyMsg, 0, sizeof(TopicTaskModifyMessage));
										bool bValidTask = false;
										bool bValidDatetime = false;
										char szDatetime[20] = { 0 };
										if (doc.HasMember("taskId")) {
											if (doc["taskId"].IsString()) {
												size_t nSize = doc["taskId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskModifyMsg.szTaskId, sizeof(taskModifyMsg.szTaskId), 
														doc["taskId"].GetString(), nSize);
													bValidTask = true;
												}
											}
										}
										if (doc.HasMember("handset")) {
											if (doc["handset"].IsString()) {
												size_t nSize = doc["handset"].GetStringLength();
												if (nSize) {
													strncpy_s(taskModifyMsg.szHandset, sizeof(taskModifyMsg.szHandset), 
														doc["handset"].GetString(), nSize);
												}
											}
										}
										if (doc.HasMember("datetime")) {
											if (doc["datetime"].IsString()) {
												size_t nSize = doc["datetime"].GetStringLength();
												if (nSize) {
													strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
													taskModifyMsg.ulMessageTime = strdatetime2time(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidTask && bValidDatetime) {
											handleTopicTaskModifyMsg(&taskModifyMsg, pMsg->szMsgMark);
										}
										else {
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic task modify message data "
												"miss, taskId=%s, datetime=%s, handset=%s, msgTopic=%s, msgSeq=%hu, msgUuid=%s, msgFrom=%s"
												"\r\n", __FUNCTION__, __LINE__, taskModifyMsg.szTaskId, szDatetime, taskModifyMsg.szHandset,
												pMsg->szMsgMark, pMsg->usMsgSequence, pMsg->szMsgUuid, pMsg->szMsgFrom);
											writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										}
										break;
									}
									case TASK_OPT_CLOSE: {
										TopicTaskCloseMessage taskCloseMsg;
										memset(&taskCloseMsg, 0, sizeof(TopicTaskCloseMessage));
										bool bValidTask = false;
										bool bValidState = false;
										bool bValidDatetime = false;
										char szDatetime[20] = { 0 };
										if (doc.HasMember("taskId")) {
											if (doc["taskId"].IsString()) {
												size_t nSize = doc["taskId"].GetStringLength();
												if (nSize) {
													strncpy_s(taskCloseMsg.szTaskId, sizeof(taskCloseMsg.szTaskId),
														doc["taskId"].GetString(), nSize);
													bValidTask = true;
												}
											}
										}
										if (doc.HasMember("state")) {
											if (doc["state"].IsInt()) {
												taskCloseMsg.nClose = doc["state"].GetInt();
												bValidState = true;
											}
										}
										if (doc.HasMember("datetime")) {
											if (doc["datetime"].IsString()) {
												size_t nSize = doc["datetime"].GetStringLength();
												if (nSize) {
													strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
													taskCloseMsg.ulMessageTime = strdatetime2time(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidState && bValidDatetime && bValidTask) {
											handleTopicTaskCloseMsg(&taskCloseMsg, pMsg->szMsgMark);
										}
										else {
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse topic task close message "
												"data miss, taskId=%s, state=%d, datetime=%s, msgTopic=%s, msgSeq=%hu, msgUuid=%s, "
												"msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskCloseMsg.szTaskId, taskCloseMsg.nClose, 
												szDatetime, pMsg->szMsgMark, pMsg->usMsgSequence, pMsg->szMsgUuid, pMsg->szMsgFrom);
											writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										}
										break;
									}
									default: {
										snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse task message subType not "
											"support=%d\r\n", __FUNCTION__, __LINE__, nSubType);
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										break;
									}
								}
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse topic task message json error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					break;
				}
				case PUBMSG_BUFFER_MODIFY: {
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
										writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									bool bValidName = false;
									bool bValidPasswd = false;
									bool bValidRoleType = false;
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
									if (doc.HasMember("roleType")) {
										if (doc["roleType"].IsInt()) {
											guarder.usRoleType = (unsigned short)doc["roleType"].GetInt();
											bValidRoleType = true;
										}
									}
									if (bValidGuarder && bValidOrg && bValidDateTime && bValidName && bValidPasswd && bValidRoleType) {
										if (nOperate == BUFFER_OPERATE_NEW) {
											pthread_mutex_lock(&g_mutex4GuarderList);
											Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, guarder.szId);
											if (pGuarder) {
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message, "
													"object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu already"
													" exists in the guarderList\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER, 
													BUFFER_OPERATE_NEW, guarder.szId, guarder.szTagName, guarder.szPassword, guarder.szOrg, 
													guarder.usRoleType);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
												pGuarder->usState = STATE_GUARDER_FREE;
												pGuarder->usRoleType = guarder.usRoleType;
												zhash_update(g_guarderList, pGuarder->szId, pGuarder);
												zhash_freefn(g_guarderList, pGuarder->szId, free);
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message"
													", object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId, 
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
												if (pGuarder->usRoleType != guarder.usRoleType) {
													pGuarder->usRoleType = guarder.usRoleType;
												}
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message"
													", object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE, guarder.szId,
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
											else {
												snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message, "
													"object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, not found in "
													"guarderList\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE,
													guarder.szId, guarder.szTagName, guarder.szPassword, guarder.szOrg);
												writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
											strncpy_s(device.deviceBasic.szFactoryId, sizeof(device.deviceBasic.szFactoryId), 
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
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										device.deviceBasic.nBattery = (unsigned char)doc["battery"].GetInt();
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
									strncpy_s(szDevKey, sizeof(szDevKey), device.deviceBasic.szDeviceId, 
										strlen(device.deviceBasic.szDeviceId));
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
											writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
											writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
											pDevice->deviceBasic.nBattery = device.deviceBasic.nBattery;
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message, "
												"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, battery=%u, datetime="
												"%s\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, BUFFER_OPERATE_UPDATE,
												device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
												device.szOrganization, device.deviceBasic.nBattery, szDatetime);
											writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										}
										else {
											snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
												"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, battery=%u,datetime=%s,"
												" not find int the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, 
												BUFFER_OPERATE_UPDATE, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
												device.szOrganization, device.deviceBasic.nBattery, szDatetime);
											writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
									"object=%d not support, seq=%u\r\n", __FUNCTION__, __LINE__, nObject, 
									pMsg->usMsgSequence);
								writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic buffer modify message"
							"error, seq=%u, \r\n", __FUNCTION__, __LINE__, pMsg->usMsgSequence);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGIN: {
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicLoginMessage loginMsg;
							bool bValidAccount = false;
							bool bValidSession = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("account")) {
								if (doc["account"].IsString()) {
									size_t nSize = doc["account"].GetStringLength();
									if (nSize) {
										strncpy_s(loginMsg.szAccount, sizeof(loginMsg.szAccount), doc["account"].GetString(), nSize);
										bValidAccount = true;
									}
								}
							}
							if (doc.HasMember("session")) {
								size_t nSize = doc["session"].GetStringLength();
								if (nSize) {
									strncpy_s(loginMsg.szSession, sizeof(loginMsg.szSession), doc["session"].GetString(), nSize);
									bValidSession = true;
								}
							}
							if (doc.HasMember("handset")) {
								size_t nSize = doc["handset"].GetStringLength();
								if (nSize) {
									strncpy_s(loginMsg.szHandset, sizeof(loginMsg.szHandset), doc["handset"].GetString(), nSize);
								}
							}
							if (doc.HasMember("datetime")) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									loginMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
							if (bValidAccount && bValidSession && bValidDatetime) {
								handleTopicLoginMsg(&loginMsg, pMsg->szMsgMark);
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic login message error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGOUT: {
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicLogoutMessage logoutMsg;
							bool bValidAccount = false;
							bool bValidSession = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("account")) {
								if (doc["account"].IsString()) {
									size_t nSize = doc["account"].GetStringLength();
									if (nSize) {
										strncpy_s(logoutMsg.szAccount, sizeof(logoutMsg.szAccount), doc["account"].GetString(), nSize);
										bValidAccount = true;
									}
								}
							}
							if (doc.HasMember("session")) {
								size_t nSize = doc["session"].GetStringLength();
								if (nSize) {
									strncpy_s(logoutMsg.szSession, sizeof(logoutMsg.szSession), doc["session"].GetString(), nSize);
									bValidSession = true;
								}
							}
							if (doc.HasMember("handset")) {
								size_t nSize = doc["handset"].GetStringLength();
								if (nSize) {
									strncpy_s(logoutMsg.szHandset, sizeof(logoutMsg.szHandset), doc["handset"].GetString(), nSize);
								}
							}
							if (doc.HasMember("datetime")) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									logoutMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
							if (bValidAccount && bValidSession && bValidDatetime) {
								handleTopicLogoutMsg(&logoutMsg, pMsg->szMsgMark);
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic login message error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
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
													if (doc["msg"][0]["msgUuid"].IsString()) {
														size_t nSize = doc["msg"][0]["msgUuid"].GetStringLength();
														if (nSize) {
															strncpy_s(topicMsg.szMsgUuid, sizeof(topicMsg.szMsgUuid),
																doc["msg"][0]["msgUuid"].GetString(), nSize);
															bValidUuid = true;
														}
													}
												}
												if (doc["msg"][0].HasMember("msgBody")) {
													if (doc["msg"][0].IsObject()) {
														char szMsgBody[512] = { 0 };
														int nIndex = 0;
														for (rapidjson::Value::ConstMemberIterator iter = doc["msg"][0]["msgBody"].MemberBegin();
															iter != doc["msg"][0]["msgBody"].MemberEnd(); ++iter) {
															if (iter->value.IsString()) {
																char szCell[128] = { 0 };
																if (nIndex == 0) {
																	sprintf_s(szCell, sizeof(szCell), "{\"%s\":\"%s\"", iter->name.GetString(),
																		iter->value.GetString());
																}
																else {
																	sprintf_s(szCell, sizeof(szCell), ",\"%s\":\"%s\"", iter->name.GetString(),
																		iter->value.GetString());
																}
																strncat_s(szMsgBody, sizeof(szMsgBody), szCell, strlen(szCell));
																nIndex++;
															}
															else if (iter->value.IsInt()) {
																size_t nSize = iter->name.GetStringLength() + sizeof(int) + 8;
																char szCell[128] = { 0 };
																if (nIndex == 0) {
																	sprintf_s(szCell, sizeof(szCell), "{\"%s\":%d", iter->name.GetString(), 
																		iter->value.GetInt());
																}
																else {
																	sprintf_s(szCell, sizeof(szCell), ",\"%s\":%d", iter->name.GetString(), 
																		iter->value.GetInt());
																}
																strncat_s(szMsgBody, sizeof(szMsgBody), szCell, strlen(szCell));
																nIndex++;
															}
															else if (iter->value.IsDouble()) {
																char szCell[128] = { 0 };
																if (nIndex == 0) {
																	sprintf_s(szCell, sizeof(szCell), "{\"%s\":%f", iter->name.GetString(),
																		iter->value.GetDouble());
																}
																else {
																	sprintf_s(szCell, sizeof(szCell), ",\"%s\":%f", iter->name.GetString(),
																		iter->value.GetDouble());
																}
																strncat_s(szMsgBody, sizeof(szMsgBody), szCell, strlen(szCell));
																nIndex++;
															}
														}
														if (strlen(szMsgBody) && nIndex > 0) {
															strcat_s(szMsgBody, sizeof(szMsgBody), "}");
															strncpy_s(topicMsg.szMsgBody, sizeof(topicMsg.szMsgBody), szMsgBody, strlen(szMsgBody));
															bValidMsg = true;
														}
													}
												}
												if (bValidMsg && bValidUuid) {
													size_t nSize = sizeof(TopicMessage);
													TopicMessage * pMsg = (TopicMessage *)zmalloc(nSize);
													memcpy_s(pMsg, nSize, &topicMsg, nSize);
													if (!addTopicMsg(pMsg)) {
														free(pMsg);
														pMsg = NULL;
													}
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
									writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									break;
								}
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, "
								"json data miss parameter, type=%d, sequence=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, bValidType ? nType : -1, bValidSeq ? nSequence : -1, 
								bValidTime ? szDatetime : "null");
							writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, "
							"verify message head failed\r\n", __FUNCTION__, __LINE__);
						writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
				}
				else {
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, JSON"
						" data parse error: %s\r\n", __FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
					writeLog(szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
				}
			}
			for (unsigned int i = 0; i < pMsg->uiContentCount; i++) {
				if (pMsg->pMsgContents[i]) {
					free(pMsg->pMsgContents[i]);
					pMsg->pMsgContents[i] = NULL;
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
	char szLog[512] = { 0 };
	if (pMsg) {
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic online msg for %s, dev=%s\r\n",
			__FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		unsigned short usBattery = 0;
		unsigned short usLoose = 0;
		double dLat = 0.00, dLng = 0.00;
		if (strlen(pMsg->szDeviceId) > 0) {
			char szGuarder[20] = { 0 };
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
						if (pDev->deviceBasic.nLooseStatus == 1) {
							changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus);
						}
						if (strlen(pDev->szBindGuard)) {
							strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
						}
						dLat = pDev->devicePosition.dLatitude;
						dLng = pDev->devicePosition.dLngitude;
						usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
						result = E_OK;
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			char szTaskId[20] = { 0 };
			int nTaskState = 0;
			if (strlen(szGuarder)) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strlen(pGuarder->szTaskId) && pGuarder->usState == STATE_GUARDER_DUTY) {
						strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (strlen(szTaskId)) {
					pthread_mutex_lock(&g_mutex4TaskList);
					EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
					if (pTask) {
						if (pTask->nTaskFlee == 1) {
							nTaskState = 2;
						}
						else {
							nTaskState = 1;
						}
					}
					pthread_mutex_unlock(&g_mutex4TaskList);
				}
			}
			if (nTaskState > 0) {
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
				if (pDev) {
					if (nTaskState == 1) {
						changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
					}
					else if (nTaskState == 2) {
						changeDeviceStatus(DEV_FLEE, pDev->deviceBasic.nStatus);
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		}
		if (pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
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
					strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
					"\"battery\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
					access_service::E_NOTIFY_DEVICE_ONLINE, pMsg->szDeviceId, usBattery, szDatetime);
				if (strlen(szMsg)) {
					bool bNeedNotify = true;
					if (strlen(szEndpoint)) {
						if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s,"
								" battery=%u, datetime=%s, notify device online\r\n", __FUNCTION__, __LINE__, szEndpoint,
								szSession, pMsg->szDeviceId, usBattery, szDatetime);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							bNeedNotify = false;
						}
						else {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s,"
								" battery=%u, datetime=%s, notify device online failed\r\n", __FUNCTION__, __LINE__, szEndpoint,
								szSession, pMsg->szDeviceId, usBattery, szDatetime);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
					}
					if (strlen(szSession)) {
						bool bNotifyPosition = false;
						bool bNotifyStatus = false;
						pthread_mutex_lock(&m_mutex4LinkList);
						if (zhash_size(m_linkList)) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
							if (pLink) {
								if (bNeedNotify) {
									pLink->nNotifyBattery = 1;
								}
								else {
									pLink->nNotifyOnline = 0;
									pLink->nNotifyBattery = 0;
									if (pLink->nNotifyPosition) {
										pLink->nNotifyPosition = 0;
										bNotifyPosition = true;
									}
									if (pLink->nNotifyStatus) {
										pLink->nNotifyStatus = 0;
										bNotifyStatus = true;
									}
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
						if (bNotifyPosition) {
							if (dLat > 0.0 && dLng > 0.0) {
								char szMsg[256] = { 0 };
								sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
									",\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
									szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery, dLat, dLng,
									szDatetime);
								sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
							}
						}
						if (bNotifyStatus) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
								",\"mode\":%d,\"battery\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
								access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, (usLoose == 1) ? 0 : 1, usBattery,
								szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicOfflineMsg(TopicOfflineMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[512] = { 0 };
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic offline msg for %s, dev=%s\r\n",
				__FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId);
			writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			bool bValidMsg = false;
			double dLat = 0.00, dLng = 0.00;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					changeDeviceStatus(DEV_OFFLINE, pDev->deviceBasic.nStatus);
					dLat = pDev->devicePosition.dLatitude;
					dLng = pDev->devicePosition.dLngitude;
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
						bool bNotify = true;
						if (strlen(szEndpoint)) {
							if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
								char szLog[512] = { 0 };
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, "
									"deviceId=%s, datetime=%s, notify device offline\r\n", __FUNCTION__, __LINE__, szEndpoint,
									szSession, pMsg->szDeviceId, szDatetime);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								bNotify = false;
							}
							else {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to endpoint=%s, session=%s, deviceId=%s"
									", datetime=%s, notify device offline failed\r\n", __FUNCTION__, __LINE__, szEndpoint,
									szSession, pMsg->szDeviceId, szDatetime);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
						}
						if (strlen(szSession)) {
							bool bNotifyPosition = false;
							pthread_mutex_lock(&m_mutex4LinkList);
							if (zhash_size(m_linkList)) {
								access_service::AppLinkInfo * pLinkInfo = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
									szSession);
								if (pLinkInfo) {
									if (bNotify) {
										pLinkInfo->nNotifyOnline = 1;
									}
									else {
										pLinkInfo->nNotifyBattery = 0;
										pLinkInfo->nNotifyStatus = 0;
										if (pLinkInfo->nNotifyPosition) {
											pLinkInfo->nNotifyPosition = 0;
											bNotifyPosition = true;
										}
									}
								}
							}
							pthread_mutex_unlock(&m_mutex4LinkList);
							if (bNotifyPosition) {
								if (dLat > 0.00 && dLng > 0.00) {
									char szMsg[256] = { 0 };
									sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\""
										":\"%s\",\"battery\":0,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}",
										access_service::E_CMD_MSG_NOTIFY, szSession, access_service::E_NOTIFY_DEVICE_OFFLINE,
										pMsg->szDeviceId, dLat, dLng, szDatetime);
									sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
								}
							}
						}
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
	char szLog[256] = { 0 };
	if (pMsg) {
		bool bValidMsg = false;
		unsigned short usBattery = 0;
		unsigned short usLoose = 0;
		double dLat = 0.00, dLng = 0.00;
		if (strlen(pMsg->szDeviceId) > 0) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
					}
					usBattery = (unsigned short)pDev->deviceBasic.nBattery;
					usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
					bValidMsg = true;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
						if (usBattery < BATTERY_THRESHOLD) {
							changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus, 0);
						}
						if (pDev->deviceBasic.nLooseStatus == 1) {
							changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus, 0);
						}
					}
					dLat = pDev->devicePosition.dLatitude;
					dLng = pDev->devicePosition.dLngitude;
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
					if (strlen(pSubInfo->szSession) && strlen(pSubInfo->szEndpoint)) {
						bFindSub = true;
						strncpy_s(szSession, sizeof(szSession), pSubInfo->szSession, strlen(pSubInfo->szSession));
						strncpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint, strlen(pSubInfo->szEndpoint));
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				bool bSend = false;
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\""
					":\"%s\",\"battery\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
					access_service::E_NOTIFY_DEVICE_BATTERY, pMsg->szDeviceId, usBattery, szDatetime);
				if (strlen(szMsg)) {
					if (strlen(szEndpoint)) {
						if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s,"
								" battery=%d, datetime=%s, notify device battery\r\n", __FUNCTION__, __LINE__, szEndpoint,
								szSession, pMsg->szDeviceId, usBattery, szDatetime);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							bSend = true;
						}
					}
				}
				if (strlen(szSession)) {
					bool bNotifyStatus = false;
					bool bNotifyPosition = false;
					pthread_mutex_lock(&m_mutex4LinkList);
					if (zhash_size(m_linkList)) {
						access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
							szSession);
						if (pLink) {
							if (!bSend) {
								pLink->nNotifyBattery = 1;
							}
							else {
								pLink->nNotifyBattery = 0;
								pLink->nNotifyOnline = 0;
								if (pLink->nNotifyStatus) {
									pLink->nNotifyStatus = 0;
									bNotifyStatus = true;
								}
								if (pLink->nNotifyPosition) {
									pLink->nNotifyPosition = 0;
									bNotifyPosition = true;
								}
							}
						}
					}
					pthread_mutex_unlock(&m_mutex4LinkList);
					if (bNotifyStatus) {
						char szMsg[256] = { 0 };
						sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
							"\"battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
							access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, usBattery, (usLoose == 1) ? 0 : 1, 
							szDatetime);
						sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
					}
					if (bNotifyPosition) {
						if (dLat > 0.00 && dLng > 0.00) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
								"\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
								szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery, dLat, dLng,
								szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicLocateGpsMsg(TopicLocateMessageGps * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[512] = { 0 };
	if (pMsg && pMsg->dLat > 0 && pMsg->dLng > 0 && pMsg->usFlag == 1) {
		if (strlen(pMsg->szDeviceId)) {
			bool bValidMsg = false;
			unsigned short usBattery = 0;
			unsigned short usLoose = 0;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
					}
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) || 
						(pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						bValidMsg = true;
					}
					pDev->devicePosition.dLatitude = pMsg->dLat;
					pDev->devicePosition.dLngitude = pMsg->dLng;
					pDev->devicePosition.usLatType = pMsg->usLatType;
					pDev->devicePosition.usLngType = pMsg->usLngType;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
					}
					usBattery = (unsigned short)pDev->deviceBasic.nBattery;
					usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[20] = { 0 };
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
					bool bSend = false;
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery,
						pMsg->dLat, pMsg->dLng, szDatetime);
					if (strlen(szMsg)) {
						if (strlen(szEndpoint)) {
							if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, "
									"deviceId=%s, battery=%u, lat=%f, lng=%f, datetime=%s, notify device position\r\n",
									__FUNCTION__, __LINE__, szEndpoint, szSession, pMsg->szDeviceId, usBattery, pMsg->dLat, 
									pMsg->dLng, szDatetime);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								bSend = true;
							}
						}
					}
					if (strlen(szSession)) {
						bool bNotifyStatus = false;
						pthread_mutex_lock(&m_mutex4LinkList);
						if (zhash_size(m_linkList)) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
								szSession);
							if (pLink) {
								if (!bSend) {
									pLink->nNotifyPosition = 1;
								}
								else {
									pLink->nNotifyPosition = 0;
									pLink->nNotifyOnline = 0;
									pLink->nNotifyBattery = 0;
									if (pLink->nNotifyStatus) {
										pLink->nNotifyStatus = 0;
										bNotifyStatus = true;
									}
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
						if (bNotifyStatus) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
								",\"battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
								szSession, access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, usBattery,
								(usLoose == 1) ? 0 : 1, szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
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
	char szLog[512] = { 0 };
	if (pMsg && pMsg->usFlag == 1 && pMsg->dLat > 0 && pMsg->dLng > 0) {
		if (strlen(pMsg->szDeviceId) > 0) {
			unsigned short usBattery = 0;
			unsigned short usLoose = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
						changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
					}
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD || 
						(pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						bValidMsg = true;
					}
					//this code would open if LBS position is correct 
					pDev->devicePosition.dLatitude = pMsg->dLat;
					pDev->devicePosition.dLngitude = pMsg->dLng;
					pDev->devicePosition.usLatType = pMsg->usLatType;
					pDev->devicePosition.usLngType = pMsg->usLngType;
					pDev->devicePosition.nPrecision = pMsg->nPrecision;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
					}
					usBattery = (unsigned short)pDev->deviceBasic.nBattery;
					usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szSession[20] = { 0 };
				char szEndpoint[40] = { 0 };
				char szDatetime[20] = { 0 };
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
				if (bNotifyMsg) {
					bool bSend = false;
					char szMsg[512] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"lat\":%.06f,\"lng\":%.06f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery,
						pMsg->dLat, pMsg->dLng, szDatetime);
					if (strlen(szEndpoint) > 0) {
						if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s"
								", battery=%u, lat=%f, lng=%f, datetime=%s, notify device position\r\n", __FUNCTION__,
								__LINE__, szEndpoint, szSession, pMsg->szDeviceId, usBattery, pMsg->dLat, pMsg->dLng,
								szDatetime);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							printf(szLog);
							bSend = true;
						} 
					}
					if (strlen(szSession)) {
						bool bNotifyBattery = false;
						bool bNotifyStatus = false;
						bool bNotifyOffline = false;
						pthread_mutex_lock(&m_mutex4LinkList);
						if (zhash_size(m_linkList)) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
								szSession);
							if (pLink) {
								if (!bSend) {
									pLink->nNotifyPosition = 1;
								}
								else {
									pLink->nNotifyPosition = 0;
									if (pLink->nNotifyOnline) {
										pLink->nNotifyOnline = 0;
										bNotifyOffline = true;
									}
									if (pLink->nNotifyBattery) {
										pLink->nNotifyBattery = 0;
										bNotifyBattery = true;
									}
									if (pLink->nNotifyStatus) {
										pLink->nNotifyStatus = 0;
										bNotifyStatus = true;
									}
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
						if (bNotifyOffline) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":"
								"\"%s\",\"battery\":0,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
								access_service::E_NOTIFY_DEVICE_OFFLINE, pMsg->szDeviceId, szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
						if (bNotifyBattery) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":"
								"\"%s\",\"battery\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
								access_service::E_NOTIFY_DEVICE_BATTERY, pMsg->szDeviceId, usBattery, szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
						if (bNotifyStatus) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":"
								"\"%s\",\"battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
								szSession, access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, usBattery,
								(usLoose == 1) ? 0 : 1, szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAlarmLowpowerMsg(TopicAlarmMessageLowpower * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[512] = { 0 };
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			unsigned short usBattery = 0;
			bool bValidMsg = false;
			unsigned short usLoose = 0;
			double dLat = 0.0, dLng = 0.0;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
						pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
						if (pMsg->usBattery > 0) {
							pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
						}
						usBattery = (unsigned short)pDev->deviceBasic.nBattery;
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
						dLat = pDev->devicePosition.dLatitude;
						dLng = pDev->devicePosition.dLngitude;
						usLoose = (unsigned short)pDev->deviceBasic.nLooseStatus;
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
					bool bSend = false;
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_ALARM_DEVICE_LOWPOWER, pMsg->szDeviceId, pMsg->usBattery,
						pMsg->usMode, szDatetime);
					if (strlen(szMsg)) {
						if (strlen(szEndpoint) > 0) {
							if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s"
									", battery=%d, mode=%u, datetime=%s, alarm device lowpower\r\n", __FUNCTION__, __LINE__,
									szEndpoint, szSession, pMsg->szDeviceId, pMsg->usBattery, pMsg->usMode, szDatetime);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								bSend = true;
							}
						}
					}
					if (strlen(szSession)) {
						bool bNotifyStatus = false;
						bool bNotifyPosition = false;
						pthread_mutex_lock(&m_mutex4LinkList);
						if (zhash_size(m_linkList)) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
								szSession);
							if (pLink) {
								if (!bSend) {
									pLink->nNotifyBattery = 1;
								}
								else {
									pLink->nNotifyBattery = 0;
									pLink->nNotifyOnline = 0;
									if (pLink->nNotifyStatus) {
										bNotifyStatus = true;
										pLink->nNotifyStatus = 0;
									}
									if (pLink->nNotifyPosition) {
										bNotifyPosition = true;
										pLink->nNotifyStatus = 0;
									}
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
						if (bNotifyStatus) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
								"\"battery\":%hu,\"mode\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
								access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, usBattery, 
								(usLoose == 1) ? 0 : 1, szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
						if (bNotifyPosition) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
								"\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
								szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery, dLat, dLng,
								szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
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
	char szLog[512] = { 0 };
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]deal topic loose msg for %s, dev=%s, mode=%hu\r\n",
				__FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId, pMsg->usMode);
			writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			unsigned short usBattery = 0;
			double dLat = 0.0, dLng = 0.0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
						pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
						result = E_OK;
						if (pMsg->usBattery > 0) {
							pDev->deviceBasic.nBattery = (unsigned char)pMsg->usBattery;
						}
						usBattery = pDev->deviceBasic.nBattery;
						if (pDev->deviceBasic.nLooseStatus == 1) {
							if (pMsg->usMode == 1) { //off
								changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus, 1);
								pDev->deviceBasic.nLooseStatus = 0;
								bValidMsg = true;
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]change status: loose revoke, %s\r\n",
									__FUNCTION__, __LINE__, pMsgSubstitle);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
						}
						else {
							if (pMsg->usMode == 0) { //on
								changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus, 0);
								pDev->deviceBasic.nLooseStatus = 1;
								bValidMsg = true;
								snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]change status: loose, %s\r\n",
									__FUNCTION__, __LINE__, pMsgSubstitle);
								writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
					else {
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]not find subscriber for %s\r\n",
							__FUNCTION__, __LINE__, pMsgSubstitle);
						writeLog(szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				if (bNotifyMsg) {
					bool bSend = false;
					char szMsg[256] = { 0 };
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\""
						",\"battery\":%u,\"mode\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession,
						access_service::E_ALARM_DEVICE_LOOSE, pMsg->szDeviceId, pMsg->usBattery, pMsg->usMode, szDatetime);
					if (strlen(szEndpoint) > 0) {
						if (sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint) == 0) {
							snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, deviceId=%s"
								", battery=%u, mode=%u, datetime=%s, alarm device loose\r\n", __FUNCTION__, __LINE__, szEndpoint,
								szSession, pMsg->szDeviceId, pMsg->usBattery, pMsg->usMode, szDatetime);
							writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							bSend = true;
						}
					}
					if (strlen(szSession)) {
						bool bNotifyPosition = false;
						pthread_mutex_lock(&m_mutex4LinkList);
						access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
						if (pLink) {
							if (!bSend) {
								pLink->nNotifyStatus = 1;
							}
							else {
								pLink->nNotifyStatus = 0;
								pLink->nNotifyBattery = 0;
								pLink->nNotifyOnline = 0;
								if (pLink->nNotifyPosition) {
									pLink->nNotifyPosition = 0;
									bNotifyPosition = true;
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
						if (bNotifyPosition) {
							char szMsg[256] = { 0 };
							sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
								"\"battery\":%hu,\"lat\":%f,\"lng\":%f,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
								szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, usBattery, dLat, dLng,
								szDatetime);
							sendDatatoEndpoint(szMsg, strlen(szMsg), szEndpoint);
						}
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAlarmFleeMsg(TopicAlarmMessageFlee * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	char szLog[512] = { 0 };
	if (pMsg_) {
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic alarm flee for %s, dev=%s, mode=%hu\r\n",
			__FUNCTION__, __LINE__, pMsgTopic_, pMsg_->szDeviceId, pMsg_->usMode);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (strlen(pMsg_->szDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
				if (pDevice) {
					changeDeviceStatus(DEV_FLEE, pDevice->deviceBasic.nStatus, pMsg_->usMode);
					pDevice->ulLastFleeAlertTime = pMsg_->ulMessageTime;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (strlen(pMsg_->szTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskId);
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pMsg_->szTaskId);
			if (pTask) {
				pTask->nTaskFlee = ((pMsg_->usMode == 0) ? 1 : 0);
			}
			pthread_mutex_unlock(&g_mutex4TaskId);
		}
	}
	return result;
}

int AccessService::handleTopicTaskSubmitMsg(TopicTaskMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szTaskId) && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szGuarder)) {
			bool bExists = false;
			EscortTask * pTask = (EscortTask *)zmalloc(sizeof(EscortTask));
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = (uint8_t)pMsg_->usTaskLimit;
			pTask->nTaskState = 0;
			pTask->nTaskType = (uint8_t)pMsg_->usTaskType;
			strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), pMsg_->szDestination, 
				strlen(pMsg_->szDestination));
			strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pMsg_->szDeviceId,
				strlen(pMsg_->szDeviceId));
			strncpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pMsg_->szFactoryId,
				strlen(pMsg_->szFactoryId));
			strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pMsg_->szGuarder, strlen(pMsg_->szGuarder));
			strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset, strlen(pMsg_->szHandset));
			strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), pMsg_->szOrg, strlen(pMsg_->szOrg));
			strncpy_s(pTask->szTarget, sizeof(pTask->szTarget), pMsg_->szTarget, strlen(pMsg_->szTarget));
			strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pMsg_->szTaskId, strlen(pMsg_->szTaskId));
			format_datetime(pMsg_->ulMessageTime, pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime));
			pTask->szTaskStopTime[0] = '\0';
			if (strlen(pTask->szHandset)) {
				pTask->nTaskMode = 1; 
			}
			else {
				pTask->nTaskMode = 0;
			}
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pData = (EscortTask *)zhash_lookup(g_taskList, pTask->szTaskId);
				if (pData) {
					bExists = true;
					free(pTask);
					pTask = NULL;
				}
				else {
					zhash_update(g_taskList, pTask->szTaskId, pTask);
					zhash_freefn(g_taskList, pTask->szTaskId, free);
				}
			}
			else {
				zhash_update(g_taskList, pTask->szTaskId, pTask);
				zhash_freefn(g_taskList, pTask->szTaskId, free);
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
			if (!bExists) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szGuarder);
				if (pGuarder) {
					strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pMsg_->szTaskId, strlen(pMsg_->szTaskId));
					format_datetime(pMsg_->ulMessageTime, pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime));
					pGuarder->usState = STATE_GUARDER_DUTY;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
				if (pDevice) {
					changeDeviceStatus(DEV_GUARD, pDevice->deviceBasic.nStatus);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		}
	}
	return result;
}

int AccessService::handleTopicTaskModifyMsg(TopicTaskModifyMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szTaskId) > 0) {
			pthread_mutex_lock(&g_mutex4TaskList);
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pMsg_->szTaskId);
			if (pTask) {
				if (pTask->nTaskMode == 0) {
					if (strlen(pMsg_->szHandset)) {
						strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset, strlen(pMsg_->szHandset));
						pTask->nTaskMode = 1;
					}
				}
				else {
					if (strlen(pMsg_->szHandset) == 0) {
						pTask->szHandset[0] = '\0';
						pTask->nTaskMode = 0;
					}
					else {
						if (strcmp(pTask->szHandset, pMsg_->szHandset) != 0) {
							strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset, strlen(pMsg_->szHandset));
						}
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
		}
	}
	return result;
}

int AccessService::handleTopicTaskCloseMsg(TopicTaskCloseMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szTaskId)) {
			char szGuarder[20] = { 0 };
			char szDeviceId[20] = { 0 };
			pthread_mutex_lock(&g_mutex4TaskList);
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pMsg_->szTaskId);
			if (pTask) {
				strncpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder, strlen(pTask->szGuarder));
				strncpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId, strlen(pTask->szDeviceId));
				zhash_delete(g_taskList, pMsg_->szTaskId);
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
			if (strlen(szGuarder)) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->szTaskId[0] = '\0';
					pGuarder->szTaskStartTime[0] = '\0';
					pGuarder->usState = STATE_GUARDER_BIND;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}
			if (strlen(szDeviceId)) {
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					changeDeviceStatus(DEV_ONLINE, pDevice->deviceBasic.nStatus, 0);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		}
	}
	return result;
}

int AccessService::handleTopicDeviceBindMsg(TopicBindMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szDeviceId) > 0 && strlen(pMsg_->szGuarder)) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
			if (pDevice) {
				if (pMsg_->usMode == 0) {
					strncpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), pMsg_->szGuarder, 
						strlen(pMsg_->szGuarder));
					pDevice->ulBindTime = pMsg_->ulMessageTime;
				}
				else {
					pDevice->szBindGuard[0] = '\0';
					pDevice->ulBindTime = 0;
				}
				pDevice->deviceBasic.nBattery = (unsigned char)pMsg_->usBattery;
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szGuarder);
			if (pGuarder) {
				if (pMsg_->usMode == 0) {
					strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pMsg_->szDeviceId);
					pGuarder->usState = STATE_GUARDER_BIND;
				}
				else {
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->usState = STATE_GUARDER_FREE;
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
	return result;
}

int AccessService::handleTopicLoginMsg(TopicLoginMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szSession) && strlen(pMsg_->szAccount)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szAccount);
			if (pGuarder) {
				strncpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), pMsg_->szSession, 
					strlen(pMsg_->szSession));
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
	return result;
}

int AccessService::handleTopicLogoutMsg(TopicLogoutMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szSession) && strlen(pMsg_->szAccount)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szAccount);
			if (pGuarder) {
				pGuarder->szCurrentSession[0] = '\0';
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
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
		zoo_acreate(m_zkHandle, "/escort/session", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, zk_session_create_completion, this);
	}
}

int AccessService::competeForMaster()
{
	if (m_zkHandle) {
		while (1) {
			if (m_bConnectZk) {
				char * path = make_zkpath(2, ESCORT_ACCESS_PATH, "master");
				int ret = zoo_acreate(m_zkHandle, path, "", 1024, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
					zk_access_master_create_completion, this);
				free(path);
				break;
			}
			else {
				if (!m_nRun) {
					break;
				}
				Sleep(200);
			}
		}
		//if (ret == 0) {
		//	loadSessionList(); //加载session到list中
		//	return 0;
		//}
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
		const char * pBuf = szBuf;
		int ret = zoo_aset(m_zkHandle, path, pBuf, data_size, -1, zk_access_set_completion, this);
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

void AccessService::zkAddSession(const char * pSession_)
{
	if (pSession_ && strlen(pSession_)) {
		access_service::AppLinkInfo * pLinkInfo = NULL;
		size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pSession_);
			if (pLink) {
				pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
				memcpy_s(pLinkInfo, nLinkInfoSize, pLink, nLinkInfoSize);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (pLinkInfo) {
			if (m_zkHandle) {
				char szBuf[1024] = { 0 };
				memcpy_s(szBuf, sizeof(szBuf), pLinkInfo, nLinkInfoSize);
				char szPath[128] = { 0 };
				sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", pLinkInfo->szSession);
				const char * pBuf = szBuf;
				ZKSessionNode * pNode = new ZKSessionNode();
				strncpy_s(pNode->szSession, sizeof(pNode->szSession), pLinkInfo->szSession, strlen(pLinkInfo->szSession));
				pNode->data = this;
				const void * pSessionNode = pNode;
				zoo_acreate(m_zkHandle, szPath, pBuf, 1024, &ZOO_OPEN_ACL_UNSAFE, 0, zk_session_child_create_completion,
					pSessionNode);
			}
			free(pLinkInfo);
			pLinkInfo = NULL;
		}
	}
}

void AccessService::zkRemoveSession(const char * pSession_)
{
	if (m_zkHandle && pSession_ && strlen(pSession_)) {
		char szFullPath[128] = { 0 };
		snprintf(szFullPath, sizeof(szFullPath), "/escort/session/%s", pSession_);
		zoo_adelete(m_zkHandle, szFullPath, -1, zk_session_child_delete_completion, this);
	}
}

void AccessService::zkSetSession(const char * pSession_)
{
	if (pSession_ && strlen(pSession_)) {
		access_service::AppLinkInfo * pLinkInfo = NULL;
		size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pSession_);
			if (pLink) {
				pLinkInfo = new access_service::AppLinkInfo();
				memcpy_s(pLinkInfo, nLinkInfoSize, pLink, nLinkInfoSize);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (pLinkInfo) {
			if (m_zkHandle) {
				char szBuf[1024] = { 0 };
				memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
				char szPath[128] = { 0 };
				sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", pSession_);
				const char * pBuf = szBuf;
				ZKSessionNode * pNode = new ZKSessionNode();
				strncpy_s(pNode->szSession, sizeof(pNode->szSession), pSession_, strlen(pSession_));
				pNode->data = this;
				const void * pSessionNode = pNode;
				zoo_aset(m_zkHandle, szPath, pBuf, 1024, -1, zk_session_child_set_completion, pSessionNode);
			}
		}
	}
}

void AccessService::loadSessionList()
{
	if (m_zkHandle) {
		struct String_vector childrenList;
		zoo_wget_children(m_zkHandle, "/escort/session", zk_session_get_children_watcher, NULL, &childrenList);
		if (childrenList.count > 0) {
			char szLog[512] = { 0 };
			for (int i = 0; i < childrenList.count; i++) {
				char szChildFullPath[128] = { 0 };
				sprintf_s(szChildFullPath, sizeof(szChildFullPath), "/escort/session/%s", childrenList.data[i]);
				char szBuf[512] = { 0 };
				int nBufLen = 512;
				size_t nAppLinkInfoSize = sizeof(access_service::AppLinkInfo);
				Stat stat;
				if (zoo_get(m_zkHandle, szChildFullPath, -1, szBuf, &nBufLen, &stat) == ZOK) {
					access_service::AppLinkInfo * pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nAppLinkInfoSize);
					memcpy_s(pLinkInfo, nAppLinkInfoSize, szBuf, nAppLinkInfoSize);
					pthread_mutex_lock(&m_mutex4LinkList);
					zhash_update(m_linkList, pLinkInfo->szSession, pLinkInfo);
					zhash_freefn(m_linkList, pLinkInfo->szSession, free);
					pthread_mutex_unlock(&m_mutex4LinkList);
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]load session=%s, guarder=%s, device=%s, org=%s,"
						" taskId=%s, handset=%s\r\n", __FUNCTION__, __LINE__, pLinkInfo->szSession, pLinkInfo->szGuarder,
						pLinkInfo->szDeviceId, pLinkInfo->szOrg, pLinkInfo->szTaskId, pLinkInfo->szHandset);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					pthread_mutex_lock(&g_mutex4GuarderList);
					if (zhash_size(g_guarderList)) {
						Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLinkInfo->szGuarder);
						if (pGuarder) {
							strncpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), pLinkInfo->szSession,
								strlen(pLinkInfo->szSession));
						}
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
					if (strlen(pLinkInfo->szDeviceId) && strlen(pLinkInfo->szFactoryId) && strlen(pLinkInfo->szOrg)) {
						char szTopicFilter[64] = { 0 };
						snprintf(szTopicFilter, sizeof(szTopicFilter), "%s_%s_%s", pLinkInfo->szOrg, pLinkInfo->szFactoryId,
							pLinkInfo->szDeviceId);
						size_t nSubInfoSize = sizeof access_service::AppSubscirbeInfo;
						access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zmalloc(nSubInfoSize);
						strncpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopicFilter, strlen(szTopicFilter));
						if (strlen(pLinkInfo->szEndpoint)) {
							strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pLinkInfo->szEndpoint);
						}
						else {
							pSubInfo->szEndpoint[0] = '\0';
						}
						strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pLinkInfo->szGuarder);
						strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), pLinkInfo->szSession);
						pthread_mutex_lock(&m_mutex4SubscribeList);
						zhash_update(m_subscribeList, szTopicFilter, pSubInfo);
						zhash_freefn(m_subscribeList, szTopicFilter, free);
						pthread_mutex_unlock(&m_mutex4SubscribeList);
					}
				}
			}
		}
		setLoadSessionFlag(true);
	}
}

bool AccessService::getLoadSessionFlag()
{
	bool result;
	pthread_mutex_lock(&m_mutex4LoadSession);
	result = m_bLoadSession;
	pthread_mutex_unlock(&m_mutex4LoadSession);
	return result;
}

void AccessService::setLoadSessionFlag(bool bFlag_)
{
	pthread_mutex_lock(&m_mutex4LoadSession);
	m_bLoadSession = bFlag_;
	pthread_mutex_unlock(&m_mutex4LoadSession);
}

int AccessService::sendDatatoEndpoint(const char * pData, size_t nDataLen, const char * pEndpoint)
{
	int result = E_DEFAULTERROR;
	if (pData && nDataLen && pEndpoint) {
		size_t nHeadSize = sizeof(access_service::AppMessageHead);
		access_service::AppMessageHead head;
		MAKE_APPMSGHEAD(head);
		std::string strUtf8Msg = AnsiToUtf8(pData);
		size_t nUtf8MsgLen = strUtf8Msg.size();
		//old version
		//head.uiDataLen = nDataLen;
		//unsigned long ulBufLen = nHeadSize + nDataLen;
		//unsigned char * pMsgBuf = (unsigned char *)zmalloc(ulBufLen + 1);
		//memcpy_s(pMsgBuf, ulBufLen, &head, nHeadSize);
		//unsigned long ulOffset = nHeadSize;
		//memcpy_s(pMsgBuf + ulOffset, nDataLen + 1, pData, nDataLen);
		//encryptMessage(pMsgBuf, ulOffset, ulOffset + nDataLen);
		head.uiDataLen = nUtf8MsgLen;
		unsigned long ulBufLen = nHeadSize + nUtf8MsgLen;
		unsigned char * pMsgBuf = (unsigned char *)zmalloc(ulBufLen + 1);
		memcpy_s(pMsgBuf, ulBufLen, &head, nHeadSize);
		memcpy_s(pMsgBuf + nHeadSize, nUtf8MsgLen + 1, strUtf8Msg.c_str(), nUtf8MsgLen);
		unsigned long ulOffset = nHeadSize;
		encryptMessage(pMsgBuf, ulOffset, ulOffset + nUtf8MsgLen);
		result = TS_SendData(m_uiSrvInst, pEndpoint, (const char *)pMsgBuf, ulBufLen);
		free(pMsgBuf);
		pMsgBuf = NULL;
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
				zframe_t * frame_from = zmsg_pop(subMsg);
				char * strMark = zframe_strdup(frame_mark);
				char * strSeq = zframe_strdup(frame_seq);
				char * strType = zframe_strdup(frame_type);
				char * strUuid = zframe_strdup(frame_uuid);
				char * strBody = zframe_strdup(frame_body);
				char * strFrom = zframe_strdup(frame_from);
				TopicMessage * pMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
				if (pMsg) {
					strncpy_s(pMsg->szMsgMark, sizeof(pMsg->szMsgMark), strMark, strlen(strMark));
					pMsg->usMsgSequence = (unsigned short)atoi(strSeq);
					pMsg->usMsgType = (unsigned short)atoi(strType);
					strncpy_s(pMsg->szMsgUuid, sizeof(pMsg->szMsgUuid), strUuid, strlen(strUuid));
					strncpy_s(pMsg->szMsgBody, sizeof(pMsg->szMsgBody), strBody, strlen(strBody));
					strncpy_s(pMsg->szMsgFrom, sizeof(pMsg->szMsgFrom), strFrom, strlen(strFrom));
					if (!addTopicMsg(pMsg)) {
						free(pMsg);
					}
				}
				zframe_destroy(&frame_mark);
				zframe_destroy(&frame_seq);
				zframe_destroy(&frame_type);
				zframe_destroy(&frame_uuid);
				zframe_destroy(&frame_body);
				zframe_destroy(&frame_from);
				zmsg_destroy(&subMsg);
				free(strMark);
				free(strSeq);
				free(strType);
				free(strUuid);
				free(strBody);
				free(strFrom);
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

void AccessService::handleLinkDisconnect(const char * pLink_, const char * pUser_, bool bFlag_)
{
	char szLog[512] = { 0 };
	if (pLink_) {
		std::string strLink = pLink_;
		char szGuarder[20] = { 0 };
		char szSession[20] = { 0 };
		bool bSubscriber = false;
		char szOrg[40] = { 0 };
		char szDeviceId[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(pLink_);
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
		if (strlen(szGuarder) == 0) {
			if (pUser_ != NULL) {
				strncpy_s(szGuarder, sizeof(szGuarder), pUser_, strlen(pUser_));
			}
		}
		if (strlen(szGuarder) > 0) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strcmp(pGuarder->szLink, pLink_) == 0) {
					if (strlen(pGuarder->szCurrentSession) > 0) {
						strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession, strlen(pGuarder->szCurrentSession));
						if (pGuarder->usState == STATE_GUARDER_DUTY || pGuarder->usState == STATE_GUARDER_BIND) {
							bSubscriber = true;
							strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
							strncpy_s(szDeviceId, sizeof(szDeviceId), pGuarder->szBindDevice, strlen(pGuarder->szBindDevice));
						}
					}
					pGuarder->szLink[0] = '\0';
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (strlen(szSession) > 0) {
			if (bFlag_) {
				pthread_mutex_lock(&m_mutex4LinkList);
				access_service::AppLinkInfo * pAppLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
				if (pAppLink) {
					pAppLink->szEndpoint[0] = '\0';
					pAppLink->ulActivateTime = 0;
					pAppLink->nActivated = 0;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
			}
			if (bSubscriber) {
				char szTopic[64] = { 0 };
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					if (strlen(pDevice->szOrganization)) {
						strncpy_s(szOrg, sizeof(szOrg), pDevice->szOrganization, strlen(pDevice->szOrganization));
					}
					snprintf(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, pDevice->deviceBasic.szFactoryId, szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				if (strlen(szTopic)) {
					pthread_mutex_lock(&m_mutex4SubscribeList);
					access_service::AppSubscirbeInfo * pSubInfo = (access_service::AppSubscirbeInfo *)zhash_lookup(
						m_subscribeList, szTopic);
					if (pSubInfo) {
						if (strcmp(pSubInfo->szEndpoint, pLink_) == 0) {
							pSubInfo->szEndpoint[0] = '\0';
						}
					}
					pthread_mutex_unlock(&m_mutex4SubscribeList);
				}
			}
		}
		//if (strlen(szGuarder) > 0 && strlen(szSession) > 0) {
		//	char szDatetime[20] = { 0 };
		//	format_datetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
		//	char szMsg[512] = { 0 };
		//	snprintf(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
		//		"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\"}]}",
		//		MSG_SUB_REPORT, getNextRequestSequence(), szDatetime, SUB_REPORT_GUARDER_OFFLINE, szGuarder,
		//		szSession);
		//	sendDataViaInteractor(szMsg, strlen(szMsg));
		//}
		snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]link=%s disconnect, guarder=%s\r\n", 
			__FUNCTION__, __LINE__, pLink_, szGuarder);
		writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::readDataBuffer()
{
	//pthread_mutex_lock(&g_mutex4DevList);
	//WristletDevice * pDev = (WristletDevice *)malloc(sizeof(WristletDevice));
	//if (pDev) {
	//	pDev->deviceBasic.nBattery = 100;
	//	pDev->deviceBasic.nStatus = 0;
	//	snprintf(pDev->deviceBasic.szDeviceId, sizeof(pDev->deviceBasic.szDeviceId), "devId01");
	//	snprintf(pDev->deviceBasic.szFactoryId, sizeof(pDev->deviceBasic.szFactoryId), "01");
	//	pDev->deviceBasic.ulLastActiveTime = (unsigned long)time(NULL);
	//	snprintf(pDev->szOrganization, sizeof(pDev->szOrganization), "org01");
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
	//	snprintf(pGuarder->szId, sizeof(pGuarder->szId), "test");
	//	snprintf(pGuarder->szPassword, sizeof(pGuarder->szPassword), "test123");
	//	snprintf(pGuarder->szOrg, sizeof(pGuarder->szOrg), "org01");
	//	snprintf(pGuarder->szTagName, sizeof(pGuarder->szTagName), "test");
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
				&& guarderRepContainer.ulSqlOptTime == guarderContainer.ulSqlOptTime
				&& guarderRepContainer.uiResultCount > 0) {
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
			if (guarderRepContainer.pStoreResult) {
				free(guarderRepContainer.pStoreResult);
				guarderRepContainer.pStoreResult = NULL;
				guarderRepContainer.uiResultCount = 0;
				guarderRepContainer.uiResultLen = 0;
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
				&& deviceContainerRep.usSqlOptType == deviceContainer.usSqlOptType
				&& deviceContainerRep.uiResultCount > 0) {
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
			if (deviceContainerRep.pStoreResult) {
				free(deviceContainerRep.pStoreResult);
				deviceContainerRep.pStoreResult = NULL;
				deviceContainerRep.uiResultCount = 0;
				deviceContainerRep.uiResultLen = 0;
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
				&& taskRepContainer.usSqlOptType == taskContainer.usSqlOptType
				&& taskRepContainer.uiResultCount > 0) {
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
			if (taskRepContainer.pStoreResult) {
				free(taskRepContainer.pStoreResult);
				taskRepContainer.pStoreResult = NULL;
				taskRepContainer.uiResultLen = 0;
				taskRepContainer.uiResultCount = 0;
			}
		}
		zframe_destroy(&frame_task_rep);
		zmsg_destroy(&msg_task_rep);
	} while (0);
}

void AccessService::verifyLinkList()
{
	char szLog[256] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_first(m_linkList);
	while (pLink) {
		time_t now = time(NULL);
		if (strlen(pLink->szEndpoint)) {
			snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]verify link=%s, guarder=%s, session=%s at time=%ld\r\n", 
				__FUNCTION__, __LINE__, pLink->szEndpoint, pLink->szGuarder, pLink->szSession, (long)now);
			writeLog(szLog, pf_logger::eLOGCATEGORY_DEFAULT, m_usLogType);
			if (pLink->nActivated) {
				if (difftime(now, (time_t)pLink->ulActivateTime) >= 120) { //2min
					pLink->nActivated = 0;
				}
			}
			if (pLink->nActivated == 0) {
				if (difftime(now, (time_t)pLink->ulActivateTime) <= 300) {
					if (!verifyLink(pLink->szEndpoint)) { 
						snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]verify link=%s disconnect, guarder=%s, session=%s"
							" at time=%ld\r\n", __FUNCTION__, __LINE__, pLink->szEndpoint, pLink->szGuarder, pLink->szSession,
							(unsigned long)now);
						writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						TS_CloseEndpoint(m_uiSrvInst, pLink->szEndpoint);
						handleLinkDisconnect(pLink->szEndpoint, pLink->szGuarder, false);
						pLink->szEndpoint[0] = '\0';
						pLink->nActivated = 0;
						pLink->ulActivateTime = 0;
					}
				}
				else {
					snprintf(szLog, sizeof(szLog), "[access_service]%s[%d]link=%s, lost connection for more than 5 minute, "
						"guarder=%s, session=%s at time=%ld\r\n", __FUNCTION__, __LINE__, pLink->szEndpoint, pLink->szGuarder,
						pLink->szSession, (unsigned long)now);
					writeLog(szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					TS_CloseEndpoint(m_uiSrvInst, pLink->szEndpoint);
					handleLinkDisconnect(pLink->szEndpoint, pLink->szGuarder, false);
					pLink->szEndpoint[0] = '\0';
					pLink->nActivated = 0;
					pLink->ulActivateTime = 0;
				}
			}
		}
		pLink = (access_service::AppLinkInfo *)zhash_next(m_linkList);
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
}

bool AccessService::verifyLink(const char * pEndpoint_)
{
	bool result = false;
	do {
		if (TS_SendData(m_uiSrvInst, pEndpoint_, NULL, 0) != -1) {
			result = true;
		}
	} while (0);
	return result;
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
				if (pService->m_remoteMsgSrvLink.nActive == 0) {
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
				pService->verifyLinkList();
			}
			if (pService->m_nTimerTickCount % 18 == 0) { //3min
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

