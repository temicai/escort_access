#include "access_concrete.h"

unsigned int AccessService::g_uiRequestSequence = 0;
int AccessService::g_nRefCount = 0;
pthread_mutex_t AccessService::g_mutex4RequestSequence;
pthread_mutex_t AccessService::g_mutex4DevList;
pthread_mutex_t AccessService::g_mutex4GuarderList;
pthread_mutex_t AccessService::g_mutex4TaskList;
pthread_mutex_t AccessService::g_mutex4TaskId;
pthread_mutex_t AccessService::g_mutex4OrgList;
pthread_mutex_t AccessService::g_mutex4LoadOrg;
pthread_mutex_t AccessService::g_mutex4LoadDevice;
pthread_mutex_t AccessService::g_mutex4LoadUser;
pthread_mutex_t AccessService::g_mutex4LoadTask;

zhash_t * AccessService::g_deviceList = NULL;
zhash_t * AccessService::g_guarderList = NULL;
zhash_t * AccessService::g_taskList = NULL;
std::map<std::string, OrganizationEx *> AccessService::g_orgList2;
BOOL AccessService::g_bInitBuffer = FALSE;
bool AccessService::g_bLoadDevice = false;
bool AccessService::g_bLoadOrg = false;
bool AccessService::g_bLoadTask = false;
bool AccessService::g_bLoadUser = false;

static char * make_zkpath(int num_, ...)
{
	const char * tmp_string;
	va_list arguments;
	va_start(arguments, num_);
	size_t nTotalLen = 0;
	for (int i = 0; i < num_; i++) {
		tmp_string = va_arg(arguments, const char *);
		if (tmp_string) {
			nTotalLen += strlen(tmp_string);
		}
	}
	va_end(arguments);
	char * path = (char *)malloc(nTotalLen + 1);
	if (path) {
		memset(path, 0, nTotalLen + 1);
		va_start(arguments, num_);
		for (int i = 0; i < num_; i++) {
			tmp_string = va_arg(arguments, const char *);
			if (tmp_string) {
				strcat_s(path, nTotalLen + 1, tmp_string);
			}
		}
	}
	return path;
}

//std::string Utf8ToAnsi(LPCSTR utf8_)
//{
//	int WLength = MultiByteToWideChar(CP_UTF8, 0, utf8_, -1, NULL, NULL);
//	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
//	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
//	MultiByteToWideChar(CP_UTF8, 0, utf8_, -1, pszW, WLength);
//	pszW[WLength] = '\0';
//	int ALength = WideCharToMultiByte(CP_ACP, 0, pszW, -1, NULL, 0, NULL, NULL);
//	LPSTR pszA = (LPSTR)_alloca((ALength + 1) * sizeof(char));
//	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
//	WideCharToMultiByte(CP_ACP, 0, pszW, -1, pszA, ALength, NULL, NULL);
//	pszA[ALength] = '\0';
//	std::string retStr = pszA;
//	return retStr;
//}
//
//std::string AnsiToUtf8(LPCSTR Ansi_)
//{
//	int WLength = MultiByteToWideChar(CP_ACP, 0, Ansi_, -1, NULL, 0);
//	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
//	//LPWSTR pszW = (LPWSTR)_malloca((WLength + 1) * sizeof(WCHAR));
//	MultiByteToWideChar(CP_ACP, 0, Ansi_, -1, pszW, WLength);
//	int ALength = WideCharToMultiByte(CP_UTF8, 0, pszW, -1, NULL, 0, NULL, NULL);
//	LPSTR pszA = (LPSTR)_alloca(ALength + 1);
//	//LPSTR pszA = (LPSTR)_malloca((ALength + 1) * sizeof(char));
//	WideCharToMultiByte(CP_UTF8, 0, pszW, -1, pszA, ALength, NULL, NULL);
//	pszA[ALength] = '\0';
//	std::string retStr(pszA);
//	return retStr;
//}

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
				pService->loadSessionList();
			}
			break;
		}
		case ZNODEEXISTS: {
			if (pService && pService->m_nRun) {
				pService->masterExist();
				pService->runAsSlaver();
				//pService->setLoadSessionFlag(true);
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
	if (type == ZOO_SESSION_EVENT) {
		AccessService * pInst = (AccessService *)watcherCtx;
		if (state == ZOO_CONNECTED_STATE) {

		}
		else if (state == ZOO_EXPIRED_SESSION_STATE) {

		}
	}
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
					sprintf_s(szSessionFullPath, sizeof(szSessionFullPath), "/escort/session/%s", pSessionNode->szUserId);
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
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
					pService->m_linkList, pNode->szSession);
				if (pLink) {
					pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
					memcpy_s(pLink, nLinkInfoSize, pLinkInfo, nLinkInfoSize);
				}
			}
			pthread_mutex_unlock(&pService->m_mutex4LinkList);
			if (pLinkInfo) {
				if (pService->m_zkHandle) {
					memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
					char szFullPath[128] = { 0 };
					sprintf_s(szFullPath, sizeof(szFullPath), "/escort/session/%s", pNode->szUserId);
					const char * pBuf = szBuf;
					zoo_acreate(pService->m_zkHandle, szFullPath, pBuf, 1024, &ZOO_OPEN_ACL_UNSAFE, 0,
						zk_session_child_create_completion, data);
				}
				free(pLinkInfo);
				pLinkInfo = NULL;
			}
		}
	}
	else if (rc == ZNODEEXISTS) {
		AccessService * pService = (AccessService *)pNode->data;
		if (pService && pService->m_nRun) {
			char szBuf[1024] = { 0 };
			access_service::AppLinkInfo * pLinkInfo = NULL;
			size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
			pthread_mutex_lock(&pService->m_mutex4LinkList);
			if (zhash_size(pService->m_linkList)) {
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
					pService->m_linkList, pNode->szSession);
				if (pLink) {
					pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
					memcpy_s(pLinkInfo, nLinkInfoSize, pLink, nLinkInfoSize);
				}
			}
			pthread_mutex_unlock(&pService->m_mutex4LinkList);
			if (pLinkInfo) {
				if (pService->m_zkHandle) {
					memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
					char szFullSessionPath[128] = { 0 };
					sprintf_s(szFullSessionPath, sizeof(szFullSessionPath), "/escort/session/%s", pNode->szUserId);
					const char * pBuf = szBuf;
					zoo_aset(pService->m_zkHandle, szFullSessionPath, pBuf, 1024, -1, zk_session_child_set_completion, 
						data);
				}
				free(pLinkInfo);
				pLinkInfo = NULL;
			}
		}
	}
	else if (rc == ZSESSIONEXPIRED) {
		AccessService * pService = (AccessService *)pNode->data;
		if (pService && pService->m_nRun) {
			//pService->m_zkHandle
			zookeeper_close(pService->m_zkHandle);
			pService->m_zkHandle = NULL;
			pService->m_zkHandle = zookeeper_init(pService->m_szHost, zk_server_watcher, 30000, NULL, pNode->data, 0);
			char szBuf[1024] = { 0 };
			access_service::AppLinkInfo * pLinkInfo = NULL;
			size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
			pthread_mutex_lock(&pService->m_mutex4LinkList);
			if (zhash_size(pService->m_linkList)) {
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
					pService->m_linkList, pNode->szSession);
				if (pLink) {
					pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
					memcpy_s(pLink, nLinkInfoSize, pLinkInfo, nLinkInfoSize);
				}
			}
			pthread_mutex_unlock(&pService->m_mutex4LinkList);
			if (pLinkInfo) {
				if (pService->m_zkHandle) {
					memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
					char szFullPath[128] = { 0 };
					sprintf_s(szFullPath, sizeof(szFullPath), "/escort/session/%s", pNode->szUserId);
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
					sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", pNode->szUserId);
					zoo_aset(pService->m_zkHandle, szPath, pBuf, 1024, -1, zk_session_child_set_completion, data);
					delete pLinkInfo;
					pLinkInfo = NULL;
				}
			}
		}
	}
}

AccessService::AccessService(const char * pZkHost_, const char * pRoot_)
{
	srand((unsigned int)time(NULL));
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
		pthread_mutex_init(&g_mutex4OrgList, NULL);
		pthread_mutex_init(&g_mutex4LoadOrg, NULL);
		pthread_mutex_init(&g_mutex4LoadTask, NULL);
		pthread_mutex_init(&g_mutex4LoadUser, NULL);
		pthread_mutex_init(&g_mutex4LoadDevice, NULL);
		g_deviceList = zhash_new();
		g_guarderList = zhash_new();
		g_taskList = zhash_new(); 
		g_bInitBuffer = FALSE;
		g_nRefCount = 1;
		g_bLoadDevice = false;
		g_bLoadOrg = false;
		g_bLoadTask = false;
		g_bLoadUser = false;
	}
	else {
		g_nRefCount++;
	}
	m_nRun = 0;
	m_pthdAppMsg.p = NULL;
	m_pthdSupervisor.p = NULL;
	m_pthdTopicMsg.p = NULL;
	m_pthdInteractionMsg.p = NULL;

	m_szInteractorIdentity[0] = '\0';
	m_szSeekerIdentity[0] = '\0';
	memset(m_zkNodePath, 0, sizeof(m_zkNodePath));
	m_bLoadSession = false;

	m_linkList = zhash_new();
	m_subscribeList = zhash_new();
	m_localTopicMsgList = zlist_new();

	m_szHost[0] = '\0';
	m_szLogRoot[0] = '\0';

	m_ullSrvInst = 0;

	m_ullLogInst = 0;

	m_usLogType = pf_logger::eLOGTYPE_FILE;

	m_nTaskCloseCheckStatus = 0;
	m_nTaskFleeReplicatedReport = 0;
	m_usSrvPort = 0;

	m_bConnectZk = false;
	if (pZkHost_ && strlen(pZkHost_)) {
		strcpy_s(m_szHost, sizeof(m_szHost), pZkHost_);
	}
	initZookeeper();

	if (pRoot_ && strlen(pRoot_)) {
		strcpy_s(m_szLogRoot, sizeof(m_szLogRoot), pRoot_);
	}
	initLog();
}

AccessService::~AccessService()
{
	if (m_nRun) {
		StopAccessService_v2();
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
	if (m_ullLogInst) {
		LOG_Release(m_ullLogInst);
		m_ullLogInst = 0;
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
		if (!g_orgList2.empty()) {
			std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.begin();
			while (iter != g_orgList2.end()) {
				OrganizationEx * pOrg = iter->second;
				if (pOrg) {
					delete pOrg;
					pOrg = NULL;
				}
				iter = g_orgList2.erase(iter);
			}
		}
		pthread_mutex_destroy(&g_mutex4RequestSequence);
		pthread_mutex_destroy(&g_mutex4DevList);
		pthread_mutex_destroy(&g_mutex4GuarderList);
		pthread_mutex_destroy(&g_mutex4TaskList);
		pthread_mutex_destroy(&g_mutex4TaskId);
		pthread_mutex_destroy(&g_mutex4OrgList);
		pthread_mutex_destroy(&g_mutex4LoadDevice);
		pthread_mutex_destroy(&g_mutex4LoadOrg);
		pthread_mutex_destroy(&g_mutex4LoadUser);
		pthread_mutex_destroy(&g_mutex4LoadTask);
		g_nRefCount = 0;
	}
	pthread_mutex_destroy(&m_mutex4LinkDataList);
	pthread_mutex_destroy(&m_mutex4AppMsgQueue);
	pthread_mutex_destroy(&m_mutex4LinkList);
	pthread_mutex_destroy(&m_mutex4SubscribeList);
	pthread_mutex_destroy(&m_mutex4InteractionMsgQueue);
	pthread_mutex_destroy(&m_mutex4TopicMsgQueue);
	pthread_cond_destroy(&m_cond4AppMsgQueue);
	pthread_cond_destroy(&m_cond4InteractionMsgQueue);
	pthread_cond_destroy(&m_cond4TopicMsgQueue);
	pthread_mutex_destroy(&m_mutex4LocalTopicMsgList);
	pthread_mutex_destroy(&m_mutex4RemoteLink);
	pthread_mutex_destroy(&m_mutex4LoadSession);
	if (m_zkHandle) {
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		zookeeper_close(m_zkHandle);
		m_zkHandle = NULL;
	}
	zsys_shutdown();
}

int AccessService::StartAccessService_v2(const char * pAccHost_, unsigned short usAccPort_,
	const char * pMsgHost_, unsigned short usMsgPort_, unsigned short usInteractPort_)
{
	if (!m_nRun) {
		std::random_device rd;
		std::default_random_engine re(rd());
		std::uniform_int_distribution<int> uid(0, 10000);

		m_accessSock = zsock_new(ZMQ_ROUTER);
		zsock_set_tcp_keepalive(m_accessSock, 1);
		zsock_set_tcp_keepalive_idle(m_accessSock, 30);//30second
		zsock_set_tcp_keepalive_cnt(m_accessSock, 5); //
		zsock_set_tcp_keepalive_intvl(m_accessSock, 5); //5second
		zsock_set_probe_router(m_accessSock, 1);
		zsock_set_router_handover(m_accessSock, 1);
		zsock_bind(m_accessSock, "tcp://*:%hu", usAccPort_);
		
		m_subscriberSock = zsock_new(ZMQ_SUB);
		zsock_set_subscribe(m_subscriberSock, "");
		zsock_connect(m_subscriberSock, "tcp://%s:%hu", pMsgHost_, usMsgPort_);
		m_interactorSock = zsock_new(ZMQ_DEALER);
		sprintf_s(m_szInteractorIdentity, sizeof(m_szInteractorIdentity),"acc_%x_%04x_%04x", (unsigned int)time(NULL),
			uid(re), uid(re));
		zsock_set_identity(m_interactorSock, m_szInteractorIdentity);
		zsock_connect(m_interactorSock, "tcp://%s:%hu", pMsgHost_, usInteractPort_);

		m_loop = zloop_new();
		zloop_timer(m_loop, 2000, 0, timerCb, this);
		zloop_reader(m_loop, m_accessSock, readAccInteract, this);
		zloop_reader_set_tolerant(m_loop, m_accessSock);
		zloop_reader(m_loop, m_subscriberSock, readMsgSubscriber, this);
		zloop_reader_set_tolerant(m_loop, m_subscriberSock);
		zloop_reader(m_loop, m_interactorSock, readMsgInteractor, this);
		zloop_reader_set_tolerant(m_loop, m_interactorSock);

		memset(&m_remoteMsgSrvLink, 0, sizeof(m_remoteMsgSrvLink));
		m_nRun = 1;
		pthread_create(&m_pthdInteractionMsg, NULL, dealInteractionMsgThread, this);
		pthread_create(&m_pthdTopicMsg, NULL, dealTopicMsgThread, this);
		pthread_create(&m_pthdSupervisor, NULL, superviseThread, this);

		getOrgList();
		getUserList();
		getDeviceList();
		getTaskList();

		memset(&m_zkMidware, 0, sizeof(m_zkMidware));
		strcpy_s(m_zkMidware.szHostIp, sizeof(m_zkMidware.szHostIp), pMsgHost_);
		m_zkMidware.usTalkPort = usInteractPort_;
		m_zkMidware.usPublisherPort = usMsgPort_;
		
		m_zkAccess.usAccessPort = usAccPort_;
		strcpy_s(m_zkAccess.szHostIp, sizeof(m_zkAccess.szHostIp), pAccHost_);
		
		competeForMaster();
		
		m_thdLoadSession = std::thread(loadSessionThread, this);
		m_thdDissconnEvent = std::thread(dealDisconnectEventThread, this);
		m_thdAccAppMsg = std::thread(dealAppAccMsgThread, this);
		m_thdHandleExpressMsg = std::thread(dealExpressMsgThread, this);

		char szLog[256] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]access service start at port=%hu,"
			" connect to msg server at tcp://%s:%hu and tcp://%s:%hu\n", __FUNCTION__, __LINE__,
			usAccPort_, pMsgHost_, usMsgPort_, pMsgHost_, usInteractPort_);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
	return 0;
}

int AccessService::StopAccessService_v2()
{
	if (m_nRun) {
		char szLog[256] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]stop access service\n", __FUNCTION__, __LINE__);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		m_nRun = 0;

		pthread_cond_broadcast(&m_cond4InteractionMsgQueue);
		pthread_join(m_pthdInteractionMsg, NULL);
		
		pthread_cond_broadcast(&m_cond4TopicMsgQueue);
		pthread_join(m_pthdTopicMsg, NULL);

		pthread_join(m_pthdSupervisor, NULL);

		m_thdLoadSession.join();
		m_cond4DissconnEventQue.notify_all();
		m_thdDissconnEvent.join();
		m_cond4AccAppMsgQue.notify_all();
		m_thdAccAppMsg.join();

		m_cond4CltExpressMsgQue.notify_all();
		m_thdHandleExpressMsg.join();

		if (m_loop) {
			zloop_destroy(&m_loop);
		}
		if (m_accessSock) {
			zsock_destroy(&m_accessSock);
		}
		if (m_subscriberSock) {
			zsock_destroy(&m_subscriberSock);
		}
		if (m_interactorSock) {
			zsock_destroy(&m_interactorSock);
		}
		m_szInteractorIdentity[0] = '\0';

		if (m_bConnectZk) {
			zoo_delete(m_zkHandle, m_zkNodePath, -1);
			m_zkNodePath[0] = '\0';
			m_bConnectZk = false;
		}

	}
	return 0;
}

void AccessService::SetLogType(unsigned short usLogType)
{
	if (m_ullLogInst) {
		if (m_usLogType != usLogType) {
			pf_logger::LogConfig logConf;
			LOG_GetConfig(m_ullLogInst, &logConf);
			if (logConf.usLogType != usLogType) {
				logConf.usLogType = usLogType;
				LOG_SetConfig(m_ullLogInst, logConf);
				m_usLogType = usLogType;
			}
		}
	}
}

void AccessService::SetParameter(int nParamType, int nParamValue)
{
	switch (nParamType) {
		case access_service::E_PARAM_LOGTYPE: {
			SetLogType((unsigned short)nParamValue);
			break;
		}
		case access_service::E_PARAM_TASK_CLOSE_CHECK_STATUS: {
			m_nTaskCloseCheckStatus = nParamValue;
			break;
		}
		case access_service::E_PARAM_TASK_FLEE_REPORT_REPLICATED: {
			m_nTaskFleeReplicatedReport = nParamValue;
			break;
		}
		case access_service::E_PARAM_ENABLE_TASK_LOOSE_CHECK: {
			m_nEnableTaskLooseCheck = nParamValue;
			break;
		}
	}
}

int AccessService::GetStatus()
{
	return m_nRun;
}

void AccessService::initLog()
{
	if (m_ullLogInst == 0) {
		m_ullLogInst = LOG_Init();
		if (m_ullLogInst) {
			pf_logger::LogConfig logInfo;
			logInfo.usLogType = m_usLogType;
			char szLogDir[256] = { 0 };
			sprintf_s(szLogDir, 256, "%slog\\", m_szLogRoot);
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strcat_s(szLogDir, 256, "escort_accesss\\");
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strncpy_s(logInfo.szLogPath, sizeof(logInfo.szLogPath), szLogDir, strlen(szLogDir));
			LOG_SetConfig(m_ullLogInst, logInfo);
		}
	}
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
			if (m_appMsgQueue.size() >= 5) {
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
			if (pMsg->pMsgData && pMsg->uiMsgDataLen) {
				parseAppMsg(pMsg);
				delete [] pMsg->pMsgData;
				pMsg->pMsgData = NULL;
				pMsg->uiMsgDataLen = 0;
			}
			delete pMsg;
			pMsg = NULL;
		}
	} while (1);
	char szLog[256] = { 0 };
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]quit\n", __FUNCTION__, __LINE__);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::parseAppMsg(MessageContent * pMsg)
{
	char szLog[512] = { 0 };
	if (pMsg) {
		std::string strEndPoint = pMsg->szEndPoint;
		//check weather exists data linger
		unsigned char * pBuf = NULL;
		unsigned int uiBufLen = 0;
		pthread_mutex_lock(&m_mutex4LinkDataList);
		LinkDataList::iterator iter = m_linkDataList.find(strEndPoint);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				if (pLinkData->uiLackDataLen == 0) {
					pBuf = new unsigned char [pMsg->uiMsgDataLen + 1];
					uiBufLen = pMsg->uiMsgDataLen;
					memcpy_s(pBuf, uiBufLen + 1, pMsg->pMsgData, uiBufLen);
					pBuf[uiBufLen] = '\0';
				}
				else {
					if (pLinkData->uiLackDataLen <= pMsg->uiMsgDataLen) { //full 
						uiBufLen = pLinkData->uiLingeDataLen + pMsg->uiMsgDataLen;
						pBuf = new unsigned char [uiBufLen + 1];
						memcpy_s(pBuf, uiBufLen, pLinkData->pLingeData, pLinkData->uiLingeDataLen);
						memcpy_s(pBuf + pLinkData->uiLingeDataLen, uiBufLen - pLinkData->uiLingeDataLen, pMsg->pMsgData,
							pMsg->uiMsgDataLen);
						pBuf[uiBufLen] = '\0';
						pLinkData->uiLackDataLen = 0;
						pLinkData->uiLingeDataLen = 0;
						delete [] pLinkData->pLingeData;
						pLinkData->pLingeData = NULL;
					}
					else if (pLinkData->uiLackDataLen > pMsg->uiMsgDataLen) { //still lack 
						memcpy_s(pLinkData->pLingeData + pLinkData->uiLingeDataLen, pLinkData->uiLackDataLen, pMsg->pMsgData,
							pMsg->uiMsgDataLen);
						pLinkData->uiLingeDataLen += pMsg->uiMsgDataLen;
						pLinkData->uiLackDataLen -= pMsg->uiMsgDataLen;
					}
				}
			}
		}
		else {
			printf("...oppppppps, are U kiding me? link[%s] not found\n", strEndPoint.c_str());
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]link(%s) not found\r\n", 
				__FUNCTION__, __LINE__, strEndPoint.c_str());
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		//sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]start parse\r\n",  __FUNCTION__, __LINE__);
		//LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (pBuf && uiBufLen) {
			unsigned int uiIndex = 0;
			unsigned int uiBeginIndex = 0;
			unsigned int uiEndIndex = 0;
			unsigned int uiUnitLen = 0;
			do {
				int n = getWholeMessage(pBuf, uiBufLen, uiIndex, uiBeginIndex, uiEndIndex, uiUnitLen);
				if (n == 0) {
					break;
				}
				else if (n == 1) {
					pthread_mutex_lock(&m_mutex4LinkDataList);
					iter = m_linkDataList.find(strEndPoint);
					if (iter != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData = iter->second;
						if (pLinkData) {
							pLinkData->uiTotalDataLen = uiUnitLen;
							pLinkData->uiLingeDataLen = uiBufLen - uiBeginIndex;
							pLinkData->uiLackDataLen = pLinkData->uiTotalDataLen - pLinkData->uiLingeDataLen;
							pLinkData->pLingeData = new unsigned char [pLinkData->uiTotalDataLen];
							memcpy_s(pLinkData->pLingeData, pLinkData->uiTotalDataLen, pBuf + uiBeginIndex, 
								pLinkData->uiLingeDataLen);
						}
					}
					pthread_mutex_unlock(&m_mutex4LinkDataList);
					break;
				}
				else if (n == 2) {
					uiIndex = uiEndIndex;
					decryptMessage(pBuf, uiBeginIndex, uiEndIndex);
					char * pContent = new char [uiUnitLen + 1];
					memcpy_s(pContent, uiUnitLen + 1, pBuf + uiBeginIndex, uiUnitLen);
					pContent[uiUnitLen] = '\0';
					// sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]pContent=%s\r\n", __FUNCTION__,
					//	__LINE__, pContent);
					//LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_DEFAULT, m_usLogType);
					char * pContent2 = utf8ToAnsi(pContent);
					printf("pContent2: %s\n", pContent2);
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]pContent2=%s\r\n", __FUNCTION__,
						__LINE__, pContent2);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_DEFAULT, m_usLogType);
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
									if (doc["account"].IsString() && doc["account"].GetStringLength()) {
										strcpy_s(loginInfo.szUser, sizeof(loginInfo.szUser), doc["account"].GetString());
										bValidAccount = true;
									}
									if (doc["passwd"].IsString() && doc["passwd"].GetStringLength()) {
										strcpy_s(loginInfo.szPasswd, sizeof(loginInfo.szPasswd), doc["passwd"].GetString());
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(loginInfo.szDateTime, sizeof(loginInfo.szDateTime), 
											doc["datetime"].GetString());
										bValidDatetime = true;
									}
									if (doc.HasMember("handset")) {
										if (doc["handset"].IsString()) {
											size_t nSize = doc["handset"].GetStringLength();
											if (nSize) {
												strcpy_s(loginInfo.szHandset, sizeof(loginInfo.szHandset), doc["handset"].GetString());
											}
										}
									}
									if (bValidAccount && bValidPasswd && bValidDatetime) {
										loginInfo.uiReqSeq = 0;	//loginInfo.uiReqSeq = getNextRequestSequence();
										handleAppLogin(loginInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										char szReply[256] = {};
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
											"\"taskInfo\":[]}", access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
										int nRetVal = sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), 
											pMsg->szEndPoint, access_service::E_ACC_DATA_TRANSFER, NULL);
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, "
											"one or more parameter needed, account=%s, passwd=%s, datetime=%s, handset=%s, "
											"reply code=%d\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, loginInfo.szUser,
											loginInfo.szPasswd, loginInfo.szDateTime,
											loginInfo.szHandset, nRetVal);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskInfo\":[]}", access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										if (nSize) {
											strcpy_s(logoutInfo.szSession, sizeof(logoutInfo.szSession), 
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(logoutInfo.szDateTime, sizeof(logoutInfo.szDateTime), 
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidDatetime) {
										logoutInfo.uiReqSeq = 0;
										handleAppLogout(logoutInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, one or more "
											"parameter needed, session=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint,
											logoutInfo.szSession, logoutInfo.szDateTime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? logoutInfo.szSession : "");
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s,"
										" JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										if (nSize) {
											strcpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strcpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), doc["deviceId"].GetString());
											bValidDevice = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidDevice && bValidDatetime) {
										bindInfo.nMode = 0;
										bindInfo.uiReqSeq = getNextRequestSequence();
										handleAppBind(bindInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, one or more "
											"parameter needed, session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
											pMsg->szEndPoint, bValidSession ? bindInfo.szSesssion : "null",
											bValidDevice ? bindInfo.szDeviceId : "null", bValidDatetime ? bindInfo.szDateTime : "null");
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"battery\":0}",
											access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER, bValidSession ? bindInfo.szSesssion : "");
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, JSON data format"
										" error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"battery\":0}",
										access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										handleAppBind(bindInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, "
											"one or more parameter needed, session=%s, deviceId=%s, datetime=%s\r\n", 
											__FUNCTION__, __LINE__, pMsg->szEndPoint, bindInfo.szSesssion,
											bindInfo.szDeviceId, bindInfo.szDateTime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? bindInfo.szSesssion : "");
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										handleAppSubmitTask(taskInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s,"
											" one or more parameter needed, session=%s, type=%d, limit=%d, target=%s, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, taskInfo.szSession,
											taskInfo.usTaskType, taskInfo.usTaskLimit, taskInfo.szTarget, taskInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"\"}", access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER,
											taskInfo.szSession);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s,"
										" JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskId\":\"\"}", access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										handleAppCloseTask(taskInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s,"
											" one or more parameter needed, session=%s, taskId=%s, closeType=%d, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, taskInfo.szSession,
											taskInfo.szTaskId, taskInfo.nCloseType, taskInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"%s\"}", access_service::E_CMD_TASK_CLOSE_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? taskInfo.szSession : "", bValidTask ? taskInfo.szTaskId : "");
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s,"
										" JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskId\":\"\"}", access_service::E_CMD_TASK_CLOSE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
								}
								break;
							}
							case access_service::E_CMD_POSITION_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("lat")
									&& doc.HasMember("lng") && doc.HasMember("datetime")) {
									access_service::AppPositionInfo posInfo;
									posInfo.nCoordinate = 1; //bd09
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidLat = false;
									bool bValidLng = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(posInfo.szSession, sizeof(posInfo.szSession),doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strcpy_s(posInfo.szTaskId, sizeof(posInfo.szTaskId), doc["taskId"].GetString());
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
									if (doc.HasMember("coordinate")) {
										if (doc["coordinate"].IsInt()) {
											posInfo.nCoordinate = doc["coordinate"].GetInt();
										}
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
										handleAppPosition(posInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, one or more"
											" parameter needed, session=%s, taskId=%s, lat=%f, lng=%f, datetime=%s\r\n", __FUNCTION__,
											__LINE__, pMsg->szEndPoint, bValidSession ? posInfo.szSession : "null",
											bValidTask ? posInfo.szTaskId : "null", bValidLat ? posInfo.dLat : 0.0000,
											bValidLng ? posInfo.dLng : 0.0000, bValidDatetime ? posInfo.szDatetime : "null");
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										handleAppFlee(fleeInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s,"
											" one or more parameter needed, session=%s, taskId=%s, datetime=%s\r\n",
											__FUNCTION__, __LINE__, pMsg->szEndPoint, fleeInfo.szSession,
											fleeInfo.szTaskId, fleeInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"%s\"}", access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER, 
											bValidSession ? fleeInfo.szSession : "", bValidTask ? fleeInfo.szTaskId : "");
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, JSON data "
										"format error=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, pMsg->pMsgData);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskId\":\"\"}", access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										handleAppFlee(fleeInfo, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s,"
											" one or more parameter needed, session=%s, taskId=%s, datetime=%s\r\n", 
											__FUNCTION__, __LINE__, pMsg->szEndPoint, fleeInfo.szSession,
											fleeInfo.szTaskId, fleeInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"%s\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER,
											fleeInfo.szSession,fleeInfo.szTaskId);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s,"
										" JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskId\":\"\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
										if (doc["session"].GetStringLength()) {
											strcpy_s(keepAlive.szSession, sizeof(keepAlive.szSession), 
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["seq"].IsInt()) {
										keepAlive.uiSeq = (unsigned int)doc["seq"].GetInt();
										bValidSeq = true;
									}
									if (doc["datetime"].IsString()) {
										if (doc["datetime"].GetStringLength()) {
											strcpy_s(keepAlive.szDatetime, sizeof(keepAlive.szDatetime), 
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidSeq && bValidDatetime) {
										handleAppKeepAlive(&keepAlive, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, "
											"one or more parameter needed, session=%s, seq=%d, datetime=%s\r\n", 
											__FUNCTION__, __LINE__, pMsg->szEndPoint, keepAlive.szSession, 
											keepAlive.uiSeq, keepAlive.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_MODIFY_PASSWD: {
								if (doc.HasMember("session") && doc.HasMember("currPasswd") 
									&& doc.HasMember("newPasswd") && doc.HasMember("datetime")) {
									access_service::AppModifyPassword modifyPasswd;
									bool bValidSession = false;
									bool bValidCurrPasswd = false;
									bool bValidNewPasswd = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szSession, sizeof(modifyPasswd.szSession),
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["currPasswd"].IsString()) {
										size_t nSize = doc["currPasswd"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szCurrPassword, sizeof(modifyPasswd.szCurrPassword),
												doc["currPasswd"].GetString());
											bValidCurrPasswd = true;
										}
									}
									if (doc["newPasswd"].IsString()) {
										size_t nSize = doc["newPasswd"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szNewPassword, sizeof(modifyPasswd.szNewPassword),
												doc["newPasswd"].GetString());
											bValidNewPasswd = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szDatetime, sizeof(modifyPasswd.szDatetime),
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidCurrPasswd && bValidNewPasswd && bValidDatetime) {
										modifyPasswd.uiSeq = getNextRequestSequence();
										handleAppModifyAccountPassword(modifyPasswd, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, "
											"one or more parameter needed, session=%s, currPasswd=%s, newPasswd=%s, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint, 
											modifyPasswd.szSession, modifyPasswd.szCurrPassword, modifyPasswd.szNewPassword,
											modifyPasswd.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"datetime\":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, 
											E_INVALIDPARAMETER, modifyPasswd.szSession, modifyPasswd.szDatetime);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify password from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\",\"retcode\":%d,"
										"\"datetime\":\"\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY,
										E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint, 
										access_service::E_ACC_DATA_TRANSFER, NULL);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK: {
								if (doc.HasMember("session") && doc.HasMember("taskId") 
									&& doc.HasMember("datetime")) {
									access_service::AppQueryTask queryTask;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szSession, sizeof(queryTask.szSession), 
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szTaskId, sizeof(queryTask.szTaskId), 
												doc["taskId"].GetString());
											bValidTask = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szDatetime, sizeof(queryTask.szDatetime), 
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidDatetime) {
										queryTask.uiQuerySeq = getNextRequestSequence();
										handleAppQueryTask(queryTask, pMsg->szEndPoint, pMsg->ulMsgTime, NULL);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s,"
											" one or more parameter miss, session=%s, task=%s, datetime=%s\r\n",
											__FUNCTION__, __LINE__, pMsg->szEndPoint, queryTask.szSession, 
											queryTask.szTaskId, queryTask.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,"
											"\"session\":\"%s\",\"datetime\":\"%s\",\"taskInfo\":[]}", 
											access_service::E_CMD_QUERY_TASK_REPLY, E_INVALIDPARAMETER, 
											queryTask.szSession, queryTask.szDatetime);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
											access_service::E_ACC_DATA_TRANSFER, NULL);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, "
										"JSON data format error=%d\r\n", __FUNCTION__, __LINE__, pMsg->szEndPoint,
										doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"datetime\":\"\",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, 
										E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
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
									handleAppDeviceCommand(devCmdInfo, pMsg->szEndPoint, NULL);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device command "
										"from %s, JSON data format error=%d\r\n", __FUNCTION__, __LINE__, 
										pMsg->szEndPoint, doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"device\":\"\",\"seq\":0,\"datetime\":\"\"}", 
										access_service::E_CMD_DEVICE_COMMAND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
								}
								break;
							}
							case access_service::E_CMD_QUERY_PERSON: {
								if (doc.HasMember("session") && doc.HasMember("queryPid") && doc.HasMember("seq")
									&& doc.HasMember("datetime") && doc.HasMember("queryMode")) {
									access_service::AppQueryPerson qryPerson;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szSession, sizeof(qryPerson.szSession), 
												doc["session"].GetString());
										}
									}
									if (doc["queryPid"].IsString()) {
										size_t nSize = doc["queryPid"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szQryPersonId, sizeof(qryPerson.szQryPersonId),
												doc["queryPid"].GetString());
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szQryDatetime, sizeof(qryPerson.szQryDatetime), 
												doc["datetime"].GetString());
										}
									}
									if (doc["seq"].IsInt()) {
										qryPerson.uiQeurySeq = (unsigned int)doc["seq"].GetInt();
									}
									if (doc["queryMode"].IsInt()) {
										qryPerson.nQryMode = doc["queryMode"].GetInt();
									}
									handleAppQueryPerson(qryPerson, pMsg->szEndPoint, NULL);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send query person "
										"from %s, JSON data format error=%d\r\n", __FUNCTION__, __LINE__, 
										pMsg->szEndPoint, doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\","
										"\"datetime\":\"\",\"seq\":0,\"personList\":[]}", 
										access_service::E_CMD_QUERY_PERSON_REPLY);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK_LIST: {
								if (doc.HasMember("orgId") && doc.HasMember("seq") && doc.HasMember("datetime")) {
									access_service::AppQueryTaskList qryTaskList;
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(qryTaskList.szOrgId, sizeof(qryTaskList.szOrgId), 
												doc["orgId"].GetString());
										}
									}
									if (doc["seq"].IsInt()) {
										qryTaskList.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(qryTaskList.szDatetime, sizeof(qryTaskList.szDatetime),
												doc["datetime"].GetString());
										}
									}
									if (strlen(qryTaskList.szDatetime)) {
										handleAppQueryTaskList(&qryTaskList, pMsg->szEndPoint, NULL);
									}
								}
								break;
							}
							case access_service::E_CMD_QUERY_DEVICE_STATUS: {
								access_service::AppQueryDeviceStatus qryDevStatus;
								memset(&qryDevStatus, 0, sizeof(qryDevStatus));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(qryDevStatus.szSession, sizeof(qryDevStatus.szSession), 
											doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDeviceId, sizeof(qryDevStatus.szDeviceId), 
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId), 
											doc["factoryId"].GetString());
									}
								}
								else {
									sprintf_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId), "01");
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										qryDevStatus.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDatetime, sizeof(qryDevStatus.szDatetime), 
											doc["datetime"].GetString());
									}
								}
								if (strlen(qryDevStatus.szSession) && strlen(qryDevStatus.szDeviceId)
									&& strlen(qryDevStatus.szDatetime)) {
									handleAppQueryDeviceStatus(&qryDevStatus, pMsg->szEndPoint, NULL);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]recv app query "
										"device status data miss parameter, deviceId=%s, session=%s, seq=%u, "
										"datetime=%s\r\n", __FUNCTION__, __LINE__, qryDevStatus.szDeviceId, 
										qryDevStatus.szSession, qryDevStatus.uiQrySeq, qryDevStatus.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\","
										"\"retcode\":%d,\"deviceId\":\"%s\",\"status\":%d,\"battery\":%d,"
										"\"seq\":%u,\"datetime\":\"%s\"}",
										access_service::E_CMD_QUERY_DEVICE_STATUS, qryDevStatus.szSession, 
										E_INVALIDPARAMETER, qryDevStatus.szDeviceId, 0, 0, qryDevStatus.uiQrySeq,
										qryDevStatus.szDatetime);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
										access_service::E_ACC_DATA_TRANSFER, NULL);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]can't recognise "
									"command: %d\r\n", __FUNCTION__, __LINE__, nCmd);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								char szReply[256] = { 0 };
								sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}",
									access_service::E_CMD_DEFAULT_REPLY, E_INVALIDCOMMAND);
								sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
									access_service::E_ACC_DATA_TRANSFER, NULL);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]can't parse JSON "
							"content: %s\r\n", __FUNCTION__, __LINE__, pContent);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						char szReply[256] = { 0 };
						sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}", 
							access_service::E_CMD_DEFAULT_REPLY, E_INVALIDCOMMAND);
						sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg->szEndPoint,
							access_service::E_ACC_DATA_TRANSFER, NULL);
					}
					delete [] pContent;
					pContent = NULL;
					delete [] pContent2;
					pContent2 = NULL;
				}
			} while (1);
			if (pBuf) {
				delete [] pBuf;
				pBuf = NULL;
			}
			uiBufLen = 0;
			char szLog[256] = { 0 };
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]finish parse\r\n", 
				__FUNCTION__, __LINE__);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

bool AccessService::addAccAppMsg(access_service::AccessAppMessage * pMsg_)
{
	bool result = false;
	do {
		if (pMsg_->pMsgData && strlen(pMsg_->szAccessFrom)) {
			std::lock_guard<std::mutex> lock(m_mutex4AccAppMsgQue);
			m_accAppMsgQue.emplace(pMsg_);
			if (m_accAppMsgQue.size() == 1) {
				m_cond4AccAppMsgQue.notify_one();
			}
			result = true;
		}
	} while (0);
	return result;
}

void AccessService::dealAccAppMsg()
{
	do {
		std::unique_lock<std::mutex> lock(m_mutex4AccAppMsgQue);
		m_cond4AccAppMsgQue.wait_for(lock, std::chrono::milliseconds(500), [&] {
			return (!m_nRun || !m_accAppMsgQue.empty());
		});
		if (!m_nRun && m_accAppMsgQue.empty()) {
			break;
		}
		if (!m_accAppMsgQue.empty()) {
			access_service::AccessAppMessage * pAccAppMsg = m_accAppMsgQue.front();
			m_accAppMsgQue.pop();
			lock.unlock();
			if (pAccAppMsg) {
				if (pAccAppMsg->uiMsgDataLen > 0 && pAccAppMsg->pMsgData && strlen(pAccAppMsg->szMsgFrom)) {
					parseAccAppMsg(pAccAppMsg);
					delete [] pAccAppMsg->pMsgData;
					pAccAppMsg->pMsgData = NULL;
				}
				delete pAccAppMsg;
				pAccAppMsg = NULL;
			}
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	} while (1);
}

void AccessService::parseAccAppMsg(access_service::AccessAppMessage * pMsg_)
{
	char szLog[512] = { 0 };
	if (pMsg_) {
		std::string strEndPoint = pMsg_->szMsgFrom;
		//check weather exists data linger
		unsigned char * pBuf = NULL;
		unsigned int uiBufLen = 0;
		pthread_mutex_lock(&m_mutex4LinkDataList);
		LinkDataList::iterator iter = m_linkDataList.find(strEndPoint);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				if (pLinkData->uiLackDataLen == 0) {
					pBuf = (unsigned char *)malloc(pMsg_->uiMsgDataLen + 1);
					uiBufLen = pMsg_->uiMsgDataLen;
					memcpy_s(pBuf, uiBufLen + 1, pMsg_->pMsgData, uiBufLen);
					pBuf[uiBufLen] = '\0';
				}
				else {
					if (pLinkData->uiLackDataLen <= pMsg_->uiMsgDataLen) { //full 
						uiBufLen = pLinkData->uiLingeDataLen + pMsg_->uiMsgDataLen;
						pBuf = (unsigned char *)malloc(uiBufLen + 1);
						memcpy_s(pBuf, uiBufLen, pLinkData->pLingeData, pLinkData->uiLingeDataLen);
						memcpy_s(pBuf + pLinkData->uiLingeDataLen, uiBufLen - pLinkData->uiLingeDataLen,
							pMsg_->pMsgData, pMsg_->uiMsgDataLen);
						pBuf[uiBufLen] = '\0';
						pLinkData->uiLackDataLen = 0;
						pLinkData->uiLingeDataLen = 0;
						delete [] pLinkData->pLingeData;
						pLinkData->pLingeData = NULL;
					}
					else if (pLinkData->uiLackDataLen > pMsg_->uiMsgDataLen) { //still lack 
						memcpy_s(pLinkData->pLingeData + pLinkData->uiLingeDataLen, 
							pLinkData->uiLackDataLen, pMsg_->pMsgData, pMsg_->uiMsgDataLen);
						pLinkData->uiLingeDataLen += pMsg_->uiMsgDataLen;
						pLinkData->uiLackDataLen -= pMsg_->uiMsgDataLen;
					}
				}
			}
		} 
		else {
			pBuf = (unsigned char *)malloc(pMsg_->uiMsgDataLen + 1);
			uiBufLen = pMsg_->uiMsgDataLen;
			memcpy_s(pBuf, uiBufLen + 1, pMsg_->pMsgData, uiBufLen);
			pBuf[uiBufLen] = '\0';
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (pBuf && uiBufLen) {
			unsigned int uiIndex = 0;
			unsigned int uiBeginIndex = 0;
			unsigned int uiEndIndex = 0;
			unsigned int uiUnitLen = 0;
			do {
				int n = getWholeMessage(pBuf, uiBufLen, uiIndex, uiBeginIndex, uiEndIndex, uiUnitLen);
				if (n == 0) {
					break;
				}
				else if (n == 1) {
					pthread_mutex_lock(&m_mutex4LinkDataList);
					iter = m_linkDataList.find(strEndPoint);
					if (iter != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData = iter->second;
						if (pLinkData) {
							pLinkData->uiTotalDataLen = uiUnitLen;
							pLinkData->uiLingeDataLen = uiBufLen - uiBeginIndex;
							pLinkData->uiLackDataLen = pLinkData->uiTotalDataLen - pLinkData->uiLingeDataLen;
							pLinkData->pLingeData = new unsigned char [pLinkData->uiTotalDataLen];
							memcpy_s(pLinkData->pLingeData, pLinkData->uiTotalDataLen, pBuf + uiBeginIndex,
								pLinkData->uiLingeDataLen);
						}
					}
					pthread_mutex_unlock(&m_mutex4LinkDataList);
					break;
				}
				else if (n == 2) {
					uiIndex = uiEndIndex;
					decryptMessage(pBuf, uiBeginIndex, uiEndIndex);
					char * pContent = new char[uiUnitLen + 1];
					memcpy_s(pContent, uiUnitLen + 1, pBuf + uiBeginIndex, uiUnitLen);
					pContent[uiUnitLen] = '\0';
					char * pContent2 = utf8ToAnsi(pContent);
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]pContent2=%s\r\n", __FUNCTION__, __LINE__, pContent2);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_DEFAULT, m_usLogType);
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
									if (doc["account"].IsString() && doc["account"].GetStringLength()) {
										strcpy_s(loginInfo.szUser, sizeof(loginInfo.szUser), doc["account"].GetString());
									}
									if (doc["passwd"].IsString() && doc["passwd"].GetStringLength()) {
										strcpy_s(loginInfo.szPasswd, sizeof(loginInfo.szPasswd), doc["passwd"].GetString());
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(loginInfo.szDateTime, sizeof(loginInfo.szDateTime), doc["datetime"].GetString());
									}
									if (doc.HasMember("handset")) {
										if (doc["handset"].IsString() && doc["handset"].GetStringLength()) {
											strcpy_s(loginInfo.szHandset, sizeof(loginInfo.szHandset), doc["handset"].GetString());
										}
									}
									if (strlen(loginInfo.szUser) && strlen(loginInfo.szPasswd)) {
										//handleAppLogin(loginInfo, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
										handleAppLoginV2(&loginInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom, pMsg_->ullMsgTime);
									}
									else {
										char szReply[256] = {};
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
											access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, 
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, one or more parameter "
											"needed, account=%s, passwd=%s, datetime=%s, handset=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom,
											loginInfo.szUser, loginInfo.szPasswd, loginInfo.szDateTime, loginInfo.szHandset);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, JSON data format error\r\n", 
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
										access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_LOGOUT: {
								if (doc.HasMember("session") && doc.HasMember("datetime")) {
									access_service::AppLogoutInfo logoutInfo;
									bool bValidSession = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString() && doc["session"].GetStringLength() ) {
										strcpy_s(logoutInfo.szSession, sizeof(logoutInfo.szSession),
											doc["session"].GetString());
										bValidSession = true;
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(logoutInfo.szDateTime, sizeof(logoutInfo.szDateTime),
											doc["datetime"].GetString());
										bValidDatetime = true;
									}
									if (bValidSession && bValidDatetime) {
										logoutInfo.uiReqSeq = 0;
										handleAppLogout(logoutInfo, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, one or more "
											"parameter needed, session=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom,
											logoutInfo.szSession, logoutInfo.szDateTime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER, logoutInfo.szSession);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, 
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, JSON data format error\r\n",
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
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
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), doc["session"].GetString());
										bValidSession = true;
									}
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), doc["deviceId"].GetString());
										bValidDevice = true;
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), doc["datetime"].GetString());
										bValidDatetime = true;
									}
									if (bValidSession && bValidDevice && bValidDatetime) {
										bindInfo.nMode = 0;
										handleAppBindV2(&bindInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom, pMsg_->ullMsgTime);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, one or more parameter needed, "
											"session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, bindInfo.szSesssion,
											bindInfo.szDeviceId, bindInfo.szDateTime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"battery\":0}",
											access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER, bindInfo.szSesssion);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, access_service::E_ACC_DATA_TRANSFER, 
											pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, JSON data format error\r\n", 
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"battery\":0}",
										access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, access_service::E_ACC_DATA_TRANSFER, 
										pMsg_->szAccessFrom);
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
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), doc["session"].GetString());
										bValidSession = true;
									}
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), doc["deviceId"].GetString());
										bValidDevice = true;
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), doc["datetime"].GetString());
										bValidDatetime = true;
									}
									if (bValidSession && bValidDevice && bValidDatetime) {
										bindInfo.nMode = 1;
										handleAppUnbindV2(&bindInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom, pMsg_->ullMsgTime);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, one or more parameter needed, "
											"session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, bindInfo.szSesssion,
											bindInfo.szDeviceId, bindInfo.szDateTime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
											access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER, bindInfo.szSesssion);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, JSON data format error\r\n",
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\"}",
										access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_TASK: {
								if (doc.HasMember("session") && doc.HasMember("type") && doc.HasMember("destination")
									&& doc.HasMember("target") && doc.HasMember("limit") && doc.HasMember("datetime")) {
									access_service::AppSubmitTaskInfo taskInfo;
									memset(&taskInfo, 0, sizeof(access_service::AppSubmitTaskInfo));
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), doc["session"].GetString());
									}
									if (doc["type"].IsInt()) {
										taskInfo.usTaskType = (unsigned short)doc["type"].GetInt();
									}
									if (doc["limit"].IsInt()) {
										taskInfo.usTaskLimit = (unsigned short)doc["limit"].GetInt();
									}
									if (doc["destination"].IsString()) {
										size_t nSize = doc["destination"].GetStringLength();
										if (nSize) {
											size_t nFieldSize = sizeof(taskInfo.szDestination);
											strncpy_s(taskInfo.szDestination, nFieldSize, doc["destination"].GetString(),
												(nSize < nFieldSize) ? nSize : nFieldSize - 1);
										}
									}
									if (doc["target"].IsString()) {
										size_t nSize = doc["target"].GetStringLength();
										if (nSize) {
											size_t nFieldSize = sizeof(taskInfo.szTarget);
											strncpy_s(taskInfo.szTarget, nFieldSize, doc["target"].GetString(),
												(nSize < nFieldSize) ? nSize : nFieldSize - 1);
										}
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), doc["datetime"].GetString());
									}
									if (strlen(taskInfo.szSession) && strlen(taskInfo.szTarget)) {
										handleAppSubmitTaskV2(&taskInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom, pMsg_->ullMsgTime);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, one or more parameter "
											"needed, session=%s, type=%d, limit=%d, target=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom,
											taskInfo.szSession, taskInfo.usTaskType, taskInfo.usTaskLimit, taskInfo.szTarget, taskInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}", 
											access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER, taskInfo.szSession);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, JSON data format error\r\n", 
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\":\"\"}", 
										access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_TASK_CLOSE: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("closeType")
									&& doc.HasMember("datetime")) {
									access_service::AppCloseTaskInfo taskInfo;
									memset(&taskInfo, 0, sizeof(access_service::AppCloseTaskInfo));
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), doc["session"].GetString());
									}
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(taskInfo.szTaskId, sizeof(taskInfo.szTaskId), doc["taskId"].GetString());
									}
									if (doc["closeType"].IsInt()) {
										taskInfo.nCloseType = doc["closeType"].GetInt();
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), doc["datetime"].GetString());
									}
									if (strlen(taskInfo.szSession) && strlen(taskInfo.szTaskId)) {
										handleAppCloseTaskV2(&taskInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom, pMsg_->ullMsgTime);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, parameter needed, session=%s,"
											" taskId=%s, closeType=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, taskInfo.szSession,
											taskInfo.szTaskId, taskInfo.nCloseType, taskInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}", 
											access_service::E_CMD_TASK_CLOSE_REPLY, E_INVALIDPARAMETER, taskInfo.szSession, taskInfo.szTaskId);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, access_service::E_ACC_DATA_TRANSFER, 
											pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, JSON data format error\r\n",
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\":\"\"}", 
										access_service::E_CMD_TASK_CLOSE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, access_service::E_ACC_DATA_TRANSFER,
										pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_POSITION_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("lat") && doc.HasMember("lng") 
									&& doc.HasMember("datetime")) {
									access_service::AppPositionInfo posInfo;
									posInfo.nCoordinate = 1; //bd09
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidLat = false;
									bool bValidLng = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(posInfo.szSession, sizeof(posInfo.szSession), doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strcpy_s(posInfo.szTaskId, sizeof(posInfo.szTaskId), doc["taskId"].GetString());
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
									if (doc.HasMember("coordinate")) {
										if (doc["coordinate"].IsInt()) {
											posInfo.nCoordinate = doc["coordinate"].GetInt();
										}
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
										handleAppPosition(posInfo, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, one or more"
											" parameter needed, session=%s, taskId=%s, lat=%f, lng=%f, datetime=%s\r\n", __FUNCTION__,
											__LINE__, pMsg_->szMsgFrom, posInfo.szSession, posInfo.szTaskId, posInfo.dLat,
											posInfo.dLng, posInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, JSON data "
										"format error\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_FLEE_REPORT: {
								if (doc.HasMember("session") && doc.HasMember("taskId") && doc.HasMember("datetime")) {
									access_service::AppSubmitFleeInfo fleeInfo;
									memset(&fleeInfo, 0, sizeof(access_service::AppSubmitFleeInfo));
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(fleeInfo.szSession, sizeof(fleeInfo.szSession), doc["session"].GetString());
									}
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(fleeInfo.szTaskId, sizeof(fleeInfo.szTaskId), doc["taskId"].GetString());
									}
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(fleeInfo.szDatetime, sizeof(fleeInfo.szDatetime), doc["datetime"].GetString());
									}
									if (strlen(fleeInfo.szTaskId) && strlen(fleeInfo.szSession) && strlen(fleeInfo.szDatetime)) {
										fleeInfo.nMode = 0;
										fleeInfo.uiReqSeq = getNextRequestSequence();
										handleAppFlee(fleeInfo, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, one or more parameter needed, "
											"session=%s, taskId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, fleeInfo.szSession, 
											fleeInfo.szTaskId, fleeInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}", 
											access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER, fleeInfo.szSession, fleeInfo.szTaskId);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, JSON data format error\r\n", 
										__FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskId\":\"\"}", 
										access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom, access_service::E_ACC_DATA_TRANSFER,
										pMsg_->szAccessFrom);
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
										handleAppFlee(fleeInfo, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s,"
											" one or more parameter needed, session=%s, taskId=%s, datetime=%s\r\n",
											__FUNCTION__, __LINE__, pMsg_->szMsgFrom, fleeInfo.szSession, fleeInfo.szTaskId, 
											fleeInfo.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"taskId\":\"%s\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER,
											fleeInfo.szSession, fleeInfo.szTaskId);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s,"
										" JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"taskId\":\"\"}", access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
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
										if (doc["session"].GetStringLength()) {
											strcpy_s(keepAlive.szSession, sizeof(keepAlive.szSession),
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["seq"].IsInt()) {
										keepAlive.uiSeq = (unsigned int)doc["seq"].GetInt();
										bValidSeq = true;
									}
									if (doc["datetime"].IsString()) {
										if (doc["datetime"].GetStringLength()) {
											strcpy_s(keepAlive.szDatetime, sizeof(keepAlive.szDatetime),
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidSeq && bValidDatetime) {
										handleAppKeepAlive(&keepAlive, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, "
											"one or more parameter needed, session=%s, seq=%d, datetime=%s\r\n",
											__FUNCTION__, __LINE__, pMsg_->szMsgFrom, keepAlive.szSession,
											keepAlive.uiSeq, keepAlive.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_MODIFY_PASSWD: {
								if (doc.HasMember("session") && doc.HasMember("currPasswd")
									&& doc.HasMember("newPasswd") && doc.HasMember("datetime")) {
									access_service::AppModifyPassword modifyPasswd;
									bool bValidSession = false;
									bool bValidCurrPasswd = false;
									bool bValidNewPasswd = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szSession, sizeof(modifyPasswd.szSession),
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["currPasswd"].IsString()) {
										size_t nSize = doc["currPasswd"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szCurrPassword, sizeof(modifyPasswd.szCurrPassword),
												doc["currPasswd"].GetString());
											bValidCurrPasswd = true;
										}
									}
									if (doc["newPasswd"].IsString()) {
										size_t nSize = doc["newPasswd"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szNewPassword, sizeof(modifyPasswd.szNewPassword),
												doc["newPasswd"].GetString());
											bValidNewPasswd = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(modifyPasswd.szDatetime, sizeof(modifyPasswd.szDatetime),
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidCurrPasswd && bValidNewPasswd && bValidDatetime) {
										modifyPasswd.uiSeq = getNextRequestSequence();
										handleAppModifyAccountPassword(modifyPasswd, pMsg_->szMsgFrom, pMsg_->ullMsgTime, 
											pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, "
											"one or more parameter needed, session=%s, currPasswd=%s, newPasswd=%s, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom,
											modifyPasswd.szSession, modifyPasswd.szCurrPassword, modifyPasswd.szNewPassword,
											modifyPasswd.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"datetime\":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY,
											E_INVALIDPARAMETER, modifyPasswd.szSession, modifyPasswd.szDatetime);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify password from %s, "
										"JSON data format error\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\",\"retcode\":%d,"
										"\"datetime\":\"\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY,
										E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK: {
								if (doc.HasMember("session") && doc.HasMember("taskId")
									&& doc.HasMember("datetime")) {
									access_service::AppQueryTask queryTask;
									bool bValidSession = false;
									bool bValidTask = false;
									bool bValidDatetime = false;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szSession, sizeof(queryTask.szSession),
												doc["session"].GetString());
											bValidSession = true;
										}
									}
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szTaskId, sizeof(queryTask.szTaskId),
												doc["taskId"].GetString());
											bValidTask = true;
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(queryTask.szDatetime, sizeof(queryTask.szDatetime),
												doc["datetime"].GetString());
											bValidDatetime = true;
										}
									}
									if (bValidSession && bValidTask && bValidDatetime) {
										queryTask.uiQuerySeq = getNextRequestSequence();
										handleAppQueryTask(queryTask, pMsg_->szMsgFrom, pMsg_->ullMsgTime, pMsg_->szAccessFrom);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, one or more "
											"parameter miss, session=%s, task=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, 
											pMsg_->szMsgFrom, queryTask.szSession, queryTask.szTaskId, queryTask.szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										char szReply[256] = { 0 };
										sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
											"\"datetime\":\"%s\",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, 
											E_INVALIDPARAMETER, queryTask.szSession, queryTask.szDatetime);
										sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
											access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
									}
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, "
										"JSON data format error=%d\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom,
										doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"datetime\":\"\",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY,
										E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
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
									handleAppDeviceCommand(devCmdInfo, pMsg_->szMsgFrom, pMsg_->szAccessFrom);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, "
										"JSON data format error=%d\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, 
										doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\","
										"\"device\":\"\",\"seq\":0,\"datetime\":\"\"}",
										access_service::E_CMD_DEVICE_COMMAND_REPLY, E_INVALIDPARAMETER);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_QUERY_PERSON: {
								if (doc.HasMember("session") && doc.HasMember("queryPid") && doc.HasMember("seq")
									&& doc.HasMember("datetime") && doc.HasMember("queryMode")) {
									access_service::AppQueryPerson qryPerson;
									if (doc["session"].IsString()) {
										size_t nSize = doc["session"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szSession, sizeof(qryPerson.szSession),
												doc["session"].GetString());
										}
									}
									if (doc["queryPid"].IsString()) {
										size_t nSize = doc["queryPid"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szQryPersonId, sizeof(qryPerson.szQryPersonId),
												doc["queryPid"].GetString());
										}
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(qryPerson.szQryDatetime, sizeof(qryPerson.szQryDatetime),
												doc["datetime"].GetString());
										}
									}
									if (doc["seq"].IsUint()) {
										qryPerson.uiQeurySeq = doc["seq"].GetUint();
									}
									if (doc["queryMode"].IsInt()) {
										qryPerson.nQryMode = doc["queryMode"].GetInt();
									}
									handleAppQueryPerson(qryPerson, pMsg_->szMsgFrom, pMsg_->szAccessFrom);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send query person from %s, "
										"JSON data format error=%d\r\n", __FUNCTION__, __LINE__, pMsg_->szMsgFrom, 
										doc.GetParseError());
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\","
										"\"datetime\":\"\",\"seq\":0,\"personList\":[]}",
										access_service::E_CMD_QUERY_PERSON_REPLY);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK_LIST: {
								if (doc.HasMember("orgId") && doc.HasMember("seq") && doc.HasMember("datetime")) {
									access_service::AppQueryTaskList qryTaskList;
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(qryTaskList.szOrgId, sizeof(qryTaskList.szOrgId),
												doc["orgId"].GetString());
										}
									}
									if (doc["seq"].IsInt()) {
										qryTaskList.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(qryTaskList.szDatetime, sizeof(qryTaskList.szDatetime),
												doc["datetime"].GetString());
										}
									}
									if (strlen(qryTaskList.szDatetime)) {
										handleAppQueryTaskList(&qryTaskList, pMsg_->szMsgFrom, pMsg_->szAccessFrom);
									}
								}
								break;
							}
							case access_service::E_CMD_QUERY_DEVICE_STATUS: {
								access_service::AppQueryDeviceStatus qryDevStatus;
								memset(&qryDevStatus, 0, sizeof(qryDevStatus));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(qryDevStatus.szSession, sizeof(qryDevStatus.szSession),
											doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDeviceId, sizeof(qryDevStatus.szDeviceId),
											doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId),
											doc["factoryId"].GetString());
									}
								}
								else {
									sprintf_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId), "01");
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										qryDevStatus.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDatetime, sizeof(qryDevStatus.szDatetime),
											doc["datetime"].GetString());
									}
								}
								if (strlen(qryDevStatus.szSession) && strlen(qryDevStatus.szDeviceId)
									&& strlen(qryDevStatus.szDatetime)) {
									handleAppQueryDeviceStatus(&qryDevStatus, pMsg_->szMsgFrom, pMsg_->szAccessFrom);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]recv app query device status "
										"data miss parameter, deviceId=%s, session=%s, seq=%u, datetime=%s\r\n", 
										__FUNCTION__, __LINE__, qryDevStatus.szDeviceId, qryDevStatus.szSession, 
										qryDevStatus.uiQrySeq, qryDevStatus.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\","
										"\"retcode\":%d,\"deviceId\":\"%s\",\"status\":%d,\"battery\":%d,"
										"\"seq\":%u,\"datetime\":\"%s\"}",
										access_service::E_CMD_QUERY_DEVICE_STATUS, qryDevStatus.szSession,
										E_INVALIDPARAMETER, qryDevStatus.szDeviceId, 0, 0, qryDevStatus.uiQrySeq,
										qryDevStatus.szDatetime);
									sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
										access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]can't recognise command: %d\r\n", 
									__FUNCTION__, __LINE__, nCmd);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								char szReply[256] = { 0 };
								sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}", access_service::E_CMD_DEFAULT_REPLY,
									E_INVALIDCOMMAND);
								sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
									access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]can't parse JSON content: %s\r\n", 
							__FUNCTION__, __LINE__, pContent);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						char szReply[256] = { 0 };
						sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}",
							access_service::E_CMD_DEFAULT_REPLY, E_INVALIDCOMMAND);
						sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pMsg_->szMsgFrom,
							access_service::E_ACC_DATA_TRANSFER, pMsg_->szAccessFrom);
					}
					delete[] pContent;
					pContent = NULL;
					delete[] pContent2;
					pContent2 = NULL;
				}
			} while (1);
			free(pBuf);
			pBuf = NULL;
			uiBufLen = 0;
		}
	}
}

int AccessService::getWholeMessage(const unsigned char * pData_, unsigned int uiDataLen_, unsigned int uiIndex_, 
	unsigned int & uiBeginIndex_, unsigned int & uiEndIndex_, unsigned int & uiUnitLen_)
{
	int result = 0;
	unsigned int i = uiIndex_;
	size_t nHeadSize = sizeof(access_service::AppMessageHead);
	access_service::AppMessageHead msgHead;
	bool bFindValidHead = false;
	do {
		if (i >= uiDataLen_) {
			break;
		}
		if (!bFindValidHead) {
			if (uiDataLen_ - i < nHeadSize) {
				break;
			}
			memcpy_s(&msgHead, nHeadSize, pData_ + i, nHeadSize);
			if (msgHead.marker[0] == 'E' && msgHead.marker[1] == 'C') {
				bFindValidHead = true;
				result = 1;
				i += (unsigned int)nHeadSize;
				uiBeginIndex_ = i;
				uiUnitLen_ = msgHead.uiDataLen;
			}
			else {
				i++;
			}
		}
		else {
			if (i + msgHead.uiDataLen <= uiDataLen_) {
				uiBeginIndex_ = i;
				uiEndIndex_ = i + msgHead.uiDataLen;
				result = 2;
			}
			break;
		}
	} while (1);
	return result;
}

void AccessService::decryptMessage(unsigned char * pData_, unsigned int uiBeginIndex_, 
	unsigned int uiEndIndex_)
{
	if (uiEndIndex_ > uiBeginIndex_ && uiBeginIndex_ >= 0) {
		for (unsigned int i = uiBeginIndex_; i < uiEndIndex_; i++) {
			pData_[i] ^= gSecret;
			pData_[i] -= 1;
		}
	}
}

void AccessService::encryptMessage(unsigned char * pData_, unsigned int uiBeginIndex_, 
	unsigned int uiEndIndex_)
{
	if (uiEndIndex_ > uiBeginIndex_ && uiBeginIndex_ >= 0) {
		for (unsigned int i = uiBeginIndex_; i < uiEndIndex_; i++) {
			pData_[i] += 1;
			pData_[i] ^= gSecret;
		}
	}
}

//action: login 
//condition:  c1-check guarder(user) wether "login" already 
//           or consider the matter <guarder(user) re-login>,
//how to handle check passwd
void AccessService::handleAppLogin(access_service::AppLoginInfo loginInfo_, const char * pEndpoint_, 
	unsigned long long ullTime_, const char * pFrom_)
{
	char szLog[512] = { 0 };
	if (!getLoadSessionFlag()) {
		char szReply[256] = { 0 };
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, E_SERVER_RESOURCE_NOT_READY);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request received from %s, user=%s, "
			"server not ready yet, wait load session information\r\n", __FUNCTION__, __LINE__, pEndpoint_, loginInfo_.szUser);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	int nErr = E_OK;
	bool bHaveTask = false;
	char szCurrentTaskId[20] = { 0 };
	char szCurrentBindDeviceId[20] = { 0 };
	char szCurrentSession[20] = { 0 };
	char szLastestSession[20] = { 0 };
	bool bNeedGenerateSession = false; 
	unsigned short usDeviceBattery = 0;
	unsigned short usDeviceStatus = 0;
	unsigned short usDeviceOnline = 0;
	pthread_mutex_lock(&g_mutex4GuarderList);
	if (zhash_size(g_guarderList)) {
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo_.szUser);
		if (pGuarder) {
			if ((pGuarder->usRoleType & USER_ROLE_OPERATOR) != 0) {
				if (strlen(pGuarder->szCurrentSession) > 0) {
					strcpy_s(szCurrentSession, sizeof(szCurrentSession), pGuarder->szCurrentSession);
					if (pGuarder->usLoginFormat == LOGIN_FORMAT_MQ) {
						bool bContinue = true;
						if (difftime(time(NULL), (time_t)pGuarder->ullActiveTime) < 120) {
							nErr = E_ACCOUNTINUSE;
							bContinue = false;
						}
						if (bContinue) {
							if (strcmp(pGuarder->szPassword, loginInfo_.szPasswd) == 0) {
								if (strlen(pGuarder->szBindDevice)) {
									strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
								}
								if (pGuarder->usState == STATE_GUARDER_DUTY) {
									if (strlen(pGuarder->szTaskId)) {
										strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
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
					else {
						if (strlen(pGuarder->szLink) > 0) {
							if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, already have link=%s in user, "
									"from=%s\r\n", __FUNCTION__, __LINE__, loginInfo_.szUser, pGuarder->szLink, pEndpoint_);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								nErr = E_ACCOUNTINUSE;
							}
							else { //same link
								if (strcmp(pGuarder->szPassword, loginInfo_.szPasswd) == 0) {
									if (pGuarder->usState == STATE_GUARDER_BIND) {
										if (strlen(pGuarder->szBindDevice)) {
											strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
										}
									}
									else if (pGuarder->usState == STATE_GUARDER_DUTY) {
										if (strlen(pGuarder->szTaskId) > 0) {
											strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
											bHaveTask = true;
										}
										if (strlen(pGuarder->szBindDevice)) {
											strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
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
							if (strcmp(pGuarder->szPassword, loginInfo_.szPasswd) == 0) {
								if (strlen(pGuarder->szBindDevice)) {
									strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
								}
								if (pGuarder->usState == STATE_GUARDER_DUTY) {
									if (strlen(pGuarder->szTaskId)) {
										strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
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
				else { //no session
					if (pGuarder->usState == STATE_GUARDER_DEACTIVATE) {
						nErr = E_UNAUTHORIZEDACCOUNT;
					}
					else {
						if (strcmp(pGuarder->szPassword, loginInfo_.szPasswd) == 0) {
							if (pGuarder->usState == STATE_GUARDER_DUTY) {
								if (strlen(pGuarder->szTaskId) > 0) {
									strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
									strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
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
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]guarder list is empty, need reload data\r\n",
			__FUNCTION__, __LINE__);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
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
				strcpy_s(szLastestSession, sizeof(szLastestSession), szCurrentSession);
			}
		}
		EscortTask * pCurrentTask = NULL;
		bool bModifyTask = false;
		if (strlen(szCurrentTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrentTaskId);
				if (pTask) {
					strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pTask->szDeviceId);
					if (pTask->nTaskMode == 0) {
						if (strlen(loginInfo_.szHandset)) {
							pTask->nTaskMode = 1;
							strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo_.szHandset);
							bModifyTask = true;
						}
					}
					else if (pTask->nTaskMode == 1) {
						if (strlen(loginInfo_.szHandset)) {//current login with handset
							if (strlen(pTask->szHandset)) { //task last handset is not empty
								if (strcmp(pTask->szHandset, loginInfo_.szHandset) != 0) { //change task handset
									bModifyTask = true;
									strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo_.szHandset);
								}
							}
							else { //task last handset is empty
								bModifyTask = true;
								strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), loginInfo_.szHandset);
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
		if (strlen(szCurrentBindDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szCurrentBindDeviceId);
				if (pDevice) {
					usDeviceStatus = pDevice->deviceBasic.nStatus;
					usDeviceBattery = pDevice->deviceBasic.nBattery;
					usDeviceOnline = pDevice->deviceBasic.nOnline;
					strcpy_s(szFactoryId, sizeof(szFactoryId), pDevice->deviceBasic.szFactoryId);
					strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bHaveTask && pCurrentTask) {
			sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,\"limit\":%d,"
				"\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%hu,\"deviceState\":%hu,"
				"\"online\":%hu,\"handset\":\"%s\"}", pCurrentTask->szTaskId, pCurrentTask->szDeviceId, pCurrentTask->nTaskType + 1,
				pCurrentTask->nTaskLimitDistance, pCurrentTask->szDestination, pCurrentTask->szTarget, pCurrentTask->szTaskStartTime, 
				usDeviceBattery, usDeviceStatus, usDeviceOnline, loginInfo_.szHandset);
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}", 
				access_service::E_CMD_LOGIN_REPLY, szLastestSession, szTaskInfo);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		}
		else {
			if (strlen(szCurrentBindDeviceId)) {
				sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"deviceId\":\"%s\",\"battery\":%hu,\"deviceState\":%hu,\"online\":%hu}",
					szCurrentBindDeviceId, usDeviceBattery, usDeviceStatus, usDeviceOnline);
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}", 
					access_service::E_CMD_LOGIN_REPLY, szLastestSession, "");
				sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			}
			else {
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[]}", 
					access_service::E_CMD_LOGIN_REPLY, szLastestSession);
				sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			}
		}
		if (bModifyTask) {
			char szBody[512] = { 0 };
			sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
				"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"handset\":\"%s\",\"session\":\"%s\"}]}", MSG_SUB_REPORT, 
				loginInfo_.uiReqSeq, loginInfo_.szDateTime, SUB_REPORT_TASK_MODIFY, pCurrentTask->szTaskId, loginInfo_.szHandset,
				szLastestSession);
			sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
		}
		//update guarder
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, loginInfo_.szUser);
		if (pGuarder) {
			strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szLastestSession);
			strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
			pGuarder->usLoginFormat = LOGIN_FORMAT_TCP_SOCKET;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		//update link
		size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
		strcpy_s(pLink->szGuarder, sizeof(pLink->szGuarder), loginInfo_.szUser);
		strcpy_s(pLink->szSession, sizeof(pLink->szSession), szLastestSession);
		strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
		if (strlen(loginInfo_.szHandset)) {
			strcpy_s(pLink->szHandset, sizeof(pLink->szHandset), loginInfo_.szHandset);
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
		pLink->ulActivateTime = ullTime_;
		pLink->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
		
		pthread_mutex_lock(&m_mutex4LinkList);
		if (bNeedGenerateSession && strlen(szCurrentSession)) {
			zhash_delete(m_linkList, szCurrentSession);
		}
		zhash_update(m_linkList, szLastestSession, pLink);
		zhash_freefn(m_linkList, szLastestSession, free);
		pthread_mutex_unlock(&m_mutex4LinkList);

		zkAddSession(szLastestSession);
		char szUploadMsg[512] = { 0 };
		sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\":\"%s\","
			"\"loginFormat\":%u}]}", MSG_SUB_REPORT, getNextRequestSequence(), loginInfo_.szDateTime, SUB_REPORT_GUARDER_LOGIN,
			loginInfo_.szUser, szLastestSession, loginInfo_.szHandset, LOGIN_FORMAT_TCP_SOCKET);
		sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));
		
		pthread_mutex_lock(&m_mutex4LinkDataList); 
		std::string strLink = pEndpoint_;
		LinkDataList::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), loginInfo_.szUser);
		}
		else {
			auto pLinkData = new access_service::LinkDataInfo();
			memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
			strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
			strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), loginInfo_.szUser);
			m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);

		sprintf_s(szLog, sizeof(szLog), "[acess_service]%s[%d]login user=%s, datetime=%s, handset=%s, result=%d, session=%s, "
			"taskId=%s, deviceId=%s, from link=%s\n", __FUNCTION__, __LINE__, loginInfo_.szUser, loginInfo_.szDateTime,
			loginInfo_.szHandset, nErr, szLastestSession, (pCurrentTask != NULL) ? pCurrentTask->szTaskId : "", 
			(pCurrentTask != NULL) ? pCurrentTask->szDeviceId : "", pEndpoint_);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

		if (pCurrentTask) {
			char szTopic[64] = { 0 };
			sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pCurrentTask->szOrg, pCurrentTask->szFactoryId, pCurrentTask->szDeviceId);
			access_service::AppSubscribeInfo * pSubInfo;
			size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
			pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
			strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
			strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pCurrentTask->szGuarder);
			strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), szLastestSession);
			strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
			strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
			pSubInfo->nFormat = LOGIN_FORMAT_TCP_SOCKET;

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
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, nErr);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, datetime=%s, handset=%s, result=%d, "
			"session=%s, from=%s\r\n", __FUNCTION__, __LINE__, loginInfo_.szUser, loginInfo_.szDateTime,
			loginInfo_.szHandset, nErr, szLastestSession, pEndpoint_);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleAppLoginV2(access_service::AppLoginInfo * pLoginInfo_, const char * pEndpoint_,
	const char * pFrom_, unsigned long long ullTime_)
{
	char szLog[512] = { 0 };
	if (!getLoadSessionFlag()) {
		char szReply[256] = { 0 };
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
			access_service::E_CMD_LOGIN_REPLY, E_SERVER_RESOURCE_NOT_READY);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request received from %s:%s, userId=%s, server not ready"
			", wait load session information\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pLoginInfo_->szUser);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
	}
	else {
		int nErr = E_OK;
		char szCurrentSession[20] = { 0 };
		char szTaskId[16] = { 0 };
		bool bDuplicateLogin = false;
		unsigned long long ullCurrentTime = time(NULL);
		pthread_mutex_lock(&g_mutex4GuarderList);
		auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLoginInfo_->szUser);
		if (pGuarder) {
			if ((pGuarder->usRoleType & USER_ROLE_OPERATOR) != 0) {
				bool bContinue = true;
				if (strlen(pGuarder->szCurrentSession)) {
					strcpy_s(szCurrentSession, sizeof(szCurrentSession), pGuarder->szCurrentSession);
					if (pGuarder->usLoginFormat == LOGIN_FORMAT_MQ) {
						if (strlen(pGuarder->szLink)) {
							if (difftime(ullCurrentTime, pGuarder->ullActiveTime) < 120.00) {
								bContinue = false;
								nErr = E_ACCOUNTINUSE;
							}
						}
					}
					else { //TCP_SOCKET
						if (strlen(pGuarder->szLink)) {
							if (strcmp(pGuarder->szLink, pEndpoint_) == 0) { //the same endpoint
								bDuplicateLogin = true;
							}
							else {
								if (difftime(ullCurrentTime, pGuarder->ullActiveTime) < 120.00) {
									bContinue = false;
									nErr = E_ACCOUNTINUSE;
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]login userId=%s, from %s, already have link=%s\n",
										__FUNCTION__, __LINE__, pLoginInfo_->szUser, pEndpoint_, pGuarder->szLink);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								}
							}
						}
					}
				}
				if (bContinue) {
					if (strcmp(pGuarder->szPassword, pLoginInfo_->szPasswd) == 0) {
						if (pGuarder->usState == STATE_GUARDER_DUTY) { //
							strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
						}
					}
					else {
						nErr = E_INVALIDPASSWD;
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
		pthread_mutex_unlock(&g_mutex4GuarderList);
		if (nErr == E_OK) {
			escort::EscortTask * pCurrentTask = NULL;
			bool bModifyTask = false;
			char szTaskInfo[1024] = { 0 };
			if (strlen(szTaskId)) {
				pthread_mutex_lock(&g_mutex4TaskId);
				auto pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
				if (pTask) {
					if (pTask->nTaskMode == 0) {
						if (strlen(pLoginInfo_->szHandset)) {
							pTask->nTaskMode = 1;
							strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
							bModifyTask = true;
						}
					}
					else {
						if (strlen(pLoginInfo_->szHandset)) {
							if (strlen(pTask->szHandset)) {
								if (strcmp(pTask->szHandset, pLoginInfo_->szHandset) != 0) {
									strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
									bModifyTask = true;
								}
							}
							else {
								strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
								bModifyTask = true;
							}
						}
						else {
							if (strlen(pTask->szHandset)) {
								pTask->szHandset[0] = '\0';
								bModifyTask = true;
							}
						}
					}
					pCurrentTask = new EscortTask();
					size_t nTaskSize = sizeof(escort::EscortTask);
					memcpy_s(pCurrentTask, nTaskSize, pTask, nTaskSize);
				}
				pthread_mutex_unlock(&g_mutex4TaskId);

				unsigned short usDeviceOnline, usDeviceState, usDeviceBattery;
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pCurrentTask->szDeviceId);
				if (pDevice) {
					usDeviceOnline = pDevice->deviceBasic.nOnline;
					usDeviceState = pDevice->deviceBasic.nStatus;
					usDeviceBattery = pDevice->deviceBasic.nBattery;
				}
				pthread_mutex_unlock(&g_mutex4DevList);

				sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%hu,\"limit\":%hu,"
					"\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%hu,\"deviceState\":%hu,\"online\":%hu,"
					"\"handset\":\"%s\"}", pCurrentTask->szTaskId, pCurrentTask->szDeviceId, pCurrentTask->nTaskType + 1,
					pCurrentTask->nTaskLimitDistance, pCurrentTask->szDestination, pCurrentTask->szTarget, pCurrentTask->szTaskStartTime,
					usDeviceBattery, usDeviceState, usDeviceOnline, pLoginInfo_->szHandset);
			}

			char szLastestSession[20] = { 0 };
			if (!bDuplicateLogin) {
				generateSession(szLastestSession, sizeof(szLastestSession));
			} 
			pthread_mutex_lock(&g_mutex4GuarderList);
			pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLoginInfo_->szUser);
			if (pGuarder) {
				if (!bDuplicateLogin) {
					strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szLastestSession);
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
				}
				pGuarder->ullActiveTime = ullCurrentTime;
				pGuarder->usLoginFormat = LOGIN_FORMAT_TCP_SOCKET;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			if (!bDuplicateLogin) {
				size_t nSessionSize = sizeof(access_service::AppLinkInfo);
				auto pSession = (access_service::AppLinkInfo *)malloc(nSessionSize);
				memset(pSession, 0, nSessionSize);
				pSession->nActivated = 1;
				strcpy_s(pSession->szSession, sizeof(pSession->szSession), szLastestSession);
				strcpy_s(pSession->szGuarder, sizeof(pSession->szGuarder), pLoginInfo_->szUser);
				strcpy_s(pSession->szEndpoint, sizeof(pSession->szEndpoint), pEndpoint_);
				pSession->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
				if (strlen(pLoginInfo_->szHandset)) {
					strcpy_s(pSession->szHandset, sizeof(pSession->szHandset), pLoginInfo_->szHandset);
				}
				if (pCurrentTask) {
					strcpy_s(pSession->szTaskId, sizeof(pSession->szTaskId), pCurrentTask->szTaskId);
					strcpy_s(pSession->szDeviceId, sizeof(pSession->szDeviceId), pCurrentTask->szDeviceId);
					strcpy_s(pSession->szFactoryId, sizeof(pSession->szFactoryId), pCurrentTask->szFactoryId);
					strcpy_s(pSession->szOrg, sizeof(pSession->szOrg), pCurrentTask->szOrg);
				}
				pthread_mutex_lock(&m_mutex4LinkList);
				zhash_update(m_linkList, szLastestSession, pSession);
				zhash_freefn(m_linkList, szLastestSession, free);
				if (strlen(szCurrentSession)) {
					zhash_delete(m_linkList, szCurrentSession);
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				
				size_t nReplySize = strlen(szTaskInfo) + 256;
				char * pReply = new char[nReplySize];
				sprintf_s(pReply, nReplySize, "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskInfo\":[%s]}",
					access_service::E_CMD_LOGIN_REPLY, nErr, szLastestSession, szTaskInfo);
				sendDataToEndpoint_v2(pReply, (uint32_t)strlen(pReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
				
				delete[] pReply;
				pReply = NULL;

				zkAddSession(szLastestSession);
				
				char szUploadMsg[512] = { 0 };
				sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\":\"%s\","
					"\"loginFormat\":%u}]}", MSG_SUB_REPORT, getNextRequestSequence(), pLoginInfo_->szDateTime, SUB_REPORT_GUARDER_LOGIN,
					pLoginInfo_->szUser, szLastestSession, pLoginInfo_->szHandset, LOGIN_FORMAT_TCP_SOCKET);
				sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));

				if (pCurrentTask) {
					char szTopic[80] = { 0 };
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pCurrentTask->szOrg, pCurrentTask->szFactoryId, pCurrentTask->szDeviceId);
					size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);

					pthread_mutex_lock(&m_mutex4SubscribeList);
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szTopic);
					pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
					pSubInfo->nFormat = LOGIN_FORMAT_TCP_SOCKET;
					strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pLoginInfo_->szUser);
					strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), szLastestSession);
					strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
					strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					zhash_update(m_subscribeList, szTopic, pSubInfo);
					zhash_freefn(m_subscribeList, szTopic, free);
					pthread_mutex_unlock(&m_mutex4SubscribeList);
				}

			}
			else {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szLastestSession);
				if (pSession) {
					pSession->nActivated = 1;
					pSession->ulActivateTime = ullCurrentTime;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
			}

			pthread_mutex_lock(&m_mutex4LinkDataList);
			LinkDataList::iterator iter = m_linkDataList.find(pEndpoint_);
			if (iter != m_linkDataList.end()) {
				auto pLinkData = iter->second;
				if (pLinkData) {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), pLoginInfo_->szUser);
				}
			}
			else {
				auto pLinkData = new access_service::LinkDataInfo();
				memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
				strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), pLoginInfo_->szUser);
				m_linkDataList.emplace(pEndpoint_, pLinkData);
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);

			if (bModifyTask) {
				char szModifyTaskMsg[512] = { 0 };
				sprintf_s(szModifyTaskMsg, sizeof(szModifyTaskMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"handset\":\"%s\",\"session\":\"%s\"}]}",
					MSG_SUB_REPORT, getNextRequestSequence(), pLoginInfo_->szDateTime, SUB_REPORT_TASK_MODIFY, szTaskId, 
					pLoginInfo_->szHandset, szLastestSession);
				sendDataViaInteractor_v2(szModifyTaskMsg, (uint32_t)strlen(szModifyTaskMsg));
			}

			if (pCurrentTask) {
				delete pCurrentTask;
				pCurrentTask = NULL;
			}
		}
		else {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
				access_service::E_CMD_LOGIN_REPLY, nErr);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]login userId=%s, datetime=%s, handset=%s, result=%d, "
				"from=%s:%s\n", __FUNCTION__, __LINE__, pLoginInfo_->szUser, pLoginInfo_->szDateTime, pLoginInfo_->szHandset,
				nErr, pEndpoint_, pFrom_);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void AccessService::handleAppLogout(access_service::AppLogoutInfo logoutInfo_, const char * pEndPoint_, 
	unsigned long long ulTime_, const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	bool bFindLink = false;
	char szReply[256] = { 0 };
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szHandset[64] = { 0 };
	bool bUpdateLink = false;
	char szLastEndpoint[32] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_LOGOUT_REPLY, E_SERVER_RESOURCE_NOT_READY, logoutInfo_.szSession);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndPoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive logout from %s, session=%s, server not ready\r\n", 
			__FUNCTION__, __LINE__, pEndPoint_, logoutInfo_.szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	//logoutInfo.szSession
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			logoutInfo_.szSession);
		if (pLink) {
			pLink->ulActivateTime = ulTime_;
			strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			if (strcmp(pLink->szEndpoint, pEndPoint_) != 0) {
				if (strlen(pLink->szEndpoint)) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pLink->szEndpoint);
				}
				strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndPoint_);
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
				if (strcmp(pGuarder->szLink, pEndPoint_) != 0) {
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint_);
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}", 
		access_service::E_CMD_LOGOUT_REPLY, nErr, logoutInfo_.szSession);
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndPoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		//delete session link
		pthread_mutex_lock(&m_mutex4LinkList);
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, logoutInfo_.szSession);
		if (pLink) {
			strcpy_s(szDevKey, sizeof(szDevKey), pLink->szDeviceId);
			strcpy_s(szHandset, sizeof(szHandset), pLink->szHandset);
		}
		zhash_delete(m_linkList, logoutInfo_.szSession);
		pthread_mutex_unlock(&m_mutex4LinkList);

		zkRemoveSession(logoutInfo_.szSession);

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
		sprintf_s(szUpdateMsg, sizeof(szUpdateMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
			"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\":\"%s\"}]}",
			MSG_SUB_REPORT, getNextRequestSequence(), logoutInfo_.szDateTime, SUB_REPORT_GUARDER_LOGOUT, szGuarder, 
			logoutInfo_.szSession, szHandset);
		sendDataViaInteractor_v2(szUpdateMsg, (uint32_t)strlen(szUpdateMsg));
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		LinkDataList::iterator iter = m_linkDataList.find((std::string)pEndPoint_);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				if (nErr == E_OK) {
					pLinkData->szUser[0] = '\0';
				}
				else {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				}
			}
			if (strlen(szLastEndpoint)) {
				LinkDataList::iterator iter2 = m_linkDataList.find((std::string)szLastEndpoint);
				if (iter2 != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData2 = iter2->second;
					if (pLinkData2) {
						if (pLinkData2->pLingeData) {
							delete[] pLinkData2->pLingeData;
							pLinkData2->pLingeData = NULL;
						}
						delete pLinkData2;
						pLinkData2 = NULL;
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout session=%s, datetime=%s, user=%s, result=%d, from=%s\n",
		__FUNCTION__, __LINE__, logoutInfo_.szSession, logoutInfo_.szDateTime, szGuarder, nErr, pEndPoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppBind(access_service::AppBindInfo bindInfo_, const char * pEndPoint_, unsigned long long ullTime_,
	const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		if (bindInfo_.nMode == 0) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"guarderId\":\"\",\"deviceId\":\"%s\","
				"\"battery\":0,\"status\":0}", access_service::E_CMD_BIND_REPLY, E_SERVER_RESOURCE_NOT_READY, bindInfo_.szSesssion,
				bindInfo_.szDeviceId);
		}
		else {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}", access_service::E_CMD_UNBIND_REPLY,
				E_SERVER_RESOURCE_NOT_READY, bindInfo_.szSesssion);
		}
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndPoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive bind request from %s, deviceId=%s, session=%s, server not "
			"ready yet, still wait for session information\r\n", __FUNCTION__, __LINE__, pEndPoint_, bindInfo_.szDeviceId, 
			bindInfo_.szSesssion);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	bool bFindLink = false;
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szFactory[4] = { 0 };
	char szBindDevice[20] = { 0 };
	char szBindGuarder[20] = { 0 };
	char szOrg[40] = { 0 };
	unsigned short usBattery = 0;
	unsigned short usStatus = 0;
	bool bBindAlready = false;
	bool bUpdateLink = false;
	char szLastEndpoint[32] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, bindInfo_.szSesssion);
		if (pLink) {
			bFindLink = true;
			pLink->nActivated = 1;
			pLink->ulActivateTime = ullTime_;
			strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			if (strcmp(pLink->szEndpoint, pEndPoint_) != 0) {
				if (strlen(pLink->szEndpoint)) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pLink->szEndpoint);
				}
				strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndPoint_);
				bUpdateLink = true;
			}
		}
		else {
			nErr = E_INVALIDSESSION;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bFindLink) {
		bool bValidGuarder = false;
		do {
			if (bindInfo_.nMode == 0) { //execute bind
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strlen(pGuarder->szTaskId)) {
						nErr = E_GUARDERINDUTY;
					}
					else {
						if (strlen(pGuarder->szBindDevice) && strcmp(pGuarder->szBindDevice, bindInfo_.szDeviceId) == 0) {
							bBindAlready = true;
						}
						if (strcmp(pGuarder->szLink, pEndPoint_) != 0) {
							strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndPoint_);
						}
						bValidGuarder = true;
					}
					strcpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg);
				}
				else {
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]not find guarder, guarder=%s\r\n",
						__FUNCTION__, __LINE__, szGuarder);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					nErr = E_INVALIDACCOUNT;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (!bValidGuarder) {
					break;
				}
				bool bFindDevice = false;
				pthread_mutex_lock(&g_mutex4DevList);
				if (zhash_size(g_deviceList)) {
					WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo_.szDeviceId);
					if (pDev) {
						bFindDevice = true;
						if (pDev->deviceBasic.nOnline == 0) {
							nErr = E_UNABLEWORKDEVICE;
						}
						else {
							if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
								|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
								nErr = E_ALREADYBINDDEVICE;
							}
							else {
								strcpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), szGuarder);
							}
						}
						usBattery = pDev->deviceBasic.nBattery;
						usStatus = pDev->deviceBasic.nStatus;
						strcpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId);
						if (strlen(pDev->deviceBasic.szOrgId)) {
							if (strcmp(pDev->deviceBasic.szOrgId, szOrg) != 0) {
								strcpy_s(szOrg, sizeof(szOrg), pDev->deviceBasic.szOrgId);
							}
						}
						else {
							strcpy_s(pDev->deviceBasic.szOrgId, sizeof(pDev->deviceBasic.szOrgId), szOrg);
						}
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				if (!bFindDevice) {
					nErr = E_INVALIDDEVICE;
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
							if (strcmp(pGuarder->szBindDevice, bindInfo_.szDeviceId) != 0) {
								nErr = E_DEVICENOTMATCH;
							}
							else {
								bValidGuarder = true;
							}
						}
						strcpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg);
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (!bValidGuarder) {
					break;
				}
				pthread_mutex_lock(&g_mutex4DevList);
				if (zhash_size(g_deviceList)) {
					WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo_.szDeviceId);
					if (pDev) {
						if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
							|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							nErr = E_DEVICEINDUTY;
						}
						else {
							if (strlen(pDev->szBindGuard) && strcmp(pDev->szBindGuard, szGuarder) != 0) {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]device already bind guarder=%s,"
									" wanna bind guarder=%s, not match\r\n", __FUNCTION__, __LINE__, pDev->szBindGuard,
									szGuarder);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								nErr = E_GUARDERNOTMATCH;
							}
						}
						strcpy_s(szFactory, sizeof(szFactory), pDev->deviceBasic.szFactoryId);
						strcpy_s(szOrg, sizeof(szOrg), pDev->deviceBasic.szOrgId);
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
	if (bindInfo_.nMode == 0) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"guarderId\":\"%s\",\"deviceId\":\"%s\","
			"\"battery\":%u,\"status\":%u}", access_service::E_CMD_BIND_REPLY, nErr, bindInfo_.szSesssion, szGuarder, 
			bindInfo_.szDeviceId, (nErr == E_OK) ? usBattery : 0, (nErr == E_OK) ? usStatus : 0);
	}
	else {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_UNBIND_REPLY, nErr, bindInfo_.szSesssion);
	}
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndPoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			LinkDataList::iterator iter = m_linkDataList.find((std::string)pEndPoint_);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				}
			}
			else {
				access_service::LinkDataInfo * pLinkData = new access_service::LinkDataInfo();
				memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
				strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndPoint_);
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				m_linkDataList.emplace(pEndPoint_, pLinkData);
			}
			if (strlen(szLastEndpoint)) {
				LinkDataList::iterator iter2 = m_linkDataList.find((std::string)szLastEndpoint);
				if (iter2 != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData2 = iter2->second;
					if (pLinkData2) {
						if (pLinkData2->pLingeData) {
							delete[] pLinkData2->pLingeData;
							pLinkData2->pLingeData = NULL;
						}
						delete pLinkData2;
						pLinkData2 = NULL;
					}
					m_linkDataList.erase(iter2);
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	if (nErr == E_OK) {
		if (!bBindAlready) {
			//1.update information; 2.send to M_S
			if (bindInfo_.nMode == 0) {
				//mark,version,type,sequence,datetime,report[subType,factoryId,deviceId,guarder]
				char szBody[512] = { 0 };
				sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\""
					",\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"session\":\"%s\"}]}", 
					MSG_SUB_REPORT, bindInfo_.uiReqSeq, bindInfo_.szDateTime, SUB_REPORT_DEVICE_BIND, szFactory, bindInfo_.szDeviceId, 
					szGuarder, bindInfo_.szSesssion);
				sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), bindInfo_.szDeviceId);
					pGuarder->usState = STATE_GUARDER_BIND;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);

				pthread_mutex_lock(&m_mutex4LinkList);
				access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, bindInfo_.szSesssion);
				if (pLink) {
					strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactory);
					strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), bindInfo_.szDeviceId);
					strcpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg);
				}
				pthread_mutex_unlock(&m_mutex4LinkList);

				char szTopic[64] = { 0 };
				sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, bindInfo_.szDeviceId);
				access_service::AppSubscribeInfo * pSubInfo;
				size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
				pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
				strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndPoint_);
				strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), szGuarder);
				strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), bindInfo_.szSesssion);
				strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
				strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				zhash_update(m_subscribeList, szTopic, pSubInfo);
				zhash_freefn(m_subscribeList, szTopic, free);
				pthread_mutex_unlock(&m_mutex4SubscribeList);

				zkSetSession(bindInfo_.szSesssion);

				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]add new subscriber for %s, link=%s\r\n", 
					__FUNCTION__, __LINE__, szTopic, pEndPoint_);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
			else {
				char szBody[512] = { 0 };
				sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,\"datetime\":\"%s\","
					"\"report\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"session\":\"%s\"}]}",
					MSG_SUB_REPORT, bindInfo_.uiReqSeq, bindInfo_.szDateTime, SUB_REPORT_DEVICE_UNBIND, szFactory, bindInfo_.szDeviceId,
					szGuarder, bindInfo_.szSesssion);
				sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));

				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->usState = STATE_GUARDER_FREE;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, bindInfo_.szDeviceId);
				if (pDev) {
					pDev->szBindGuard[0] = '\0';
					pDev->ulBindTime = 0;
				}
				pthread_mutex_unlock(&g_mutex4DevList);

				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, bindInfo_.szSesssion);
				if (pLink) {
					pLink->szDeviceId[0] = '\0';
					pLink->szFactoryId[0] = '\0';
					pLink->szOrg[0] = '\0';
					pLink->nNotifyOffline = 0;
					pLink->nNotifyStatus = 0;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				//remove subsciber
				char szTopic[64] = { 0 };
				sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, bindInfo_.szDeviceId);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				zhash_delete(m_subscribeList, szTopic);
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				
				zkSetSession(bindInfo_.szSesssion);
			}
		}
		else {
			//already bind
			pthread_mutex_lock(&m_mutex4LinkList);
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, bindInfo_.szSesssion);
			if (pLink) {
				strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactory);
				strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), bindInfo_.szDeviceId);
				strcpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg);
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			zkSetSession(bindInfo_.szSesssion);
		}
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]seq=%lu, %s, device=%s, factoryId=%s, "
		"session=%s, guarder=%s, datetime=%s, org=%s, result=%d, from=%s\r\n", __FUNCTION__, __LINE__,
		bindInfo_.uiReqSeq, (bindInfo_.nMode == 0) ? "bind" : "unbind", bindInfo_.szDeviceId, 
		strlen(szFactory) ? szFactory : "01", bindInfo_.szSesssion, szGuarder, bindInfo_.szDateTime, 
		szOrg, nErr, pEndPoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppBindV2(access_service::AppBindInfo * pBindInfo_, const char * pEndpoint_, const char * pFrom_,
	unsigned long long ullMsgTime_)
{
	char szLog[512] = { 0 };
	if (pBindInfo_ && strlen(pBindInfo_->szSesssion) && strlen(pBindInfo_->szDeviceId)) {
		if (!getLoadSessionFlag()) {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"guarderId\":\"\",\"deviceId\":\"%s\","
				"\"battery\":0,\"status\":0}", access_service::E_CMD_BIND_REPLY, pBindInfo_->szSesssion, E_SERVER_RESOURCE_NOT_READY, 
				pBindInfo_->szDeviceId);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]bind from %s:%s, session=%s, deviceId=%s, datetime=%s, service "
				"not ready\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pBindInfo_->szSesssion, pBindInfo_->szDeviceId, 
				pBindInfo_->szDateTime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			int nErr = E_OK;
			bool bDuplicateBind = false;
			unsigned long long ullCurrentTime = time(NULL);
			char szGuarder[20] = { 0 };
			bool bValidSession = false;
			bool bValidGuarder = false;
			char szLastEndpoint[40] = { 0 };
			char szReply[256] = { 0 };
			unsigned short usDeviceStatue = 0, usDeviceBattey = 0;

			pthread_mutex_lock(&m_mutex4LinkList);
			auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
			if (pSession) {
				bValidSession = true;
				pSession->nActivated = 1;
				pSession->ulActivateTime = ullCurrentTime;
				if (pSession->nLinkFormat != LOGIN_FORMAT_TCP_SOCKET) {
					pSession->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
				}
				if (strcmp(pSession->szEndpoint, pEndpoint_) != 0) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pSession->szEndpoint);
					strcpy_s(pSession->szEndpoint, sizeof(pSession->szEndpoint), pEndpoint_);
				}
				strcpy_s(szGuarder, sizeof(szGuarder), pSession->szGuarder);
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);

			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (pGuarder->usState == STATE_GUARDER_DUTY) {
						if (strcmp(pGuarder->szBindDevice, pBindInfo_->szDeviceId) == 0) {
							bDuplicateBind = true;
							bValidGuarder = true;
						}
						else {
							nErr = E_GUARDERINDUTY;
						}
					}
					else {
						bValidGuarder = true;
					}
					pGuarder->usLoginFormat = LOGIN_FORMAT_TCP_SOCKET;
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
				}
				else {
					nErr = E_INVALIDACCOUNT;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}

			if (bValidGuarder) {
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (escort::WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
				if (pDevice) {
					if (pDevice->deviceBasic.nOnline == 0) {
						nErr = E_UNABLEWORKDEVICE;
					}
					else {
						if (((pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) 
							|| ((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)) {
							if (strcmp(pDevice->szBindGuard, szGuarder) != 0) {
								nErr = E_DEVICEINDUTY;
							}
						}
					}
					usDeviceBattey = pDevice->deviceBasic.nBattery;
					usDeviceStatue = pDevice->deviceBasic.nStatus;
				}
				else {
					nErr = E_INVALIDDEVICE;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}

			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"guarderId\":\"%s\","
				"\"deviceId\":\"%s\",\"battery\":%hu,\"status\":%hu}", access_service::E_CMD_BIND_REPLY, pBindInfo_->szSesssion,
				nErr, szGuarder, pBindInfo_->szDeviceId, usDeviceBattey, usDeviceStatue);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]bind from %s:%s, session=%s, deviceId=%s, guarderId=%s, "
				"battery=%hu, status=%hu, result=%d\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pBindInfo_->szSesssion,
				pBindInfo_->szDeviceId, szGuarder, usDeviceBattey, usDeviceStatue, nErr);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

			if (nErr == E_OK) {
				if (bDuplicateBind) {
					if (strlen(szLastEndpoint)) {
						char szTopic[80] = { 0 };
						pthread_mutex_lock(&g_mutex4DevList);
						auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
						if (pDevice) {
							sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pDevice->deviceBasic.szOrgId, pDevice->deviceBasic.szFactoryId,
								pDevice->deviceBasic.szDeviceId);
						}
						pthread_mutex_unlock(&g_mutex4DevList);
						if (strlen(szTopic)) {
							pthread_mutex_lock(&m_mutex4SubscribeList);
							auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szTopic);
							if (pSubInfo) {
								pSubInfo->nFormat = LOGIN_FORMAT_TCP_SOCKET;
								strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
								strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
							}
							pthread_mutex_unlock(&m_mutex4SubscribeList);
						}
					}
				}
				else {
					pthread_mutex_lock(&m_mutex4LinkList);
					auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
					if (pSession) {
						strcpy_s(pSession->szDeviceId, sizeof(pSession->szDeviceId), pBindInfo_->szDeviceId);
					}
					pthread_mutex_unlock(&m_mutex4LinkList);
				}
				pthread_mutex_lock(&m_mutex4LinkDataList);
				LinkDataList::iterator iter = m_linkDataList.find(pEndpoint_);
				if (iter != m_linkDataList.end()) {
					auto pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				if (strlen(szLastEndpoint)) {
					LinkDataList::iterator iter2 = m_linkDataList.find(szLastEndpoint);
					if (iter2 != m_linkDataList.end()) {
						auto pLastLinkData = iter2->second;
						if (pLastLinkData) {
							if (pLastLinkData->pLingeData) {
								delete[] pLastLinkData;
								pLastLinkData = NULL;
							}
							delete pLastLinkData;
							pLastLinkData = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkDataList);
			}
		}
	}
}

void AccessService::handleAppUnbindV2(access_service::AppBindInfo * pUnbindInfo_, const char * pEndpoint_, const char * pFrom_,
	unsigned long long ullMsgTime_)
{
	char szLog[512] = { 0 };
	char szReply[256] = { 0 };
	if (pUnbindInfo_ && strlen(pUnbindInfo_->szDeviceId) && strlen(pUnbindInfo_->szSesssion)) {
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d}", 
				access_service::E_CMD_UNBIND_REPLY, pUnbindInfo_->szSesssion, E_SERVER_RESOURCE_NOT_READY);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]unbind from %s:%s, session=%s, deviceId=%s, service not ready,"
				" datetime=%s\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pUnbindInfo_->szSesssion, pUnbindInfo_->szDeviceId, 
				pUnbindInfo_->szDateTime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			int nErr = E_OK;
			char szGuarder[20] = { 0 };
			unsigned long long ullCurrentTime = time(NULL);
			bool bValidSession = false;
			char szLastEndpoint[40] = { 0 };
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pUnbindInfo_->szSesssion);
			if (pSession) {
				bValidSession = true;
				pSession->nActivated = 1;
				pSession->ulActivateTime = ullCurrentTime;
				strcpy_s(szGuarder, sizeof(szGuarder), pSession->szGuarder);
				if (pSession->nLinkFormat != LOGIN_FORMAT_TCP_SOCKET) {
					pSession->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
				}
				if (strcmp(pEndpoint_, pSession->szEndpoint) != 0) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pSession->szEndpoint);
					strcpy_s(pSession->szEndpoint, sizeof(pSession->szEndpoint), pEndpoint_);
				}
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				if (strlen(szGuarder)) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
							strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
						}
					}
					else {
						nErr = E_INVALIDACCOUNT;
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d}",
				access_service::E_CMD_UNBIND_REPLY, pUnbindInfo_->szSesssion, nErr);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind from %s:%s, session=%s, deviceId=%s, datetime=%s, "
				"retcode=%d\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pUnbindInfo_->szSesssion, pUnbindInfo_->szDeviceId,
				pUnbindInfo_->szDateTime, nErr);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (nErr == E_OK) {
				pthread_mutex_lock(&m_mutex4LinkDataList);
				LinkDataList::iterator iter = m_linkDataList.find(pEndpoint_);
				if (iter != m_linkDataList.end()) {
					auto pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				if (strlen(szLastEndpoint)) {
					LinkDataList::iterator iter2 = m_linkDataList.find(szLastEndpoint);
					if (iter2 != m_linkDataList.end()) {
						auto pLinkData2 = iter2->second;
						if (pLinkData2) {
							if (pLinkData2->pLingeData) {
								delete[] pLinkData2->pLingeData;
								pLinkData2->pLingeData = NULL;
							}
							delete pLinkData2;
							pLinkData2 = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkDataList);
			}
		}
	}
}

void AccessService::handleAppSubmitTask(access_service::AppSubmitTaskInfo taskInfo_, 
	const char * pEndpoint_, unsigned long long ulTime_, const char * pFrom_)
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
	char szTaskId[16] = { 0 };
	char szReply[256] = { 0 };
	bool bUpdateLink = false;
	char szLastLink[32] = { 0 };
	char szFilter[64] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}",
			access_service::E_CMD_TASK_REPLY, E_SERVER_RESOURCE_NOT_READY, taskInfo_.szSession) ;
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive submit task from %s, session=%s,"
			" server not ready\r\n", __FUNCTION__, __LINE__, pEndpoint_, taskInfo_.szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, taskInfo_.szSession);
		if (pLink) {
			pLink->ulActivateTime = ulTime_;
			pLink->nActivated = 1;
			strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			strcpy_s(szFactoryId, sizeof(szFactoryId), pLink->szFactoryId);
			strcpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId);
			if (strlen(pLink->szHandset)) {
				strncpy_s(szHandset, sizeof(szHandset), pLink->szHandset, strlen(pLink->szHandset));
			}
			if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
				strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
				strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
				sprintf_s(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, 
					pLink->szDeviceId);
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
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
				else if (pGuarder->usState == STATE_GUARDER_FREE) {
					nErr = E_UNBINDDEVICE;
				}
				else if (pGuarder->usState == STATE_GUARDER_BIND) {
					if (strlen(szDeviceId) == 0) {
						strcpy_s(szDeviceId, sizeof(szDeviceId), pGuarder->szBindDevice);
					}
					bValidGuarder = true;
				}
				else {
					nErr = E_DEFAULTERROR;
				}
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
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
				if (pDev->deviceBasic.nOnline == 0) {
					nErr = E_UNABLEWORKDEVICE;
				}
				else {
					if (((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
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
				}
				if (strlen(szFactoryId) == 0) {
					strcpy_s(szFactoryId, sizeof(szFactoryId), pDev->deviceBasic.szFactoryId);
				}
				if (strlen(pDev->deviceBasic.szOrgId)) {
					strcpy_s(szOrg, sizeof(szOrg), pDev->deviceBasic.szOrgId);
				}
			}
			else {
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]not find deviceId=%s in the list\r\n", 
					__FUNCTION__, __LINE__, szDeviceId);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
				nErr = E_INVALIDDEVICE;
			}
		}
		else {
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]not find deviceId=%s in the list, list empty\r\n",
				__FUNCTION__, __LINE__, szDeviceId);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
			strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId);
			pTask->nTaskType = (uint8_t)taskInfo_.usTaskType - 1;
			pTask->nTaskState = 0;
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = (uint8_t)taskInfo_.usTaskLimit;
			strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactoryId);
			strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), szDeviceId);
			strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg);
			strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), szGuarder);
			if (strlen(szHandset)) {
				strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), szHandset);
				pTask->nTaskMode = 1;
			}
			else {
				pTask->nTaskMode = 0;
			}
			if (strlen(taskInfo_.szDestination) > 0) {
				strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), taskInfo_.szDestination,
					strlen(taskInfo_.szDestination));
			}
			if (strlen(taskInfo_.szTarget) > 0) {
				strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), taskInfo_.szTarget);
			}
			strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), taskInfo_.szDatetime);
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
				access_service::AppSubscribeInfo * pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					}
					if (pFrom_ && strcmp(pSubInfo->szAccSource, pFrom_) != 0) {
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::string strLink = pEndpoint_;
		LinkDataList::iterator iter = m_linkDataList.find(strLink);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
			}
		}
		if (strlen(szLastLink)) {
			LinkDataList::iterator iter2 = m_linkDataList.find((std::string)szLastLink);
			if (iter2 != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData2 = iter2->second;
				if (pLinkData2) {
					if (pLinkData2->pLingeData) {
						delete [] pLinkData2->pLingeData;
						pLinkData2->pLingeData = NULL;
					}
					delete pLinkData2;
					pLinkData2 = NULL;
				}
				m_linkDataList.erase(iter2);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
		"\"taskId\":\"%s\"}", access_service::E_CMD_TASK_REPLY, nErr, taskInfo_.szSession,
		szTaskId);
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		char szBody[512] = { 0 };
		sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
			"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"taskType\":%u,\"limit\":%u,\"factoryId\":\"%s\",\"deviceId\":\"%s\","
			"\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\"%s\",\"handset\":\"%s\",\"session\":\"%s\"}]}", MSG_SUB_REPORT,
			taskInfo_.uiReqSeq, taskInfo_.szDatetime, SUB_REPORT_TASK, szTaskId, taskInfo_.usTaskType - 1, taskInfo_.usTaskLimit,
			szFactoryId, szDeviceId, szGuarder, taskInfo_.szDestination, taskInfo_.szTarget, szHandset, taskInfo_.szSession);
		sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId);
			pGuarder->usState = STATE_GUARDER_DUTY;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&m_mutex4LinkList);
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, taskInfo_.szSession);
		if (pLink) {
			strcpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), szTaskId);
			if (strlen(pLink->szDeviceId) == 0) {
				strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), szDeviceId);
			}
			if (strlen(pLink->szFactoryId) == 0) {
				strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactoryId);
			}
			if (strlen(pLink->szOrg) == 0) {
				strncpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg, strlen(szOrg));
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		zkSetSession(taskInfo_.szSession);
	}
	//publishNotifyMessage(taskInfo.szSession, pEndpoint, szDeviceId);
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task %d: session=%s, type=%u, limit=%u, target=%s, "
		"destination=%s, datetime=%s, result=%d, taskId=%s, handset=%s, from=%s\r\n", __FUNCTION__, __LINE__, 
		taskInfo_.uiReqSeq, taskInfo_.szSession, taskInfo_.usTaskType - 1, taskInfo_.usTaskLimit, taskInfo_.szTarget, 
		taskInfo_.szDestination, taskInfo_.szDatetime, nErr, szTaskId, szHandset, pEndpoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppSubmitTaskV2(access_service::AppSubmitTaskInfo * pTaskInfo_, const char * pEndpoint_,
	const char * pFrom_, unsigned long long ullTime_)
{
	char szLog[512] = { 0 };
	if (pTaskInfo_ && strlen(pTaskInfo_->szSession)) {
		if (!getLoadSessionFlag()) {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"\"}",
				access_service::E_CMD_TASK_REPLY, pTaskInfo_->szSession, E_SERVER_RESOURCE_NOT_READY);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]submit task from %s:%s, session=%s, datetime=%s, retcode=%d\n", 
				__FUNCTION__, __LINE__, pEndpoint_, pFrom_, pTaskInfo_->szSession, pTaskInfo_->szDatetime, E_SERVER_RESOURCE_NOT_READY);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			char szDeviceId[24] = { 0 };
			bool bValidSession = false;
			char szGuarder[20] = { 0 };
			char szHandset[64] = { 0 };
			char szLastEndpoint[40] = { 0 };
			int nErr = E_OK;
			char szReply[256] = { 0 };
			unsigned long long ullCurrentTime = time(NULL);
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pTaskInfo_->szSession);
			if (pSession) {
				bValidSession = true;
				pSession->nActivated = 1;
				pSession->ulActivateTime = ullCurrentTime;
				strcpy_s(szGuarder, sizeof(szGuarder), pSession->szGuarder);
				if (pSession->nLinkFormat != LOGIN_FORMAT_TCP_SOCKET) {
					pSession->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
				}
				if (strcmp(pSession->szEndpoint, pEndpoint_) != 0) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pSession->szEndpoint);
					strcpy_s(pSession->szEndpoint, sizeof(pSession->szEndpoint), pEndpoint_);
				}
				strcpy_s(szDeviceId, sizeof(szDeviceId), pSession->szDeviceId);
				if (strlen(pSession->szHandset)) {
					strcpy_s(szHandset, sizeof(szHandset), pSession->szHandset);
				}
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				if (strlen(szGuarder)) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (pGuarder->usState == STATE_GUARDER_DUTY) {
							nErr = E_GUARDERINDUTY;
						}
						pGuarder->usLoginFormat = LOGIN_FORMAT_TCP_SOCKET;
						if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
							strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
						}
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}
				else {
					nErr = E_INVALIDACCOUNT;
				}
			}
			if (!strlen(szDeviceId)) {
				nErr = E_UNBINDDEVICE;
			}
			else {
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					if (((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
						|| ((pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE)) {
						nErr = E_DEVICEINDUTY;
					}
				}
				else {
					nErr = E_INVALIDDEVICE;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		
			if (nErr == E_OK) {
				char szTaskId[20] = { 0 };
				unsigned short usDeviceBattery = 0, usDeviceState = 0, usDeviceOnline = 0;
				generateTaskId(szTaskId, sizeof(szTaskId));
				char szFactoryId[4] = { 0 };
				char szOrg[40] = { 0 };

				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					strcpy_s(szFactoryId, sizeof(szFactoryId), pDevice->deviceBasic.szFactoryId);
					strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
					strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), szGuarder);
					changeDeviceStatus(DEV_GUARD, pDevice->deviceBasic.nStatus);
					usDeviceBattery = pDevice->deviceBasic.nBattery;
					usDeviceState = pDevice->deviceBasic.nStatus;
					usDeviceOnline = pDevice->deviceBasic.nOnline;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				
				size_t nTaskSize = sizeof(escort::EscortTask);
				auto pTask = (escort::EscortTask *)zmalloc(nTaskSize);
				memset(pTask, 0, nTaskSize);
				strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId);
				strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), szDeviceId);
				strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactoryId);
				strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg);
				strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), szGuarder);
				if (strlen(pTaskInfo_->szDestination)) {
					strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pTaskInfo_->szDestination);
				}
				strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pTaskInfo_->szTarget);
				pTask->nTaskType = pTaskInfo_->usTaskType;
				pTask->nTaskLimitDistance = pTaskInfo_->usTaskLimit;
				if (strlen(szHandset)) {
					strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), szHandset);
					pTask->nTaskMode = 1;
				}
				formatDatetime(ullCurrentTime, pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime));
				pthread_mutex_lock(&g_mutex4TaskList);
				zhash_update(g_taskList, szTaskId, pTask);
				zhash_freefn(g_taskList, szTaskId, free);
				pthread_mutex_unlock(&g_mutex4TaskList);
				
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->usState = STATE_GUARDER_DUTY;
					strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId);
					strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);

				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\"}",
					access_service::E_CMD_TASK_REPLY, pTaskInfo_->szSession, nErr, szTaskId);
				sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]submit task from %s:%s, session=%s, retcode=%d, taskId=%s, "
					"guarder=%s, deviceId=%s, datetime=%s\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pTaskInfo_->szSession,
					nErr, szTaskId, szGuarder, szDeviceId, pTaskInfo_->szDatetime);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

				pthread_mutex_lock(&m_mutex4LinkList);
				auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pTaskInfo_->szSession);
				if (pSession) {
					strcpy_s(pSession->szTaskId, sizeof(pSession->szTaskId), szTaskId);
					strcpy_s(pSession->szDeviceId, sizeof(pSession->szDeviceId), szDeviceId);
					strcpy_s(pSession->szFactoryId, sizeof(pSession->szFactoryId), szFactoryId);
					strcpy_s(pSession->szOrg, sizeof(pSession->szOrg), szOrg);
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				zkSetSession(pTaskInfo_->szSession);

				char szTopic[80] = { 0 };
				sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactoryId, szDeviceId);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
				auto pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
				pSubInfo->nFormat = LOGIN_FORMAT_TCP_SOCKET;
				strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
				strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
				strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), szGuarder);
				strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), pTaskInfo_->szSession);
				strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
				zhash_update(m_subscribeList, szTopic, pSubInfo);
				zhash_freefn(m_subscribeList, szTopic, free);
				pthread_mutex_unlock(&m_mutex4SubscribeList);

				char szUploadMsg[512] = { 0 };
				sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"taskType\":%hu,\"limit\":%hu,\"factoryId\":\"%s\","
					"\"deviceId\":\"%s\",\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\"%s\",\"handset\":\"%s\","
					"\"session\":\"%s\"}]}", MSG_SUB_REPORT, getNextRequestSequence(), pTaskInfo_->szDatetime, SUB_REPORT_TASK, szTaskId,
					pTaskInfo_->usTaskType, pTaskInfo_->usTaskLimit, szFactoryId, szDeviceId, szGuarder, pTaskInfo_->szDestination, 
					pTaskInfo_->szTarget, szHandset, pTaskInfo_->szSession);
				sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));

				char szMsg[256] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\",\"battery\":%hu,"
					"\"status\":%hu,\"online\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, pTaskInfo_->szSession, 
					access_service::E_NOTIFY_DEVICE_INFO, szDeviceId, usDeviceBattery, usDeviceState, usDeviceOnline, 
					pTask->szTaskStartTime);
				sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), pEndpoint_, access_service::E_ACC_DATA_DISPATCH, pFrom_);
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]send deivce information to %s:%s, deviceId=%s, session=%s, "
					"online=%hu, status=%hu, battery=%hu\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, szDeviceId, pTaskInfo_->szSession,
					usDeviceOnline, usDeviceState, usDeviceBattery);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

				pthread_mutex_lock(&m_mutex4LinkDataList);
				LinkDataList::iterator iter = m_linkDataList.find(pEndpoint_);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				if (strlen(szLastEndpoint)) {
					LinkDataList::iterator iter2 = m_linkDataList.find(szLastEndpoint);
					if (iter2 != m_linkDataList.end()) {
						auto pLastLinkData = iter2->second;
						if (pLastLinkData) {
							if (pLastLinkData->pLingeData) {
								delete[] pLastLinkData->pLingeData;
								pLastLinkData->pLingeData = NULL;
							}
							delete pLastLinkData;
							pLastLinkData = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkDataList);
			}
			else {
				char szReply[256] = { 0 };
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"\"}",
					access_service::E_CMD_TASK_REPLY, pTaskInfo_->szSession, nErr);
				sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task from %s:%s, session=%s, datetime=%s, retcode=%d\n",
					__FUNCTION__, __LINE__, pEndpoint_, pFrom_, pTaskInfo_->szSession, pTaskInfo_->szDatetime, nErr);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
		}
	}
}

void AccessService::handleAppCloseTask(access_service::AppCloseTaskInfo taskInfo_, const char * pEndpoint_, 
	unsigned long long ulTime_, const char * pFrom_)
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
	char szLastLink[32] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
			access_service::E_CMD_TASK_CLOSE_REPLY, E_SERVER_RESOURCE_NOT_READY, taskInfo_.szSession, taskInfo_.szTaskId);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive close task from %s, session=%s, server not ready\r\n",
			__FUNCTION__, __LINE__, pEndpoint_, taskInfo_.szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}

	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, taskInfo_.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ulTime_;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			//strncpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId, strlen(pLink->szDeviceId));
			if (strlen(pLink->szTaskId) > 0 && strcmp(pLink->szTaskId, taskInfo_.szTaskId) == 0) {
				bValidLink = true;
				sprintf_s(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
				if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
					if (strlen(pLink->szEndpoint)) {
						strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
					}
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
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
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, taskInfo_.szTaskId);
			if (pTask) {
				strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				if (strlen(pTask->szGuarder) > 0 && strcmp(pTask->szGuarder, szGuarder) == 0) {
					if (m_nTaskCloseCheckStatus) {
						if (pTask->nTaskFlee == 1) {
							nErr = E_TASK_IN_FLEE_STATUS;
						}
						else {
							bValidTask = true;
							pTask->nTaskState = (taskInfo_.nCloseType == 0) ? 2 : 1;
							strcpy_s(pTask->szTaskStopTime, sizeof(pTask->szTaskStopTime), taskInfo_.szDatetime);
						}
					}
					else {
						bValidTask = true;
						pTask->nTaskState = (taskInfo_.nCloseType == 0) ? 2 : 1;
						strcpy_s(pTask->szTaskStopTime, sizeof(pTask->szTaskStopTime), taskInfo_.szDatetime);
					}
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
	if (strlen(szDeviceId)) {
		if (m_nEnableTaskLooseCheck) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDevice) {
				if (pDevice->deviceBasic.nLooseStatus == 1) {
					nErr = E_TASK_IN_LOOSE_STATUS;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			if (pGuarder->usState != STATE_GUARDER_DEACTIVATE) {
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		if (strlen(szFilter)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
					m_subscribeList, szFilter);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					}
					if (pFrom_ && strcmp(pSubInfo->szAccSource, pFrom_) != 0) {
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
		}
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			std::string strLink = pEndpoint_;
			LinkDataList::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				}
			}
			else {
				auto pLinkData = new access_service::LinkDataInfo();
				memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
				strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
			}
			std::string strLastLink = szLastLink;
			LinkDataList::iterator iter2 = m_linkDataList.find(strLastLink);
			if (iter2 != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData2 = iter2->second;
				if (pLinkData2) {
					if (pLinkData2->pLingeData) {
						delete [] pLinkData2->pLingeData;
						pLinkData2->pLingeData = NULL;
					}
					delete pLinkData2;
					pLinkData2 = NULL;
				}
				m_linkDataList.erase(iter2);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
	}
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		access_service::E_CMD_TASK_CLOSE_REPLY, nErr, taskInfo_.szSession, taskInfo_.szTaskId);
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		char szBody[256] = { 0 };
		sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
			"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"closeType\":%d,\"session\":\"%s\"}]}", MSG_SUB_REPORT, 
			taskInfo_.uiReqSeq, taskInfo_.szDatetime, SUB_REPORT_TASK_CLOSE, taskInfo_.szTaskId, taskInfo_.nCloseType, 
			taskInfo_.szSession);
		sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));

		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, 
			taskInfo_.szSession);
		if (pLink) {
			pLink->szTaskId[0] = '\0';
		}
		pthread_mutex_unlock(&m_mutex4LinkList);

		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDev) {
				pDev->deviceBasic.nStatus = DEV_ONLINE;
				if (pDev->deviceBasic.nLooseStatus == 1) {
					pDev->deviceBasic.nStatus += DEV_LOOSE;
				}
				if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
					pDev->deviceBasic.nStatus += DEV_LOWPOWER;
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
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&g_mutex4TaskList);
		zhash_delete(g_taskList, taskInfo_.szTaskId);
		pthread_mutex_unlock(&g_mutex4TaskList);

		zkSetSession(taskInfo_.szSession);
	}
	//publishNotifyMessage(taskInfo.szSession, pEndpoint, szDeviceId);
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit close task, session=%s, taskId=%s, datetime=%s, "
		"deviceId=%s, guarder=%s, result=%d, from=%s\r\n", __FUNCTION__, __LINE__, taskInfo_.szSession, taskInfo_.szTaskId,
		taskInfo_.szDatetime, szDeviceId, szGuarder, nErr, pEndpoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppCloseTaskV2(access_service::AppCloseTaskInfo * pTaskInfo_, const char * pEndpoint_,
	const char * pFrom_, unsigned long long ullTime_)
{
	char szLog[512] = { 0 };
	char szReply[256] = { 0 };
	if (pTaskInfo_ && strlen(pTaskInfo_->szSession) && strlen(pTaskInfo_->szTaskId)) {
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\"}",
				access_service::E_CMD_TASK_CLOSE_REPLY, pTaskInfo_->szSession, E_SERVER_RESOURCE_NOT_READY, pTaskInfo_->szTaskId);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task from %s:%s, taskId=%s, session=%s, datetime=%s, "
				"service not ready\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pTaskInfo_->szTaskId, pTaskInfo_->szSession,
				pTaskInfo_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			int nErr = E_OK;
			bool bValidSession = false;
			char szGuarder[20] = { 0 };
			char szDeviceId[20] = { 0 };
			char szFactoryId[4] = { 0 };
			char szOrg[40] = { 0 };
			char szLastEndpoint[40] = { 0 };
			unsigned long long ullCurrentTime = time(NULL);
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pTaskInfo_->szSession);
			if (pSession) {
				bValidSession = true;
				pSession->nActivated = 1;
				pSession->ulActivateTime = ullCurrentTime;
				pSession->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
				if (strcmp(pSession->szEndpoint, pEndpoint_) != 0) {
					strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pSession->szEndpoint);
					strcpy_s(pSession->szEndpoint, sizeof(pSession->szEndpoint), pEndpoint_);
				}
				strcpy_s(szGuarder, sizeof(szGuarder), pSession->szGuarder);
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4TaskList);
				auto pTask = (escort::EscortTask *)zhash_lookup(g_taskList, pTaskInfo_->szTaskId);
				if (pTask) {
					strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
					strcpy_s(szFactoryId, sizeof(szFactoryId), pTask->szFactoryId);
					strcpy_s(szOrg, sizeof(szOrg), pTask->szOrg);
					if (pTask->nTaskFlee) {
						nErr = E_TASK_IN_FLEE_STATUS;
					}
				}
				else {
					nErr = E_INVALIDTASK;
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\"}",
				access_service::E_CMD_TASK_CLOSE_REPLY, pTaskInfo_->szSession, nErr, pTaskInfo_->szTaskId);
			sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]close task from %s:%s, taskId=%s, session=%s, retcode=%d, "
				"datetime=%s\n", __FUNCTION__, __LINE__, pEndpoint_, pFrom_, pTaskInfo_->szTaskId, pTaskInfo_->szSession,
				nErr, pTaskInfo_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

			if (nErr == E_OK) {
				pthread_mutex_lock(&g_mutex4TaskList);
				zhash_delete(g_taskList, pTaskInfo_->szTaskId);
				pthread_mutex_unlock(&g_mutex4TaskList);

				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->szBindDevice[0] = '\0';
					pGuarder->szTaskId[0] = '\0';
					pGuarder->usState = STATE_GUARDER_FREE;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);

				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					pDevice->deviceBasic.nStatus = DEV_ONLINE;
					if (pDevice->deviceBasic.nBattery < 20) {
						pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
					}
					if (pDevice->deviceBasic.nLooseStatus == 1) {
						pDevice->deviceBasic.nStatus += DEV_LOOSE;
					}
					pDevice->szBindGuard[0] = '\0';
				}
				pthread_mutex_unlock(&g_mutex4DevList);

				char szUploadMsg[512] = { 0 };
				sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"closeType\":%d,\"session\":\"%s\"}]}",
					MSG_SUB_REPORT, getNextRequestSequence(), pTaskInfo_->szDatetime, SUB_REPORT_TASK_CLOSE, pTaskInfo_->szTaskId, 
					pTaskInfo_->nCloseType, pTaskInfo_->szSession);
				sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));

				char szTopic[80] = { 0 };
				sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactoryId, szDeviceId);
				pthread_mutex_lock(&m_mutex4SubscribeList);
				zhash_delete(m_subscribeList, szTopic);
				pthread_mutex_unlock(&m_mutex4SubscribeList);

				pthread_mutex_lock(&m_mutex4LinkList);
				auto pSession = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pTaskInfo_->szSession);
				if (pSession) {
					pSession->szDeviceId[0] = '\0';
					pSession->szFactoryId[0] = '\0';
					pSession->szOrg[0] = '\0';
					pSession->szTaskId[0] = '\0';
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				zkSetSession(pTaskInfo_->szSession);
			
				pthread_mutex_lock(&m_mutex4LinkDataList);
				LinkDataList::iterator iter = m_linkDataList.find(pEndpoint_);
				if (iter != m_linkDataList.end()) {
					auto pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				if (strlen(szLastEndpoint)) {
					LinkDataList::iterator iter2 = m_linkDataList.find(szLastEndpoint);
					if (iter2 != m_linkDataList.end()) {
						auto pLastLinkData = iter2->second;
						if (pLastLinkData) {
							if (pLastLinkData->pLingeData) {
								delete[] pLastLinkData->pLingeData;
								pLastLinkData->pLingeData = NULL;
							}
							delete pLastLinkData;
							pLastLinkData = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkDataList);
			}
		}
	}
}

void AccessService::handleAppPosition(access_service::AppPositionInfo posInfo_, const char * pEndpoint_, 
	unsigned long long ullTime_, const char * pFrom_)
{
	char szLog[256] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szDevice[16] = { 0 };
	char szFilter[64] = { 0 };
	char szGuarder[20] = { 0 };
	bool bUpdateLink = false;
	unsigned short usBattery = 0;
	unsigned short usStatus = 0;
	bool bNotifyOffline = false;
	bool bNotifyStatus = false;
	char szLastLink[32] = { 0 };
	char szDatetime[20] = { 0 };
	int nRetCode = E_OK;

	if (!getLoadSessionFlag()) {
		char szReply[256] = { 0 };
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\",\"datetime\":\"%s\"}",
			access_service::E_CMD_POSITION_REPLY, posInfo_.szSession, E_SERVER_RESOURCE_NOT_READY, posInfo_.szTaskId, 
			posInfo_.szDatetime);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access-service]%s[%d]handle app position session=%s,"
			" taskId=%s, datetime=%s, server resource not ready\n", __FUNCTION__, __LINE__, 
			posInfo_.szSession, posInfo_.szTaskId, posInfo_.szDatetime);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}

	formatDatetime(ullTime_, szDatetime, sizeof(szDatetime));
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, posInfo_.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ullTime_;
			if (strlen(pLink->szTaskId) > 0 && strcmp(pLink->szTaskId, posInfo_.szTaskId) == 0) {
				bValidLink = true;
				sprintf_s(szFilter, sizeof(szFilter), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				strcpy_s(szDevice, sizeof(szDevice), pLink->szDeviceId);
				if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
					strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
					bUpdateLink = true;
				}
			}
			else {
				nRetCode = E_INVALIDTASK;
			}
			if (strlen(pLink->szDeviceId)) {
				if (pLink->nNotifyOffline == 1) {
					bNotifyOffline = true;
				}
				else {
					if (pLink->nNotifyStatus == 1) {
						bNotifyStatus = true;
					}
				}
			}
		}
		else {
			nRetCode = E_INVALIDSESSION;
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	char szReply[256] = { 0 };
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\",\"datetime\":\"%s\"}",
		access_service::E_CMD_POSITION_REPLY, posInfo_.szSession, nRetCode, posInfo_.szTaskId, posInfo_.szDatetime);
	int nRet = sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);

	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]app position reply to endpoint=%s, session=%s, "
		"taskId=%s, datetime=%s, guarder=%s, deviceId=%s, ret=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_,
		posInfo_.szSession, posInfo_.szTaskId, posInfo_.szDatetime, szGuarder, szDevice, nRet);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDevice);
		if (pDev) {
			if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
				|| (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
				pDev->guardPosition.dLatitude = posInfo_.dLat;
				pDev->guardPosition.dLngitude = posInfo_.dLng;
			}
			usBattery = pDev->deviceBasic.nBattery;
			usStatus = pDev->deviceBasic.nStatus;
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		char szBody[512] = { 0 };
		sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%lu,\"datetime\":\"%s\","
			"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"lat\":%.06f,\"lng\":%.06f,\"coordinate\":%d,\"session\":\"%s\"}]}",
			MSG_SUB_REPORT, posInfo_.uiReqSeq, posInfo_.szDatetime, SUB_REPORT_POSITION, posInfo_.szTaskId, posInfo_.dLat, 
			posInfo_.dLng, posInfo_.nCoordinate, posInfo_.szSession);
		sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
				strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		pthread_mutex_lock(&m_mutex4SubscribeList);
		auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szFilter);
		if (pSubInfo) {
			if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
				strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
			}
			if (pFrom_ && strcmp(pSubInfo->szAccSource, pFrom_) != 0) {
				strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
			}
		}
		else {
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]not find subscriber topic=%s\r\n",
				__FUNCTION__, __LINE__, szFilter);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
		}
		pthread_mutex_unlock(&m_mutex4SubscribeList);
		if (bUpdateLink) {
			pthread_mutex_lock(&m_mutex4LinkDataList);
			if (!m_linkDataList.empty()) {
				std::string strLink = pEndpoint_;
				LinkDataList::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				else {
					auto pLinkData = new access_service::LinkDataInfo();
					memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
					strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
				}

				if (strlen(szLastLink)) {
					std::string strLastLink = szLastLink;
					LinkDataList::iterator iter2 = m_linkDataList.find(strLastLink);
					if (iter2 != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData2 = iter2->second;
						if (pLinkData2) {
							if (pLinkData2->pLingeData) {
								delete [] pLinkData2->pLingeData;
								pLinkData2->pLingeData = NULL;
							}
							delete pLinkData2;
							pLinkData2 = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
		}
		if (bNotifyOffline) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, posInfo_.szSession,
				access_service::E_NOTIFY_DEVICE_OFFLINE, szDevice, szDatetime);
			sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), pEndpoint_, access_service::E_ACC_DATA_DISPATCH, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send offline endpoint=%s, session=%s, "
				"deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pEndpoint_, posInfo_.szSession, 
				szDevice, szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		if (bNotifyStatus) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%u,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				posInfo_.szSession, access_service::E_NOTIFY_DEVICE_INFO, szDevice, usBattery, usStatus, szDatetime);
			sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), pEndpoint_, access_service::E_ACC_DATA_DISPATCH, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send status to endpoint=%s, session=%s, "
				"deviceId=%s, battery=%u, status=%u, datetime=%s\r\n", __FUNCTION__, __LINE__, pEndpoint_, 
				posInfo_.szSession, szDevice, usBattery, usStatus, szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]report position session=%s, taskId=%s, lat=%.06f,"
		" lng=%0.6f, coordinate=%d, datetime=%s, from=%s\r\n", __FUNCTION__, __LINE__, posInfo_.szSession, 
		posInfo_.szTaskId, posInfo_.dLat, posInfo_.dLng, posInfo_.nCoordinate, posInfo_.szDatetime, pEndpoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppFlee(access_service::AppSubmitFleeInfo fleeInfo_, const char * pEndpoint_, 
	unsigned long long ullTime_, const char * pFrom_)
{
	int nErr = E_OK;
	char szLog[512] = { 0 };
	char szFactoryId[4] = { 0 };
	char szDeviceId[16] = { 0 };
	bool bValidLink = false;
	bool bValidTask = false;
	char szReply[256] = { 0 };
	bool bUpdateLink = false;
	char szLastLink[32] = { 0 };
	char szGuarder[20] = { 0 };
	char szFilter[64] = { 0 };
	bool bNotify = true;
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
			fleeInfo_.nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
			E_SERVER_RESOURCE_NOT_READY, fleeInfo_.szSession, fleeInfo_.szTaskId);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive flee from %s, mode=%d, session=%s, "
			"server not ready\r\n", __FUNCTION__, __LINE__, pEndpoint_, fleeInfo_.nMode, fleeInfo_.szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, fleeInfo_.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ullTime_;
			bValidLink = true;
			strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			pLink->nLinkFormat = LOGIN_FORMAT_TCP_SOCKET;
			if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
				strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
				strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
				bUpdateLink = true;
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (bValidLink) {
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList)) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, fleeInfo_.szTaskId);
			if (pTask) {
				strcpy_s(szFactoryId, sizeof(szFactoryId), pTask->szFactoryId);
				strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				sprintf_s(szFilter, sizeof(szFilter), "%s_%s_%s", pTask->szOrg, pTask->szFactoryId, pTask->szDeviceId);
				if (fleeInfo_.nMode == 0) {
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
				if (fleeInfo_.nMode == 0) { //flee
					if (pDev->deviceBasic.nOnline == 0) {
						nErr = E_UNABLEWORKDEVICE;
					}
					else {
						if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]change status guard->flee, deviceId=%s, session=%s\r\n",
								__FUNCTION__, __LINE__, szDeviceId, fleeInfo_.szSession);
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							//do nothing
							if (!m_nTaskFleeReplicatedReport) {
								bNotify = false;
							}
						}
						pDev->deviceBasic.nStatus = DEV_FLEE;
						if (pDev->deviceBasic.nLooseStatus) {
							pDev->deviceBasic.nStatus += DEV_LOOSE;
						}
						if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
				}
				else { //flee revoke
					if (pDev->deviceBasic.nOnline == 0) {
						nErr = E_UNABLEWORKDEVICE;
					}
					else {
						if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
							//do nothing
							if (!m_nTaskFleeReplicatedReport) {
								bNotify = false;
							}
						}
						else if ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]change status flee->guard, deviceId=%s, session=%s\r\n",
								__FUNCTION__, __LINE__, szDeviceId, fleeInfo_.szSession);
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						pDev->deviceBasic.nStatus = DEV_GUARD;
						if (pDev->deviceBasic.nLooseStatus) {
							pDev->deviceBasic.nStatus += DEV_LOOSE;
						}
						if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
				}
				pGuarder->ullActiveTime = ullTime_;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			if (bUpdateLink) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szFilter);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]update subscriber for %s, endpoint=%s\r\n", 
							__FUNCTION__, __LINE__, szFilter, pEndpoint_);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				pthread_mutex_lock(&m_mutex4LinkDataList);
				std::string strLink = pEndpoint_;
				LinkDataList::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				else {
					auto pLinkData = new access_service::LinkDataInfo();
					memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
					strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
				}
				if (strlen(szLastLink)) {
					std::string strLastLink = szLastLink;
					LinkDataList::iterator iter2 = m_linkDataList.find(strLastLink);
					if (iter2 != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData2 = iter2->second;
						if (pLinkData2) {
							if (pLinkData2->pLingeData) {
								delete [] pLinkData2->pLingeData;
								pLinkData2->pLingeData = NULL;
							}
							delete pLinkData2;
							pLinkData2 = NULL;
						}
						m_linkDataList.erase(iter);
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
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
		fleeInfo_.nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
		nErr, fleeInfo_.szSession, fleeInfo_.szTaskId);
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		if (bNotify) {
			char szBody[512] = { 0 };
			sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"session\":\"%s\"}]}",
				MSG_SUB_REPORT, fleeInfo_.uiReqSeq, fleeInfo_.szDatetime, 
				fleeInfo_.nMode == 0 ? SUB_REPORT_DEVICE_FLEE : SUB_REPORT_DEVICE_FLEE_REVOKE, fleeInfo_.szTaskId, fleeInfo_.szSession);
			sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
		}
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]report %s %d, session=%s, taskId=%s, deviceId=%s, "
		"datetime=%s, result=%d, from=%s\r\n", __FUNCTION__, __LINE__, fleeInfo_.nMode == 0 ? "flee" : "flee revoke",
		fleeInfo_.uiReqSeq, fleeInfo_.szSession, fleeInfo_.szTaskId, szDeviceId, fleeInfo_.szDatetime, nErr, pEndpoint_);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppKeepAlive(access_service::AppKeepAlive * pKeepAlive_, const char * pEndpoint_, 
	unsigned long long ullTime_, const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d}",
			access_service::E_CMD_KEEPALIVE_REPLY, pKeepAlive_->szSession, pKeepAlive_->uiSeq, E_SERVER_RESOURCE_NOT_READY);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive alive from %s, session=%s, "
			"server not ready wait to load session\r\n", __FUNCTION__, __LINE__, pEndpoint_, 
			pKeepAlive_->szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pKeepAlive_->szSession);
		if (!pLink) {
			nErr = E_INVALIDSESSION;
		}
	}
	else {
		nErr = E_INVALIDSESSION;
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	sprintf_s(szReply, 256, "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d}",
		access_service::E_CMD_KEEPALIVE_REPLY, pKeepAlive_->szSession, pKeepAlive_->uiSeq, nErr);
	uint32_t nReplyLen = (uint32_t)strlen(szReply);
	int nRetVal = sendDataToEndpoint_v2(szReply, nReplyLen, pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		bool bUpdateLink = false;
		char szGuarder[20] = { 0 };
		bool bHaveSubscribe = false;
		char szSubTopic[64] = { 0 };
		char szDeviceId[20] = { 0 };
		bool bNotifyOffline = false;
		bool bNotifyStatus = false;
		char szLastLink[32] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pKeepAlive_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = ullTime_;
				if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
					bUpdateLink = true;
					strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
					strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
					if (strlen(pLink->szDeviceId) && strlen(pLink->szFactoryId) && strlen(pLink->szOrg)) {
						sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId,
							pLink->szDeviceId);
						strcpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId);
						bHaveSubscribe = true;
					}
				}
				if (pLink->nNotifyOffline == 1) {
					bNotifyOffline = true;
				}
				else {
					if (pLink->nNotifyStatus == 1) {
						bNotifyStatus = true;
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bUpdateLink && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4LinkDataList);
			std::string strLink = pEndpoint_;
			LinkDataList::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				}
			}
			else {
				auto pLinkData = new access_service::LinkDataInfo();
				memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
				strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
			}
			if (strlen(szLastLink)) {
				std::string strLastLink = szLastLink;
				LinkDataList::iterator iter2 = m_linkDataList.find(strLastLink);
				if (iter2 != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData2 = iter2->second;
					if (pLinkData2) {
						if (pLinkData2->pLingeData) {
							delete[] pLinkData2->pLingeData;
							pLinkData2->pLingeData = nullptr;
						}
						delete pLinkData2;
						pLinkData2 = nullptr;
					}
					m_linkDataList.erase(iter2);
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
		}
		if (bHaveSubscribe) {
			if (strlen(szSubTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
					m_subscribeList, szSubTopic);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					}
					if (pFrom_ && strcmp(pSubInfo->szAccSource, pFrom_) != 0) {
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
			}
		}
		unsigned short usBattery = 0;
		unsigned short usStatus = 0;
		int nCoordinate = 0;
		if (strlen(szDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDev) {
					usBattery = pDev->deviceBasic.nBattery;
					usStatus = pDev->deviceBasic.nStatus;
					nCoordinate = pDev->devicePosition.nCoordinate;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		char szDatetime[20] = { 0 };
		formatDatetime(ullTime_, szDatetime, sizeof(szDatetime));
		int nRetVal = 0;
		if (bNotifyOffline) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,"
				"\"deviceId\":\"%s\",\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				pKeepAlive_->szSession, access_service::E_NOTIFY_DEVICE_OFFLINE, szDeviceId,
				szDatetime);
			nRetVal = sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), pEndpoint_, 
				access_service::E_ACC_DATA_DISPATCH, pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send offline, endpoint=%s, session=%s,"
				" deviceId=%s, datetime=%s, ret=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_, 
				pKeepAlive_->szSession, szDeviceId, szDatetime, nRetVal);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		if (bNotifyStatus) {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\battery\":%u,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				pKeepAlive_->szSession, access_service::E_NOTIFY_DEVICE_INFO, szDeviceId, usBattery,
				usStatus, szDatetime);
			nRetVal = sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), pEndpoint_, access_service::E_ACC_DATA_DISPATCH,
				pFrom_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send status, endpoint=%s, session=%s, "
				"deviceId=%s, battery=%u, status=%u, datetime=%s, ret=%d\r\n", __FUNCTION__, __LINE__,
				pEndpoint_, pKeepAlive_->szSession, szDeviceId, usBattery, usStatus, szDatetime, nRetVal);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]report keep alive, from=%s, session=%s, "
		" seq=%u, datetime=%s, retcode=%d, reply code=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_, 
		pKeepAlive_->szSession, pKeepAlive_->uiSeq, pKeepAlive_->szDatetime, nErr, nRetVal);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppModifyAccountPassword(access_service::AppModifyPassword modifyPasswd_, const char * pEndpoint_,
	unsigned long long ullTime_, const char * pFrom_)
{
	char szLog[512] = { 0 };
	char szReply[256] = { 0 };
	int nErr = E_OK;
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,"
			"\"datetime\":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, modifyPasswd_.szSession,
			E_SERVER_RESOURCE_NOT_READY, modifyPasswd_.szDatetime);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive modify password from %s, "
			"session=%s, server not ready\r\n", __FUNCTION__, __LINE__, pEndpoint_, modifyPasswd_.szSession);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	bool bValidLink = false;
	bool bUpdateLink = false;
	char szLastEndpoint[32] = { 0 };
	char szGuarder[20] = { 0 };
	char szSubTopic[64] = { 0 };
	bool bHaveSubscribe = false;
	char szDeviceId[20] = { 0 };
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, modifyPasswd_.szSession);
		if (pLink) {
			if (pLink->nActivated == 0) {
				pLink->nActivated = 1;
			}
			pLink->ulActivateTime = ullTime_;
			strncpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder, strlen(pLink->szGuarder));
			if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
				strcpy_s(szLastEndpoint, sizeof(szLastEndpoint), pLink->szEndpoint);
				strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
				bUpdateLink = true;
				if (strlen(pLink->szDeviceId) && strlen(pLink->szFactoryId) && strlen(pLink->szOrg)) {
					bHaveSubscribe = true;
					sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, 
						pLink->szDeviceId);
					strcpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId);
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
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s,"
						" datetime=%s, current password is incorrect, can not execute modify, input=%s\r\n", 
						__FUNCTION__, __LINE__, pEndpoint_, szGuarder, modifyPasswd_.szDatetime, 
						modifyPasswd_.szCurrPassword);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
				}
				if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
					strncpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_, strlen(pEndpoint_));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		else {
			nErr = E_INVALIDPASSWD;
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s, "
				"datetime=%s, new password is same with the current password, nothing to modify\r\n",
				__FUNCTION__, __LINE__, pEndpoint_, szGuarder, modifyPasswd_.szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
		}
	}
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,"
		"\"datetime\":\"%s\"}", access_service::E_CMD_MODIFY_PASSWD_REPLY, modifyPasswd_.szSession, 
		nErr, modifyPasswd_.szDatetime);
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	if (nErr == E_OK) {
		char szMsgBody[512] = { 0 };
		sprintf_s(szMsgBody, sizeof(szMsgBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
			"\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"currPasswd\":\"%s\",\"newPasswd\":\"%s\",\"session\":\"%s\"}]}",
			MSG_SUB_REPORT, modifyPasswd_.uiSeq, modifyPasswd_.szDatetime, SUB_REPORT_MODIFY_USER_PASSWD, szGuarder, 
			modifyPasswd_.szCurrPassword, modifyPasswd_.szNewPassword, modifyPasswd_.szSession);
		sendDataViaInteractor_v2(szMsgBody, (uint32_t)strlen(szMsgBody));
	}
	if (bUpdateLink) {
		pthread_mutex_lock(&m_mutex4LinkDataList);
		if (!m_linkDataList.empty()) {
			std::string strLink = pEndpoint_;
			LinkDataList::iterator iter = m_linkDataList.find(strLink);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strncpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder, strlen(szGuarder));
				}
			}
			else {
				auto pLinkData = new access_service::LinkDataInfo();
				memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
				strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
				strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
			}
			if (strlen(szLastEndpoint)) {
				LinkDataList::iterator iter2 = m_linkDataList.find((std::string)szLastEndpoint);
				if (iter2 != m_linkDataList.end()) {
					auto pLinkData2 = iter2->second;
					if (pLinkData2) {
						if (pLinkData2->pLingeData) {
							delete[] pLinkData2->pLingeData;
							pLinkData2->pLingeData = NULL;
						}
						delete pLinkData2;
						pLinkData2 = NULL;
					}
					m_linkDataList.erase(iter2);
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkDataList);
		if (bHaveSubscribe && strlen(szSubTopic)) {
			pthread_mutex_lock(&m_mutex4SubscribeList);
			auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
				m_subscribeList, szSubTopic);
			if (pSubInfo) {
				if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
					strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
				}
				if (pFrom_ && strcmp(pSubInfo->szAccSource, pFrom_) != 0) {
					strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			//publishNotifyMessage(modifyPasswd_.szSession, pEndpoint_, szDeviceId);
		}
	}
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]report modify password from %s, session=%s,"
		" seq=%u, datetime=%s, result=%d\r\n", __FUNCTION__, __LINE__, pEndpoint_,
		modifyPasswd_.szSession, modifyPasswd_.uiSeq, modifyPasswd_.szDatetime, nErr);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppQueryTask(access_service::AppQueryTask queryTask_, const char * pEndpoint_, 
	unsigned long long ullTime_, const char * pFrom_)
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
	char szOldEndpoint[32] = { 0 };
	unsigned short usDeviceStatus = 0;
	unsigned short usDeviceBattery = 0;
	unsigned short usDeviceOnline = 0;
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, queryTask_.szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = ullTime_;
			if (strlen(pLink->szDeviceId) && strlen(pLink->szTaskId)) {
				if (strcmp(pLink->szTaskId, queryTask_.szTaskId) == 0) {
					strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
					if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(szOldEndpoint, sizeof(szOldEndpoint), pLink->szEndpoint);
						strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
						bUpdateLink = true;
					}
					bValidLink = true;
					bHaveSubscribe = true;
					sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
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
						strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&m_mutex4LinkDataList);
			if (!m_linkDataList.empty()) {
				std::string strLink = pEndpoint_;
				LinkDataList::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData) {
						if (strlen(szGuarder)) {
							strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
						}
					}
				}
				else {
					auto pLinkData = new access_service::LinkDataInfo();
					memset(pLinkData, 0, sizeof(access_service::LinkDataInfo));
					strcpy_s(pLinkData->szLinkId, sizeof(pLinkData->szLinkId), pEndpoint_);
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					m_linkDataList.emplace((std::string)pEndpoint_, pLinkData);
				}
				if (strlen(szOldEndpoint)) {
					std::string strOldLink = szOldEndpoint;
					LinkDataList::iterator iter2 = m_linkDataList.find(strOldLink);
					if (iter2 != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData2 = iter2->second;
						if (pLinkData2) {
							if (pLinkData2->pLingeData) {
								delete [] pLinkData2->pLingeData;
								pLinkData2->pLingeData = NULL;
							}
							delete pLinkData2;
							pLinkData2 = NULL;
						}
						m_linkDataList.erase(iter2);
					}
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
			if (bHaveSubscribe && strlen(szSubTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
					m_subscribeList, szSubTopic);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					}
					if (pFrom_ && strcmp(pFrom_, pSubInfo->szAccSource) != 0) {
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
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
				usDeviceOnline = pDevice->deviceBasic.nOnline;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
	}
	char szReply[512] = { 0 };
	if (nErr == E_OK) {
		if (bValidTask && pCurrTask) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\","
				"\"taskInfo\":[{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,\"limit\":%d,\"destination\":\"%s\","
				"\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%u,\"deviceState\":%u,\"online\":%hu,\"handset\":\"%s\"}]}", 
				access_service::E_CMD_QUERY_TASK_REPLY, nErr, queryTask_.szSession, queryTask_.szDatetime,
				pCurrTask->szTaskId, pCurrTask->szDeviceId, pCurrTask->nTaskType + 1, pCurrTask->nTaskLimitDistance,
				pCurrTask->szDestination, pCurrTask->szTarget, pCurrTask->szTaskStartTime, usDeviceBattery, usDeviceStatus,
				usDeviceOnline, pCurrTask->szHandset);
		}
		else {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\","
				"\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, nErr, queryTask_.szSession, queryTask_.szDatetime);
		}
	}
	else {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\","
			"\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, nErr, queryTask_.szSession, queryTask_.szDatetime);
	}
	sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, session=%s, taskId=%s, "
		"datetime=%s, result=%d, deviceState=%u, online=%hu\r\n", __FUNCTION__, __LINE__, pEndpoint_,
		queryTask_.szSession, queryTask_.szTaskId, queryTask_.szDatetime, nErr, usDeviceStatus, usDeviceOnline);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	if (pCurrTask) {
		free(pCurrTask);
		pCurrTask = NULL;
	}
}

void AccessService::handleAppDeviceCommand(access_service::AppDeviceCommandInfo cmdInfo_, const char * pEndpoint_, 
	const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%u,\"retcode\":%d,"
			"\"datetime\":\"%s\",\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY, 
			cmdInfo_.szSession, cmdInfo_.nSeq, E_SERVER_RESOURCE_NOT_READY, cmdInfo_.szDatetime, 
			cmdInfo_.szDeviceId);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive device command from %s, session=%s,"
			" seq=%u, datetime=%s, deviceId=%s, server not ready, wait to load session\r\n", __FUNCTION__, 
			__LINE__, pEndpoint_, cmdInfo_.szSession, cmdInfo_.nSeq, cmdInfo_.szDatetime, cmdInfo_.szDeviceId);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	if (strlen(cmdInfo_.szSession) > 0 && strlen(cmdInfo_.szDeviceId) && cmdInfo_.nParam1 > 0) {
		bool bValidLink = false;
		bool bValidDevice = false;
		char szLastLink[32] = { 0 };
		char szSubTopic[64] = { 0 };
		char szGuarder[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, cmdInfo_.szSession);
			if (pLink) {
				if (strcmp(pLink->szDeviceId, cmdInfo_.szDeviceId) != 0) {
					nErr = E_DEVICENOTMATCH;
				}
				else {
					pLink->nActivated = 1;
					pLink->ulActivateTime = time(nullptr);
					bValidLink = true;
					if (strcmp(pEndpoint_, pLink->szEndpoint) != 0) {
						strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
						strcpy_s(szLastLink, sizeof(szLastLink), pLink->szEndpoint);
						strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
					}
				}
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bValidLink) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, cmdInfo_.szDeviceId);
			if (pDevice) {
				sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pDevice->deviceBasic.szOrgId,
					pDevice->deviceBasic.szFactoryId, pDevice->deviceBasic.szDeviceId);
				if (pDevice->deviceBasic.nOnline == 0) {
					nErr = E_UNABLEWORKDEVICE;
				}
				else {
					if (strlen(pDevice->szBindGuard)) {
						bValidDevice = true;
					}
					else {
						nErr = E_INVALIDDEVICE;
					}
				}
			}
			else {
				nErr = E_INVALIDDEVICE;
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
			else if (cmdInfo_.nParam1 == access_service::E_DEV_CMD_QUERY_POSITION) {
				nUploadCmd = PROXY_QUERY_DEVICE_POSITION;
			}
			char szUploadMsg[512] = { 0 };
			sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
				"\"datetime\":\"%s\",\"request\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"param\":%d,"
				"\"session\":\"%s\"}]}", MSG_SUB_REQUEST, getNextRequestSequence(), cmdInfo_.szDatetime, nUploadCmd, 
				cmdInfo_.szFactoryId, cmdInfo_.szDeviceId, cmdInfo_.nParam2, cmdInfo_.szSession);
			sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));
		}
		if (strlen(szLastLink)) {
			pthread_mutex_lock(&m_mutex4LinkDataList);
			std::string strEndpoint = pEndpoint_;
			LinkDataList::iterator iter = m_linkDataList.find(strEndpoint);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
				}
			}
			std::string strLastLink = szLastLink;
			LinkDataList::iterator iter2 = m_linkDataList.find(strLastLink);
			if (iter2 != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData2 = iter2->second;
				if (pLinkData2) {
					if (pLinkData2->pLingeData) {
						delete [] pLinkData2->pLingeData;
						pLinkData2->pLingeData = NULL;
					}
					delete pLinkData2;
					pLinkData2 = NULL;
				}
				m_linkDataList.erase(iter2);
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
		}
	}
	sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d,"
		"\"datetime\":\"%s\",\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY,
		cmdInfo_.szSession, cmdInfo_.nSeq, nErr, cmdInfo_.szDatetime, cmdInfo_.szDeviceId);
	int nRetVal = sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, 
		access_service::E_ACC_DATA_TRANSFER, pFrom_);
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, deviceId=%s, "
		"session=%s, param1=%d, param2=%d, seq=%d, datetime=%s, retcode=%d, reply code=%d\r\n", 
		__FUNCTION__, __LINE__, pEndpoint_, cmdInfo_.szDeviceId, cmdInfo_.szSession, cmdInfo_.nParam1,
		cmdInfo_.nParam2, cmdInfo_.nSeq, cmdInfo_.szDatetime, nErr, nRetVal);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppQueryPerson(access_service::AppQueryPerson qryPerson_, const char * pEndpoint_, 
	const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%d,\"datetime\":\"%s\","
			"\"count\":0,\"personList\":[]}", access_service::E_CMD_QUERY_PERSON_REPLY, qryPerson_.szSession,
			qryPerson_.uiQeurySeq, qryPerson_.szQryDatetime);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive query person from %s, session=%s, "
			"qryPid=%s, qryMode=%d, seq=%d, datetime=%s, server not ready, wait to load sessionn\r\n", 
			__FUNCTION__, __LINE__, pEndpoint_, qryPerson_.szSession, qryPerson_.szQryPersonId, 
			qryPerson_.nQryMode, qryPerson_.uiQeurySeq, qryPerson_.szQryDatetime);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		return;
	}
	char szDatetime[20] = { 0 };
	formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
	
	unsigned int uiUploadSeq = getNextRequestSequence();
	char szDevMsg[256] = { 0 };
	sprintf_s(szDevMsg, sizeof(szDevMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,\"datetime\":\"%s\","
		"\"getType\":%d,\"param1\":\"%s\",\"param2\":\"\",\"param3\":0,\"param4\":%d}", MSG_SUB_GETINFO, uiUploadSeq, szDatetime,
		BUFFER_PERSON, qryPerson_.szQryPersonId, qryPerson_.nQryMode);
	sendDataViaInteractor_v2(szDevMsg, (uint32_t)strlen(szDevMsg));

	access_service::QueryPersonEvent * pEvent = new access_service::QueryPersonEvent();
	memset(pEvent, 0, sizeof(access_service::QueryPersonEvent));
	strcpy_s(pEvent->szEndpoint, sizeof(pEvent->szEndpoint), pEndpoint_);
	pEvent->uiQrySeq = uiUploadSeq;
	strcpy_s(pEvent->szSession, sizeof(pEvent->szSession), qryPerson_.szSession);
	strcpy_s(pEvent->szLink, sizeof(pEvent->szLink), pFrom_);
	pEvent->usFormat = LOGIN_FORMAT_TCP_SOCKET;
	std::lock_guard<std::mutex> lock(m_mutex4QryPersonEventList);
	m_qryPersonEventList.emplace(uiUploadSeq, pEvent);
}

void AccessService::handleAppQueryTaskList(access_service::AppQueryTaskList * pQryTaskList_, const char * pEndpoint_, 
	const char * pFrom_)
{
	char szLog[512] = { 0 };
	if (!getLoadSessionFlag()) {
		char szReply[256] = { 0 };
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"orgId\":\"%s\",\"req\":%u,\"datetime\":\"%s\","
			"\"count\":0,\"list\":[]}", access_service::E_CMD_QUERY_TASK_LIST_REPLY, pQryTaskList_->szOrgId,
			pQryTaskList_->uiQrySeq, pQryTaskList_->szDatetime);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive query task list from %s, orgId=%s, "
			"req=%u, datetime=%s, reply empty\r\n", __FUNCTION__, __LINE__, pEndpoint_, pQryTaskList_->szOrgId,
			pQryTaskList_->uiQrySeq, pQryTaskList_->szDatetime);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		if (!getLoadOrg()) {
			getOrgList();
		}
		if (!getLoadUser()) {
			getUserList();
		}
		if (!getLoadDevice()) {
			getDeviceList();
		}
		if (!getLoadTask()) {
			getTaskList();
		}
		return;
	}
	std::string strTaskList;
	int nTaskCount = 0;
	if (strlen(pQryTaskList_->szOrgId)) {
		std::set<std::string> orgList;
		std::string strOrgId = pQryTaskList_->szOrgId;
		pthread_mutex_lock(&g_mutex4OrgList);
		findOrgChild(strOrgId, orgList);
		pthread_mutex_unlock(&g_mutex4OrgList);
		orgList.emplace(strOrgId);
		pthread_mutex_lock(&g_mutex4TaskList);
		EscortTask * pTask = (EscortTask *)zhash_first(g_taskList);
		while (pTask) {
			if (orgList.count(std::string(pTask->szOrg))) {
				char szCellTask[512] = { 0 };
				sprintf_s(szCellTask, sizeof(szCellTask), "{\"taskId\":\"%s\",\"deviceId\":\"%s\","
					"\"guarder\":\"%s\",\"target\":\"%s\",\"destination\":\"%s\",\"type\":%d,\"limit\":%d,"
					"\"startTime\":\"%s\"}", pTask->szTaskId, pTask->szDeviceId, pTask->szGuarder,
					pTask->szTarget, pTask->szDestination, pTask->nTaskType, pTask->nTaskLimitDistance,
					pTask->szTaskStartTime);
				if (strTaskList.empty()) {
					strTaskList = std::string(szCellTask);
				}
				else {
					strTaskList = strTaskList + "," + std::string(szCellTask);
				}
				nTaskCount++;
			}
			pTask = (EscortTask *)zhash_next(g_taskList);
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
	}
	else {
		pthread_mutex_lock(&g_mutex4TaskList);
		EscortTask * pTask = (EscortTask *)zhash_first(g_taskList);
		while (pTask) {
			char szCellTask[512] = { 0 };
			sprintf_s(szCellTask, sizeof(szCellTask), "{\"taskId\":\"%s\",\"deviceId\":\"%s\","
				"\"guarder\":\"%s\",\"target\":\"%s\",\"destination\":\"%s\",\"type\":%d,\"limit\":%d,"
				"\"startTime\":\"%s\"}", pTask->szTaskId, pTask->szDeviceId, pTask->szGuarder, 
				pTask->szTarget, pTask->szDestination, pTask->nTaskType, pTask->nTaskLimitDistance,
				pTask->szTaskStartTime);
			if (strTaskList.empty()) {
				strTaskList = std::string(szCellTask);
			}
			else {
				strTaskList = strTaskList + "," + std::string(szCellTask);
			}
			nTaskCount++;
			pTask = (EscortTask *)zhash_next(g_taskList);
		}
		pthread_mutex_unlock(&g_mutex4TaskList);
	}
	size_t nSize = 512 + strTaskList.size();
	char * pReply = new char[nSize];
	sprintf_s(pReply, nSize, "{\"cmd\":%d,\"orgId\":\"%s\",\"req\":%u,\"datetime\":\"%s\",\"count\":%d,"
		"\"list\":[%s]}", access_service::E_CMD_QUERY_TASK_LIST_REPLY, pQryTaskList_->szOrgId,
		pQryTaskList_->uiQrySeq, pQryTaskList_->szDatetime, nTaskCount, strTaskList.c_str());
	sendDataToEndpoint_v2(pReply, (uint32_t)strlen(pReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task list from %s, orgId=%s, seq=%u, "
		"datetime=%s, return %d task\r\n", __FUNCTION__, __LINE__, pEndpoint_, pQryTaskList_->szOrgId,
		pQryTaskList_->uiQrySeq, pQryTaskList_->szDatetime, nTaskCount);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
}

void AccessService::handleAppQueryDeviceStatus(access_service::AppQueryDeviceStatus * pQryDeviceStatus_,
  const char * pEndpoint_, const char * pFrom_)
{
	char szLog[512] = { 0 };
	int nErr = E_DEFAULTERROR;
	char szReply[256] = { 0 };
	if (!getLoadSessionFlag()) {
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retCode\":%d,\"session\":\"%s\",\"deviceId\":\"%s\","
			"\"status\":%d,\"battery\":%d,\"online\":%hu,\"seq\":%u,\"datetime\":\"%s\"}",
			access_service::E_CMD_QUERY_DEVICE_STATUS_REPLY, E_SERVER_RESOURCE_NOT_READY, 
			pQryDeviceStatus_->szSession, pQryDeviceStatus_->szDeviceId, 0, 0, 0, pQryDeviceStatus_->uiQrySeq,
			pQryDeviceStatus_->szDatetime);
		sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive query device status from %s, "
			"sessionId=%s, deviceId=%s, req=%u, datetime=%s, server resource not loaded\r\n", 
			__FUNCTION__, __LINE__, pEndpoint_, pQryDeviceStatus_->szSession, pQryDeviceStatus_->szDeviceId,
			pQryDeviceStatus_->uiQrySeq, pQryDeviceStatus_->szDatetime);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
	if (strlen(pQryDeviceStatus_->szSession) && strlen(pQryDeviceStatus_->szDeviceId)) {
		bool bValidLink = false;
		bool bValidDevice = false;
		bool bUpdateLink = true;
		std::string strOldLink;
		bool bHaveSubscriber = false;
		char szTopic[64] = { 0 };
		char szGuarder[20] = { 0 };
		unsigned short usDeviceStatus = 0;
		unsigned short usDeviceBattery = 0;
		unsigned short usDeviceOnline = 0;
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pQryDeviceStatus_->szSession);
			if (pLink) {
				bValidLink = true;
				pLink->nActivated = 1;
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				if (strcmp(pLink->szEndpoint, pEndpoint_) != 0) {
					strOldLink = pLink->szEndpoint;
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pEndpoint_);
					bUpdateLink = true;
				}
				if (strlen(pLink->szDeviceId) && strlen(pLink->szFactoryId) && strlen(pLink->szOrg)) {
					bHaveSubscriber = true;
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pLink->szOrg, pLink->szFactoryId, pLink->szDeviceId);
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
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pQryDeviceStatus_->szDeviceId);
				if (pDevice) {
					bValidDevice = true;
					usDeviceStatus = pDevice->deviceBasic.nStatus;
					usDeviceBattery = pDevice->deviceBasic.nBattery;
					usDeviceOnline = pDevice->deviceBasic.nOnline;
					nErr = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bUpdateLink) {
			if (strlen(szGuarder)) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strcmp(pGuarder->szLink, pEndpoint_) != 0) {
						strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pEndpoint_);
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}
			pthread_mutex_lock(&m_mutex4LinkDataList);
			if (!m_linkDataList.empty()) {
				std::string strLink = pEndpoint_;
				LinkDataList::iterator iter = m_linkDataList.find(strLink);
				if (iter != m_linkDataList.end()) {
					access_service::LinkDataInfo * pLinkData = iter->second;
					if (pLinkData && strlen(szGuarder)) {
						strcpy_s(pLinkData->szUser, sizeof(pLinkData->szUser), szGuarder);
					}
				}
				if (!strOldLink.empty()) {
					LinkDataList::iterator iter2 = m_linkDataList.find(strOldLink);
					if (iter2 != m_linkDataList.end()) {
						access_service::LinkDataInfo * pLinkData2 = iter2->second;
						if (pLinkData2) {
							if (pLinkData2->pLingeData) {
								delete [] pLinkData2->pLingeData;
								pLinkData2->pLingeData = NULL;
							}
							delete pLinkData2;
							pLinkData2 = NULL;
						}
					}
					m_linkDataList.erase(iter);
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
			if (bHaveSubscriber && strlen(szTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
					m_subscribeList, szTopic);
				if (pSubInfo) {
					if (strcmp(pSubInfo->szEndpoint, pEndpoint_) != 0) {
						strcpy_s(pSubInfo->szEndpoint, sizeof(pSubInfo->szEndpoint), pEndpoint_);
					}
					if (pFrom_ && strcmp(pFrom_, pSubInfo->szAccSource) != 0) {
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pFrom_);
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
			}
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"deviceId\":\"%s\","
			"\"status\":%hu,\"battery\":%hu,\"online\":%hu,\"seq\":%u,\"datetime\":\"%s\"}",
			access_service::E_CMD_QUERY_DEVICE_STATUS_REPLY, nErr, pQryDeviceStatus_->szSession,
			pQryDeviceStatus_->szDeviceId, usDeviceStatus, usDeviceBattery, usDeviceOnline,
			pQryDeviceStatus_->uiQrySeq, pQryDeviceStatus_->szDatetime);
		int nVal = sendDataToEndpoint_v2(szReply, (uint32_t)strlen(szReply), pEndpoint_, 
			access_service::E_ACC_DATA_TRANSFER, pFrom_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]reply query device status to %s, %d, deviceId=%s,"
			" session=%s, status=%hu, battery=%hu, online=%hu, seq=%u, datetime=%s\r\n", __FUNCTION__, __LINE__,
			pEndpoint_, nVal, pQryDeviceStatus_->szDeviceId, pQryDeviceStatus_->szSession, usDeviceStatus,
			usDeviceBattery, usDeviceOnline, pQryDeviceStatus_->uiQrySeq, pQryDeviceStatus_->szDatetime);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
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
	unsigned int now = (unsigned int)time(NULL);
	char szSession[16] = { 0 };
	sprintf_s(szSession, sizeof(szSession), "%u", now);
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
	char szTaskId[16] = { 0 };
	sprintf_s(szTaskId, sizeof(szTaskId), "%x%02x", now, ++n);
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
			switch (pMsg->uiMsgType) {
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
									aliveMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicDeviceAliveMsg(&aliveMsg, pMsg->szMsgMark) == E_OK) {
							bNeedStore = true;
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alive message error\r\n",
							__FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
									if (doc["direction"].IsDouble()) {
										gpsLocateMsg.dDirection = doc["direction"].GetDouble();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											gpsLocateMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
										}
									}
								}
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										gpsLocateMsg.nFlag = doc["locateFlag"].GetInt();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										gpsLocateMsg.nCoordinate = doc["coordinate"].GetInt();
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
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										lbsLocateMsg.nCoordinate = doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											lbsLocateMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
										}
									}
								}
								if (handleTopicLocateLbsMsg(&lbsLocateMsg, pMsg->szMsgMark) == E_OK) {
									bNeedStore = true;
								}
								break;
							}
							case LOCATE_APP: { //ignore
								if (strcmp(m_szInteractorIdentity, pMsg->szMsgFrom) != 0) {
									TopicLocateMessageApp locateMsgApp;
									char szDatetime[20] = { 0 };
									if (doc.HasMember("factoryId")) {
										if (doc["factoryId"].IsString()) {
											size_t nSize = doc["factoryId"].GetStringLength();
											if (nSize) {
												strcpy_s(locateMsgApp.szFactoryId, sizeof(locateMsgApp.szFactoryId),
													doc["factoryId"].GetString());
											}
										}
									}
									if (doc.HasMember("deviceId")) {
										if (doc["deviceId"].IsString()) {
											size_t nSize = doc["deviceId"].GetStringLength();
											if (nSize) {
												strncpy_s(locateMsgApp.szDeviceId, sizeof(locateMsgApp.szDeviceId),
													doc["deviceId"].GetString(), nSize);
											}
										}
									}
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(locateMsgApp.szOrg, sizeof(locateMsgApp.szOrg), doc["orgId"].GetString());
											}
										}
									}
									if (doc.HasMember("taskId")) {
										if (doc["taskId"].IsString()) {
											size_t nSize = doc["taskId"].GetStringLength();
											if (nSize) {
												strcpy_s(locateMsgApp.szTaskId, sizeof(locateMsgApp.szTaskId), doc["taskId"].GetString());
											}
										}
									}
									if (doc.HasMember("latitude")) {
										if (doc["latitude"].IsDouble()) {
											locateMsgApp.dLat = doc["latitude"].GetDouble();
										}
									}
									if (doc.HasMember("lngitude")) {
										if (doc["lngitude"].IsDouble()) {
											locateMsgApp.dLng = doc["lngitude"].GetDouble();
										}
									}
									if (doc.HasMember("coordinate")) {
										if (doc["coordinate"].IsInt()) {
											locateMsgApp.nCoordinate = doc["coordinate"].GetInt();
										}
									}
									if (doc.HasMember("battery")) {
										if (doc["battery"].IsInt()) {
											locateMsgApp.usBattery = (unsigned short)doc["battery"].GetInt();
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
												locateMsgApp.ulMessageTime = makeDatetime(szDatetime);
											}
										}
									}
									if (strlen(locateMsgApp.szDeviceId) && strlen(locateMsgApp.szTaskId) 
										&& locateMsgApp.dLat > 0.00 && locateMsgApp.dLng > 0.00 && strlen(szDatetime)) {
										//update information 
										pthread_mutex_lock(&g_mutex4DevList);
										if (zhash_size(g_deviceList)) {
											WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
												locateMsgApp.szDeviceId);
											if (pDev) {
												if (pDev->ulLastGuarderLocateTime < locateMsgApp.ulMessageTime) {
													pDev->ulLastDeviceLocateTime = locateMsgApp.ulMessageTime;
													pDev->guardPosition.dLatitude = locateMsgApp.dLat;
													pDev->guardPosition.dLngitude = locateMsgApp.dLng;
													pDev->guardPosition.nCoordinate = locateMsgApp.nCoordinate;
												}
											}
										}
										pthread_mutex_unlock(&g_mutex4DevList);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]locate app message parameter"
											" data miss, factoryId=%s, deviceId=%s, orgId=%s, taskId=%s, latitude=%f, "
											"lngitude=%f, coordinate=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, 
											locateMsgApp.szFactoryId, locateMsgApp.szDeviceId, locateMsgApp.szOrg, 
											locateMsgApp.szTaskId, locateMsgApp.dLat, locateMsgApp.dLng, 
											locateMsgApp.nCoordinate, szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic locate message "
									"error, not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic locate message "
							"error\r\n", __FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
											lpAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
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
											lsAlarmMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
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
												fleeAlarmMsg.ulMessageTime = makeDatetime(szDatetime);
												bValidDatetime = true;
											}
										}
									}
									if (bValidDevice && bValidGuarder && bValidTask && bValidMode && bValidDatetime) {
										handleTopicAlarmFleeMsg(&fleeAlarmMsg, pMsg->szMsgMark);
									}
									else {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse flee message data miss, "
											"factoryId=%s, deviceId=%s, orgId=%s, guarder=%s, taskId=%s, mode=%hu, battery=%hu, "
											"datetime=%s\r\n", __FUNCTION__, __LINE__, fleeAlarmMsg.szFactoryId,
											fleeAlarmMsg.szDeviceId, fleeAlarmMsg.szOrg, fleeAlarmMsg.szGuarder,
											fleeAlarmMsg.szTaskId, fleeAlarmMsg.usMode, fleeAlarmMsg.usBattery, szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								break;
							}
							case ALARM_LOCATE_LOST: {
								TopicAlarmMessageLocateLost alarmLocateLost;
								bool bValidDevice = false;
								bool bValidOrg = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmLocateLost.szFactoryId, sizeof(alarmLocateLost.szFactoryId),
												doc["factoryId"].GetString());
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmLocateLost.szDeviceId, sizeof(alarmLocateLost.szDeviceId),
												doc["deviceId"].GetString());
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmLocateLost.szOrg, sizeof(alarmLocateLost.szOrg), 
												doc["orgId"].GetString());
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmLocateLost.szGuarder, sizeof(alarmLocateLost.szGuarder),
												doc["guarder"].GetString());
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										alarmLocateLost.usDeviceBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										alarmLocateLost.usAlarmMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											alarmLocateLost.ulMessageTime = makeDatetime(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidDatetime && bValidDevice && bValidOrg ) {
									handleTopicAlarmLocateLostMsg(&alarmLocateLost, pMsg->szMsgFrom);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm locate "
										"lost data miss, factoryId=%s, deviceId=%s, org=%s, guarder=%s, battery=%hu, "
										"datetime=%s\r\n", __FUNCTION__, __LINE__, alarmLocateLost.szFactoryId, 
										alarmLocateLost.szDeviceId, alarmLocateLost.szOrg, alarmLocateLost.szGuarder, 
										alarmLocateLost.usDeviceBattery, szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case ALARM_DEVICE_FENCE: {
								TopicAlarmMessageFence fenceAlarmMsg;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFactoryId, sizeof(fenceAlarmMsg.szFactoryId),
												doc["factoryId"].GetString());
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szDeviceId, sizeof(fenceAlarmMsg.szDeviceId),
												doc["deviceId"].GetString());
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szOrgId, sizeof(fenceAlarmMsg.szOrgId), doc["orgId"].GetString());
										}
									}
								}
								if (doc.HasMember("fenceId")) {
									if (doc["fenceId"].IsString()) {
										size_t nSize = doc["fenceId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFenceId, sizeof(fenceAlarmMsg.szFenceId), doc["fenceId"].GetString());
										}
									}
								}
								if (doc.HasMember("fenceTaskId")) {
									if (doc["fenceTaskId"].IsString()) {
										size_t nSize = doc["fenceTaskId"].GetStringLength();
										if (nSize) {
											strcpy_s(fenceAlarmMsg.szFenceTaskId, sizeof(fenceAlarmMsg.szFenceTaskId),
												doc["fenceTaskId"].GetString());
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										fenceAlarmMsg.dLatitude = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										fenceAlarmMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										fenceAlarmMsg.dLngitude = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										fenceAlarmMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										fenceAlarmMsg.nCoordinate = (int8_t)doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("locateType")) {
									if (doc["locateType"].IsInt()) {
										fenceAlarmMsg.nLocateType = (int8_t)doc["locateType"].GetInt();
									}
								}
								if (doc.HasMember("policy")) {
									if (doc["policy"].IsInt()) {
										fenceAlarmMsg.nPolicy = (int8_t)doc["policy"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										fenceAlarmMsg.nMode = (int8_t)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											fenceAlarmMsg.ulMessageTime = makeDatetime(szDatetime);
										}
									}
								}
								if (strlen(fenceAlarmMsg.szOrgId) && strlen(fenceAlarmMsg.szFactoryId) 
									&& strlen(szDatetime) && strlen(fenceAlarmMsg.szDeviceId) 
									&& strlen(fenceAlarmMsg.szFenceTaskId) && strlen(fenceAlarmMsg.szFenceId) 
									&& fenceAlarmMsg.nLocateType != LOCATE_APP
									&& fenceAlarmMsg.dLatitude > 0.00 && fenceAlarmMsg.dLngitude > 0.00 ) {
									handleTopicAlarmFenceMsg(&fenceAlarmMsg, pMsg->szMsgMark);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse fence alarm, "
										"data parameter miss, deviceId=%s, factoryId=%s, orgId=%s, fenceTaskId=%s, fenceId=%s,"
										" locateType=%d, latitude=%f, lngitude=%f, policy=%d, mode=%d, datetime=%s\r\n", 
										__FUNCTION__, __LINE__, fenceAlarmMsg.szDeviceId, fenceAlarmMsg.szFactoryId, 
										fenceAlarmMsg.szOrgId, fenceAlarmMsg.szFenceTaskId, fenceAlarmMsg.szFenceId, 
										fenceAlarmMsg.nLocateType, fenceAlarmMsg.dLatitude, fenceAlarmMsg.dLngitude, 
										fenceAlarmMsg.nPolicy, fenceAlarmMsg.nMode, szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error,"
									" not support type=%d\r\n", __FUNCTION__, __LINE__, nSubType);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic alarm message error\r\n",
							__FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										strcpy_s(devBindMsg.szOrg, sizeof(devBindMsg.szOrg), doc["orgId"].GetString());
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
										devBindMsg.ulMessageTime = makeDatetime(szDatetime);
									}
								}
							}
							if (bValidDevice && bValidGuarder && bValidMode && bValidDatetime) {
								handleTopicDeviceBindMsg(&devBindMsg, pMsg->szMsgMark);
							}
							else {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic device bind "
									"message data miss, factoryId=%s, deviceId=%s, org=%s, guarder=%s, mode=%hu, "
									"battery=%hu, datetime=%s, topic=%s, sequence=%u, uuid=%s, from=%s\r\n", 
									__FUNCTION__, __LINE__, devBindMsg.szFactoryId, devBindMsg.szDeviceId, devBindMsg.szOrg,
									devBindMsg.szGuarder, devBindMsg.usMode, devBindMsg.usBattery, szDatetime, 
									pMsg->szMsgMark, pMsg->uiMsgSequence, pMsg->szMsgUuid, pMsg->szMsgFrom);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic device bind "
								"message error\r\n", __FUNCTION__, __LINE__);
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
									strcpy_s(onlineMsg.szOrg, sizeof(onlineMsg.szOrg), doc["orgId"].GetString());
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
									onlineMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicDeviceOnlineMsg(&onlineMsg, pMsg->szMsgMark) == 0) {
							bNeedStore = true;
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic online message "
							"error=%d, \r\n", __FUNCTION__, __LINE__, doc.GetParseError());
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
									offlineMsg.ulMessageTime = makeDatetime(doc["datetime"].GetString());
								}
							}
						}
						if (handleTopicDeviceOfflineMsg(&offlineMsg, pMsg->szMsgMark) == 0) {
							bNeedStore = true;
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic offline message error\n", 
							__FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_CHARGE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicDeviceChargeMessage devChargeMsg;
						memset(&devChargeMsg, 0, sizeof(TopicDeviceChargeMessage));
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
								strcpy_s(devChargeMsg.szFactoryId, sizeof(devChargeMsg.szFactoryId), doc["factoryId"].GetString());
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(devChargeMsg.szDeviceId, sizeof(devChargeMsg.szDeviceId), doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("org")) {
							if (doc["org"].IsString() && doc["org"].GetStringLength()) {
								strcpy_s(devChargeMsg.szOrg, sizeof(devChargeMsg.szOrg), doc["org"].GetString());
							}
						}
						if (doc.HasMember("state")) {
							if (doc["state"].IsInt()) {
								devChargeMsg.nState = doc["state"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
								devChargeMsg.ullMsgTime = makeDatetime(doc["datetime"].GetString());
							}
						}
						if (strlen(devChargeMsg.szDeviceId)) {
							handleTopicDeviceChargeMsg(&devChargeMsg, pMsg->szMsgMark);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic charge message error\n",
							__FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
													strcpy_s(taskMsg.szTaskId, sizeof(taskMsg.szTaskId), 
														doc["taskId"].GetString());
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
													strcpy_s(taskMsg.szDeviceId, sizeof(taskMsg.szDeviceId), 
														doc["deviceId"].GetString());
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
													strcpy_s(taskMsg.szGuarder, sizeof(taskMsg.szGuarder), doc["guarder"].GetString());
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
													strcpy_s(taskMsg.szTarget, sizeof(taskMsg.szTarget), doc["target"].GetString());
													bValidTask = true;
												}
											}
										}
										if (doc.HasMember("handset")) {
											if (doc["handset"].IsString()) {
												size_t nSize = doc["handset"].GetStringLength();
												if (nSize) {
													strcpy_s(taskMsg.szHandset, sizeof(taskMsg.szHandset), doc["handset"].GetString());
												}
											}
										}
										if (doc.HasMember("datetime")) {
											if (doc["datetime"].IsString()) {
												size_t nSize = doc["datetime"].GetStringLength();
												if (nSize) {
													strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
													taskMsg.ulMessageTime = makeDatetime(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidTask && bValidDevice && bValidGuarder && bValidDatetime) {
											handleTopicTaskSubmitMsg(&taskMsg, pMsg->szMsgMark);
										}
										else {
											sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic task message data miss, "
												"taskId=%s, guarder=%s, deviceId=%s, factoryId=%s, datetime=%s, msgTopic=%s, msgSeq=%u, "
												"msgUuid=%s, msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskMsg.szTaskId, taskMsg.szGuarder,
												taskMsg.szDeviceId, taskMsg.szFactoryId, szDatetime, pMsg->szMsgMark, pMsg->uiMsgSequence,
												pMsg->szMsgUuid, pMsg->szMsgFrom);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
													taskModifyMsg.ulMessageTime = makeDatetime(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidTask && bValidDatetime) {
											handleTopicTaskModifyMsg(&taskModifyMsg, pMsg->szMsgMark);
										}
										else {
											sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic task modify message data "
												"miss, taskId=%s, datetime=%s, handset=%s, msgTopic=%s, msgSeq=%u, msgUuid=%s, msgFrom=%s"
												"\r\n", __FUNCTION__, __LINE__, taskModifyMsg.szTaskId, szDatetime, taskModifyMsg.szHandset,
												pMsg->szMsgMark, pMsg->uiMsgSequence, pMsg->szMsgUuid, pMsg->szMsgFrom);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
													taskCloseMsg.ulMessageTime = makeDatetime(szDatetime);
													bValidDatetime = true;
												}
											}
										}
										if (bValidState && bValidDatetime && bValidTask) {
											handleTopicTaskCloseMsg(&taskCloseMsg, pMsg->szMsgMark);
										}
										else {
											sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse topic task close message "
												"data miss, taskId=%s, state=%d, datetime=%s, msgTopic=%s, msgSeq=%u, msgUuid=%s, "
												"msgFrom=%s\r\n", __FUNCTION__, __LINE__, taskCloseMsg.szTaskId, taskCloseMsg.nClose, 
												szDatetime, pMsg->szMsgMark, pMsg->uiMsgSequence, pMsg->szMsgUuid, pMsg->szMsgFrom);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										}
										break;
									}
									default: {
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse task message subType not "
											"support=%d\r\n", __FUNCTION__, __LINE__, nSubType);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
										break;
									}
								}
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse topic task message json error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic Buffer Modify,"
											" object=%u, operate=%d, guarder=%s, org=%s, datetime=%s\r\n", __FUNCTION__, 
											__LINE__, BUFFER_GUARDER, nOperate, bValidGuarder ? guarder.szId : "",
											bValidOrg ? guarder.szOrg : "", bValidDateTime ? szDateTime : "");
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									}
								}
								else {
									bool bValidPasswd = false;
									bool bValidRoleType = false;
									if (doc.HasMember("name")) {
										if (doc["name"].IsString()) {
											size_t nSize = doc["name"].GetStringLength();
											if (nSize) {
												strcpy_s(guarder.szTagName, sizeof(guarder.szTagName), doc["name"].GetString());
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
									if (bValidGuarder && bValidOrg && bValidDateTime && bValidPasswd && bValidRoleType) {
										if (nOperate == BUFFER_OPERATE_NEW) {
											pthread_mutex_lock(&g_mutex4GuarderList);
											Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, guarder.szId);
											if (pGuarder) {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify"
													" message, object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s,"
													" roleType=%hu already exists in the guarderList\r\n", __FUNCTION__, 
													__LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId, 
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
												pGuarder->usState = STATE_GUARDER_FREE;
												pGuarder->usRoleType = guarder.usRoleType;
												zhash_update(g_guarderList, pGuarder->szId, pGuarder);
												zhash_freefn(g_guarderList, pGuarder->szId, free);
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message"
													", object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
													__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_NEW, guarder.szId, 
													guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
												LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											} 
											pthread_mutex_unlock(&g_mutex4GuarderList);
										}
										else if (nOperate == BUFFER_OPERATE_UPDATE) {
											pthread_mutex_lock(&g_mutex4GuarderList);
											Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, guarder.szId);
											if (pGuarder) {
												bool bFlag = false;
												if (strcmp(pGuarder->szTagName, guarder.szTagName) != 0) {
													strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
														guarder.szTagName, strlen(guarder.szTagName));
													if (!bFlag) {
														bFlag = true;
													}
												}
												if (strcmp(pGuarder->szPassword, guarder.szPassword) != 0) {
													strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
														guarder.szPassword, strlen(guarder.szPassword));
													if (!bFlag) {
														bFlag = true;
													}
												}
												if (strcmp(pGuarder->szOrg, guarder.szOrg) != 0) {
													strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), guarder.szOrg, 
														strlen(guarder.szOrg));
													if (!bFlag) {
														bFlag = true;
													}
												}
												if (pGuarder->usRoleType != guarder.usRoleType) {
													pGuarder->usRoleType = guarder.usRoleType;
													if (!bFlag) {
														bFlag = true;
													}
												}
												if (bFlag) {
													sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message"
														", object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, roleType=%hu\r\n",
														__FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE, guarder.szId,
														guarder.szTagName, guarder.szPassword, guarder.szOrg, guarder.usRoleType);
													LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
												}
											}
											else {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer Modify message, "
													"object=%u, operate=%u, guarder=%s, name=%s, passwd=%s, orgId=%s, not found in "
													"guarderList\r\n", __FUNCTION__, __LINE__, BUFFER_GUARDER, BUFFER_OPERATE_UPDATE,
													guarder.szId, guarder.szTagName, guarder.szPassword, guarder.szOrg);
												LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
											strcpy_s(device.deviceBasic.szDeviceId, sizeof(device.deviceBasic.szDeviceId),
												doc["deviceId"].GetString());
											bValidDeviceId = true;
										}
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strcpy_s(device.deviceBasic.szFactoryId, sizeof(device.deviceBasic.szFactoryId), 
												doc["factoryId"].GetString());
											bValidFactoryId = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(device.deviceBasic.szOrgId, sizeof(device.deviceBasic.szOrgId),
												doc["orgId"].GetString());
											bValidOrgId = true;
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										device.deviceBasic.nBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
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
											bool bFlag = false;
											if (strlen(pDevice->deviceBasic.szOrgId) == 0) {
												strcpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
													device.deviceBasic.szOrgId);
												if (!bFlag) {
													bFlag = true;
												}
											}
											if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
												strcpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
													device.deviceBasic.szFactoryId);
												if (!bFlag) {
													bFlag = true;
												}
											}
											if (bFlag) {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]topic Buffer modify mess"
													"age, object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s"
													", already found in the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE,
													nOperate, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
													device.deviceBasic.szOrgId, szDatetime);
												LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
										}
										else {
											pDevice = (WristletDevice *)zmalloc(nDeviceSize);
											memset(pDevice, 0, nDeviceSize);
											strncpy_s(pDevice->deviceBasic.szDeviceId, sizeof(pDevice->deviceBasic.szDeviceId),
												device.deviceBasic.szDeviceId, strlen(device.deviceBasic.szDeviceId));
											strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
												device.deviceBasic.szFactoryId, strlen(device.deviceBasic.szFactoryId));
											strncpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
												device.deviceBasic.szOrgId, strlen(device.deviceBasic.szOrgId));
											pDevice->deviceBasic.nOnline = 0;
											pDevice->deviceBasic.nBattery = 0;
											pDevice->deviceBasic.nStatus = DEV_ONLINE;
											pDevice->deviceBasic.ulLastActiveTime = 0;
											pDevice->nLastLocateType = 0;
											pDevice->szBindGuard[0] = '\0';
											pDevice->szLinkId[0] = '\0';
											pDevice->ulBindTime = 0;
											pDevice->ulLastFleeAlertTime = 0;
											pDevice->ulLastDeviceLocateTime = 0;
											pDevice->ulLastGuarderLocateTime = 0;
											pDevice->ulLastLooseAlertTime = 0;
											pDevice->ulLastLowPowerAlertTime = 0;
											pDevice->devicePosition.dLatitude = pDevice->devicePosition.dLngitude = 0.000000;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = 0;
											pDevice->guardPosition.dLatitude = pDevice->guardPosition.dLngitude = 0.000000;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->guardPosition.nPrecision = 0;
											pDevice->nDeviceFenceState = 0;
											pDevice->nDeviceHasFence = 0;
											zhash_update(g_deviceList, szDevKey, pDevice);
											zhash_freefn(g_deviceList, szDevKey, free);
											sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]topic Buffer modify message,"
												" object=%u, operate=%d, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
												__FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate, device.deviceBasic.szDeviceId,
												device.deviceBasic.szFactoryId, device.deviceBasic.szOrgId, szDatetime);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										}
										pthread_mutex_unlock(&g_mutex4DevList);
									}
									else if (nOperate == BUFFER_OPERATE_UPDATE) {
										pthread_mutex_lock(&g_mutex4DevList);
										WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDevKey);
										if (pDevice) {
											bool bFlag = false;
											if (strcmp(pDevice->deviceBasic.szFactoryId, device.deviceBasic.szFactoryId) != 0) {
												strcpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
													device.deviceBasic.szFactoryId);
												bFlag = true;
											}
											if (strcmp(pDevice->deviceBasic.szOrgId, device.deviceBasic.szOrgId) != 0) {
												strcpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
													device.deviceBasic.szOrgId);
												bFlag = true;
											}
											if (bFlag) {
												sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message, "
													"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s\r\n",
													__FUNCTION__, __LINE__, BUFFER_DEVICE, BUFFER_OPERATE_UPDATE,
													device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
													device.deviceBasic.szOrgId, szDatetime);
												LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
											}
										}
										else {
											sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
												"object=%u, operate=%u, deviceId=%s, factoryId=%s, orgId=%s, datetime=%s,"
												" not find int the deviceList\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, 
												BUFFER_OPERATE_UPDATE, device.deviceBasic.szDeviceId, device.deviceBasic.szFactoryId,
												device.deviceBasic.szOrgId, szDatetime);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
										" object=%u, operate=%d, parameter miss, deviceId=%s, factoryId=%s, orgId=%s, "
										"datetime=%s\r\n", __FUNCTION__, __LINE__, BUFFER_DEVICE, nOperate,
										bValidDeviceId ? device.deviceBasic.szDeviceId : "",
										bValidFactoryId ? device.deviceBasic.szFactoryId : "",
										bValidOrgId ? device.deviceBasic.szOrgId : "", bValidDatetime ? szDatetime : "");
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case BUFFER_ORG: {
								size_t nOrgSize = sizeof(OrganizationEx);
								char szDatetime[20] = { 0 };
								if (nOperate == BUFFER_OPERATE_NEW) {
									OrganizationEx * pOrg = (OrganizationEx *)zmalloc(nOrgSize);
									memset(pOrg, 0, nOrgSize);
									pOrg->childList.clear();
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szOrgId, sizeof(pOrg->org.szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (doc.HasMember("orgName")) {
										if (doc["orgName"].IsString()) {
											size_t nSize = doc["orgName"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szOrgName, sizeof(pOrg->org.szOrgName), 
													doc["orgName"].GetString());
											}
										}
									}
									if (doc.HasMember("parentId")) {
										if (doc["parentId"].IsString()) {
											size_t nSize = doc["parentId"].GetStringLength();
											if (nSize) {
												strcpy_s(pOrg->org.szParentOrgId, sizeof(pOrg->org.szParentOrgId), 
													doc["parentId"].GetString());
											}
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											}
										}
									}
									if (strlen(pOrg->org.szOrgId) && strlen(szDatetime)) {
										std::string strOrgId = pOrg->org.szOrgId;
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]topic buffer add new org "
											", orgId=%s, orgName=%s, parentId=%s\r\n", __FUNCTION__, __LINE__, pOrg->org.szOrgId,
											pOrg->org.szOrgName, pOrg->org.szParentOrgId);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										pthread_mutex_lock(&g_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.find(strOrgId);
										if (iter != g_orgList2.end()) {
											free(pOrg);
											pOrg = NULL;
										} else {
											g_orgList2.emplace(strOrgId, pOrg);
										}
										pthread_mutex_unlock(&g_mutex4OrgList);
										loadOrgList();
									}
								}
								else if (nOperate == BUFFER_OPERATE_UPDATE) {
									Organization org;
									memset(&org, 0, sizeof(org));
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szOrgId, sizeof(org.szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (doc.HasMember("orgName")) {
										if (doc["orgName"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szOrgName, sizeof(org.szOrgName), doc["orgName"].GetString());
											}
										}
									}
									if (doc.HasMember("parentId")) {
										if (doc["parentId"].IsString()) {
											size_t nSize = doc["parentId"].GetStringLength();
											if (nSize) {
												strcpy_s(org.szParentOrgId, sizeof(org.szParentOrgId), 
													doc["parentId"].GetString());
											}
										}
									}
									if (doc.HasMember("datetime")) {
										if (doc["datetime"].IsString()) {
											size_t nSize = doc["datetime"].GetStringLength();
											if (nSize) {
												strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											}
										}
									}
									if (strlen(org.szOrgId) && strlen(szDatetime)) {
										std::string strOrgId = org.szOrgId;
										bool bReload = false;
										pthread_mutex_lock(&g_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.find(strOrgId);
										if (iter != g_orgList2.end()) {
											OrganizationEx * pOrg = iter->second;
											if (pOrg) {
												if (strcmp(pOrg->org.szOrgName, org.szOrgName) != 0) {
													strcpy_s(pOrg->org.szOrgName, sizeof(pOrg->org.szOrgName), org.szOrgName);
												}
												if (strcmp(pOrg->org.szParentOrgId, org.szParentOrgId) != 0) {
													strcpy_s(pOrg->org.szParentOrgId, sizeof(pOrg->org.szParentOrgId), 
														org.szParentOrgId);
													bReload = true;
												}
											}
										}
										pthread_mutex_unlock(&g_mutex4OrgList);
										if (bReload) {
											loadOrgList();
										}
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]topic buffer update org, "
											"orgId=%s, orgName=%s, parentId=%s\r\n", __FUNCTION__, __LINE__, org.szOrgId,
											org.szOrgName, org.szParentOrgId);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								else if (nOperate == BUFFER_OPERATE_DELETE) {
									char szOrgId[40] = { 0 };
									if (doc.HasMember("orgId")) {
										if (doc["orgId"].IsString()) {
											size_t nSize = doc["orgId"].GetStringLength();
											if (nSize) {
												strcpy_s(szOrgId, sizeof(szOrgId), doc["orgId"].GetString());
											}
										}
									}
									if (strlen(szOrgId)) {
										std::string strOrgId = szOrgId;
										bool bReload = false;
										pthread_mutex_lock(&g_mutex4OrgList);
										std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.find(strOrgId);
										if (iter != g_orgList2.end()) {
											OrganizationEx * pOrg = iter->second;
											if (pOrg) {
												delete pOrg;
												pOrg = NULL;
											}
											g_orgList2.erase(iter);
										}
										pthread_mutex_unlock(&g_mutex4OrgList);
										if (bReload) {
											loadOrgList(true);
										}
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]topic buffer delete org, "
											"orgId=%s\r\n", __FUNCTION__, __LINE__, szOrgId);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]Topic Buffer modify message,"
									"object=%d not support, seq=%u\r\n", __FUNCTION__, __LINE__, nObject, 
									pMsg->uiMsgSequence);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic buffer modify message"
							"error, seq=%u, \r\n", __FUNCTION__, __LINE__, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGIN: {
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicLoginMessage loginMsg;
							memset(&loginMsg, 0, sizeof(TopicLoginMessage));
							bool bValidAccount = false;
							bool bValidSession = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("account")) {
								if (doc["account"].IsString()) {
									size_t nSize = doc["account"].GetStringLength();
									if (nSize) {
										strcpy_s(loginMsg.szAccount, sizeof(loginMsg.szAccount), doc["account"].GetString());
										bValidAccount = true;
									}
								}
							}
							if (doc.HasMember("session")) {
								size_t nSize = doc["session"].GetStringLength();
								if (nSize) {
									strcpy_s(loginMsg.szSession, sizeof(loginMsg.szSession), doc["session"].GetString());
									bValidSession = true;
								}
							}
							if (doc.HasMember("handset")) {
								size_t nSize = doc["handset"].GetStringLength();
								if (nSize) {
									strcpy_s(loginMsg.szHandset, sizeof(loginMsg.szHandset), doc["handset"].GetString());
								}
							}
							if (doc.HasMember("loginFormat")) {
								if (doc["loginFormat"].IsUint()) {
									loginMsg.usLoginFormat = (unsigned short)doc["loginFormat"].GetUint();
								}
							}
							if (doc.HasMember("datetime")) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
									loginMsg.ulMessageTime = makeDatetime(szDatetime);
									bValidDatetime = true;
								}
							}
							if (bValidAccount && bValidSession && bValidDatetime) {
								handleTopicUserLoginMsg(&loginMsg, pMsg->szMsgMark);
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic login message error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGOUT: {
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicLogoutMessage logoutMsg;
							memset(&logoutMsg, 0, sizeof(TopicLogoutMessage));
							bool bValidAccount = false;
							bool bValidSession = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("account")) {
								if (doc["account"].IsString()) {
									size_t nSize = doc["account"].GetStringLength();
									if (nSize) {
										strcpy_s(logoutMsg.szAccount, sizeof(logoutMsg.szAccount), doc["account"].GetString());
										bValidAccount = true;
									}
								}
							}
							if (doc.HasMember("session")) {
								size_t nSize = doc["session"].GetStringLength();
								if (nSize) {
									strcpy_s(logoutMsg.szSession, sizeof(logoutMsg.szSession), doc["session"].GetString());
									bValidSession = true;
								}
							}
							if (doc.HasMember("handset")) {
								size_t nSize = doc["handset"].GetStringLength();
								if (nSize) {
									strcpy_s(logoutMsg.szHandset, sizeof(logoutMsg.szHandset), doc["handset"].GetString());
								}
							}
							if (doc.HasMember("datetime")) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
									logoutMsg.ulMessageTime = makeDatetime(szDatetime);
									bValidDatetime = true;
								}
							}
							if (bValidAccount && bValidSession && bValidDatetime) {
								handleTopicUserLogoutMsg(&logoutMsg, pMsg->szMsgMark);
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse Topic login message error=%d\r\n",
								__FUNCTION__, __LINE__, doc.GetParseError());
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					break;
				}
				case PUBMSG_ACCOUNT_ALIVE: {
					if (strcmp(pMsg->szMsgFrom, m_szInteractorIdentity) != 0) {
						rapidjson::Document doc;
						if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
							TopicUserAliveMessage userAliveMsg;
							memset(&userAliveMsg, 0, sizeof(TopicUserAliveMessage));
							if (doc.HasMember("account")) {
								if (doc["account"].IsString() && doc["account"].GetStringLength()) {
									strcpy_s(userAliveMsg.szAccount, sizeof(userAliveMsg.szAccount), doc["account"].GetString());
								}
							}
							if (doc.HasMember("session")) {
								if (doc["session"].IsString() && doc["session"].GetStringLength()) {
									strcpy_s(userAliveMsg.szSession, sizeof(userAliveMsg.szSession), doc["session"].GetString());
								}
							}
							if (doc.HasMember("datetime")) {
								if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
									userAliveMsg.ullMessageTime = makeDatetime(doc["datetime"].GetString());
								}
							}
							if (doc.HasMember("loginFormat")) {
								if (doc["loginFormat"].IsUint()) {
									userAliveMsg.usFormat = (unsigned short)doc["loginFormat"].GetUint();
								}
							}
							if (strlen(userAliveMsg.szAccount) && strlen(userAliveMsg.szSession)) {
								handleTopicUserAliveMsg(&userAliveMsg, pMsg->szMsgMark);
							}
						}
					}
					break;
				}
			}
			if (bNeedStore && m_nRun) {
				storeTopicMsg(pMsg);
			}
			pthread_mutex_lock(&m_mutex4RemoteLink);
			if (m_remoteMsgSrvLink.nActive == 0) {
				m_remoteMsgSrvLink.nActive = 1;
			}
			m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
			pthread_mutex_unlock(&m_mutex4RemoteLink);
		}
	} while (1);
}

void AccessService::storeTopicMsg(const TopicMessage * pMsg)
{
	if (strlen(pMsg->szMsgBody) && strlen(pMsg->szMsgUuid) && pMsg->uiMsgType > 0) {
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
						unsigned int nSequence = 0;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("type")) {
							if (doc["type"].IsInt()) {
								nType = doc["type"].GetInt();
								bValidType = true;
							}
						}
						if (doc.HasMember("sequence")) {
							if (doc["sequence"].IsUint()) {
								nSequence = doc["sequence"].GetUint();
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
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									break;
								}
								case MSG_SUB_SNAPSHOT: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
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
														topicMsg.uiMsgSequence = (unsigned int)doc["msg"][0]["msgSequence"].GetInt();
													}
												}
												if (doc["msg"][0].HasMember("msgType")) {
													if (doc["msg"][0]["msgType"].IsInt()) {
														topicMsg.uiMsgType = (unsigned int)doc["msg"][0]["msgType"].GetInt();
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
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									break;
								}
								case MSG_SUB_REQUEST: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									break;
								}
								case MSG_SUB_GETINFO: {
									pthread_mutex_lock(&m_mutex4RemoteLink);
									if (m_remoteMsgSrvLink.nActive == 0) {
										m_remoteMsgSrvLink.nActive = 1;
									}
									m_remoteMsgSrvLink.ulLastActiveTime = (unsigned long long)time(NULL);
									pthread_mutex_unlock(&m_mutex4RemoteLink);
									int nGetType = 0;
									if (doc.HasMember("getType")) {
										if (doc["getType"].IsInt()) {
											nGetType = doc["getType"].GetInt();
										}
									}
									if (doc.HasMember("list")) {
										if (doc["list"].IsArray()) {
											int nCount = (int)doc["list"].Size();
											switch (nGetType) {
												case BUFFER_ORG: {
													size_t nOrgSize = sizeof(Organization);
													pthread_mutex_lock(&g_mutex4OrgList);
													for (int i = 0; i < nCount; i++) {
														OrganizationEx org;
														memset(&org.org, 0, nOrgSize);
														if (doc["list"][i].HasMember("orgId")) {
															if (doc["list"][i]["orgId"].IsString()
																&& doc["list"][i]["orgId"].GetStringLength()) {
																strcpy_s(org.org.szOrgId, sizeof(org.org.szOrgId),
																	doc["list"][i]["orgId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("orgName")) {
															if (doc["list"][i]["orgName"].IsString()
																&& doc["list"][i]["orgName"].GetStringLength()) {
																strcpy_s(org.org.szOrgName, sizeof(org.org.szOrgName),
																	doc["list"][i]["orgName"].GetString());
															}
														}
														if (doc["list"][i].HasMember("parentId")) {
															if (doc["list"][i]["parentId"].IsString()
																&& doc["list"][i]["parentId"].GetStringLength()) {
																strcpy_s(org.org.szParentOrgId, sizeof(org.org.szParentOrgId),
																	doc["list"][i]["parentId"].GetString());
															}
														}
														if (strlen(org.org.szOrgId)) {
															std::string strOrgId = org.org.szOrgId;
															if (g_orgList2.empty()) {
																OrganizationEx * pOrg = new OrganizationEx();
																memcpy_s(&pOrg->org, nOrgSize, &org.org, nOrgSize);
																g_orgList2.emplace(strOrgId, pOrg);
															}
															else {
																OrgList::iterator iter = g_orgList2.find(strOrgId);
																if (iter == g_orgList2.end()) {
																	OrganizationEx * pOrg = new OrganizationEx();
																	memcpy_s(&pOrg->org, nOrgSize, &org.org, nOrgSize);
																	g_orgList2.emplace(strOrgId, pOrg);
																}
															}
														}
													}
													pthread_mutex_unlock(&g_mutex4OrgList);
													loadOrgList();
													setLoadOrg(true);
													break;
												}
												case BUFFER_GUARDER: {
													size_t nGuarderSize = sizeof(Guarder);
													pthread_mutex_lock(&g_mutex4GuarderList);
													for (int i = 0; i < nCount; i++) {
														auto pGuarder = (Guarder *)zmalloc(nGuarderSize);
														memset(pGuarder, 0, nGuarderSize);
														if (doc["list"][i].HasMember("id")) {
															if (doc["list"][i]["id"].IsString()
																&& doc["list"][i]["id"].GetStringLength()) {
																strcpy_s(pGuarder->szId, sizeof(pGuarder->szId),
																	doc["list"][i]["id"].GetString());
															}
														}
														if (doc["list"][i].HasMember("name")) {
															if (doc["list"][i]["name"].IsString()
																&& doc["list"][i]["name"].GetStringLength()) {
																strcpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
																	doc["list"][i]["name"].GetString());
															}
														}
														if (doc["list"][i].HasMember("password")) {
															if (doc["list"][i]["password"].IsString()
																&& doc["list"][i]["password"].GetStringLength()) {
																strcpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
																	doc["list"][i]["password"].GetString());
															}
														}
														if (doc["list"][i].HasMember("org")) {
															if (doc["list"][i]["org"].IsString()
																&& doc["list"][i]["org"].GetStringLength()) {
																strcpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg),
																	doc["list"][i]["org"].GetString());
															}
														}
														if (doc["list"][i].HasMember("roleType")) {
															if (doc["list"][i]["roleType"].IsInt()) {
																pGuarder->usRoleType = 
																	(unsigned short)doc["list"][i]["roleType"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("session")) {
															if (doc["list"][i]["session"].IsString()
																&& doc["list"][i]["session"].GetStringLength()) {
																strcpy_s(pGuarder->szCurrentSession, 
																	sizeof(pGuarder->szCurrentSession),
																	doc["list"][i]["session"].GetString());
															}
														}
														if (doc["list"][i].HasMember("phone")) {
															if (doc["list"][i]["phone"].IsString()
																&& doc["list"][i]["phone"].GetStringLength()) {
																strcpy_s(pGuarder->szPhoneCode, sizeof(pGuarder->szPhoneCode),
																	doc["list"][i]["phone"].GetString());
															}
														}
														if (doc["list"][i].HasMember("state")) {
															if (doc["list"][i]["state"].IsInt()) {
																pGuarder->usState = (unsigned short)doc["list"][i]["state"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("device")) {
															if (doc["list"][i]["device"].IsString()
																&& doc["list"][i]["device"].GetStringLength()) {
																strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice),
																	doc["list"][i]["device"].GetString());
															}
														}
														if (doc["list"][i].HasMember("taskId")) {
															if (doc["list"][i]["taskId"].IsString() && doc["list"][i]["taskId"].GetStringLength()) {
																strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId),
																	doc["list"][i]["taskId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("loginFormat")) {
															if (doc["list"][i]["loginFormat"].IsUint()) {
																pGuarder->usLoginFormat = (unsigned short)doc["list"][i]["loginFormat"].GetUint();
															}
														}
														if (doc["list"][i].HasMember("activeTime")) {
															if (doc["list"][i]["activeTime"].IsUint64()) {
																pGuarder->ullActiveTime = doc["list"][i]["activeTime"].GetUint64();
															}
														}
														if (strlen(pGuarder->szId) && strlen(pGuarder->szOrg)) {
															zhash_update(g_guarderList, pGuarder->szId, pGuarder);
															zhash_freefn(g_guarderList, pGuarder->szId, free);
														}
														else {
															free(pGuarder);
															pGuarder = NULL;
														}
													}
													pthread_mutex_unlock(&g_mutex4GuarderList);
													setLoadUser(true);
													break;
												}
												case BUFFER_DEVICE: {
													size_t nDeviceSize = sizeof(WristletDevice);
													pthread_mutex_lock(&g_mutex4DevList);
													for (int i = 0; i < nCount; i++) {
														auto pDevice = (WristletDevice *)zmalloc(nDeviceSize);
														memset(pDevice, 0, nDeviceSize);
														if (doc["list"][i].HasMember("factoryId")) {
															if (doc["list"][i]["factoryId"].IsString() 
																&& doc["list"][i]["factoryId"].GetStringLength()) {
																strcpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
																	doc["list"][i]["factoryId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("deviceId")) {
															if (doc["list"][i]["deviceId"].IsString()
																&& doc["list"][i]["deviceId"].GetStringLength()) {
																strcpy_s(pDevice->deviceBasic.szDeviceId, sizeof(pDevice->deviceBasic.szDeviceId),
																	doc["list"][i]["deviceId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("org")) {
															if (doc["list"][i]["org"].IsString()
																&& doc["list"][i]["org"].GetStringLength()) {
																strcpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
																	doc["list"][i]["org"].GetString());
															}
														}
														if (doc["list"][i].HasMember("status")) {
															if (doc["list"][i]["status"].IsInt()) {
																pDevice->deviceBasic.nStatus = (unsigned short)doc["list"][i]["status"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("battery")) {
															if (doc["list"][i]["battery"].IsInt()) {
																pDevice->deviceBasic.nBattery = (unsigned short)doc["list"][i]["battery"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("loose")) {
															if (doc["list"][i]["loose"].IsInt()) {
																pDevice->deviceBasic.nLooseStatus = (unsigned short)doc["list"][i]["loose"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("online")) {
															if (doc["list"][i]["online"].IsInt()) {
																pDevice->deviceBasic.nOnline = (unsigned short)doc["list"][i]["online"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("lastActiveTime")) {
															if (doc["list"][i]["lastActiveTime"].IsUint64()) {
																pDevice->deviceBasic.ulLastActiveTime = doc["list"][i]["lastActiveTime"].GetUint64();
															}
														}
														if (doc["list"][i].HasMember("imei")) {
															if (doc["list"][i]["imei"].IsString() && doc["list"][i]["imei"].GetStringLength()) {
																strcpy_s(pDevice->deviceBasic.szDeviceImei, sizeof(pDevice->deviceBasic.szDeviceImei),
																	doc["list"][i]["imei"].GetString());
															}
														}
														if (doc["list"][i].HasMember("mnc")) {
															if (doc["list"][i]["mnc"].IsInt()) {
																pDevice->deviceBasic.nDeviceMnc = doc["list"][i]["mnc"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("link")) {
															if (doc["list"][i]["link"].IsString() && doc["list"][i]["link"].GetStringLength()) {
																strcpy_s(pDevice->szLinkId, sizeof(pDevice->szLinkId), doc["list"][i]["link"].GetString());
															}
														}
														if (doc["list"][i].HasMember("lastLocateType")) {
															if (doc["list"][i]["lastLocateType"].IsInt()) {
																pDevice->nLastLocateType = doc["list"][i]["lastLocateType"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("lastDeviceLocateTime")) {
															if (doc["list"][i]["lastDeviceLocateTime"].IsUint64()) {
																pDevice->ulLastDeviceLocateTime = doc["list"][i]["lastDeviceLocateTime"].GetUint64();
															}
														}
														if (doc["list"][i].HasMember("lastGuarderLocateTime")) {
															if (doc["list"][i]["lastGuarderLocateTime"].IsUint64()) {
																pDevice->ulLastGuarderLocateTime =
																	doc["list"][i]["lastGuarderLocateTime"].GetUint64();
															}
														}
														if (doc["list"][i].HasMember("bindGuarder")) {
															if (doc["list"][i]["bindGuarder"].IsString()
																&& doc["list"][i]["bindGuarder"].GetStringLength()) {
																strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard),
																	doc["list"][i]["bindGuarder"].GetString());
															}
														}
														if (doc["list"][i].HasMember("devLat")) {
															if (doc["list"][i]["devLat"].IsDouble()) {
																pDevice->devicePosition.dLatitude
																	= doc["list"][i]["devLat"].GetDouble();
															}
														}
														if (doc["list"][i].HasMember("devLng")) {
															if (doc["list"][i]["devLng"].IsDouble()) {
																pDevice->devicePosition.dLngitude
																	= doc["list"][i]["devLng"].GetDouble();
															}
														}
														if (doc["list"][i].HasMember("devCoordinate")) {
															if (doc["list"][i]["devCoordinate"].IsDouble()) {
																pDevice->devicePosition.nCoordinate
																	= doc["list"][i]["devCoordinate"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("guarderLat")) {
															if (doc["list"][i]["guarderLat"].IsDouble()) {
																pDevice->guardPosition.dLatitude =
																	doc["list"][i]["guarderLat"].GetDouble();
															}
														}
														if (doc["list"][i].HasMember("guarderLng")) {
															if (doc["list"][i]["guarderLng"].IsDouble()) {
																pDevice->guardPosition.dLngitude =
																	doc["list"][i]["guarderLng"].GetDouble();
															}
														}
														if (doc["list"][i].HasMember("guarderCoordinate")) {
															if (doc["list"][i]["guarderCoordinate"].IsInt()) {
																pDevice->guardPosition.nCoordinate
																	= doc["list"][i]["guarderCoordinate"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("fenceHave")) {
															if (doc["list"][i]["fenceHave"].IsInt()) {
																pDevice->nDeviceHasFence
																	= doc["list"][i]["fenceHave"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("fenceAlarm")) {
															if (doc["list"][i]["fenceAlarm"].IsInt()) {
																pDevice->nDeviceFenceState = doc["list"][i]["fenceAlarm"].GetInt();
															}
														}
														if (doc["list"][i].HasMember("charge")) {
															if (doc["list"][i]["charge"].IsInt()) {
																pDevice->nDeviceInCharge = doc["list"][i]["charge"].GetInt();
															}
														}
														if (strlen(pDevice->deviceBasic.szDeviceId)) {
															zhash_update(g_deviceList, pDevice->deviceBasic.szDeviceId, pDevice);
															zhash_freefn(g_deviceList, pDevice->deviceBasic.szDeviceId, free);
														}
														else {
															free(pDevice);
															pDevice = NULL;
														}
													}
													pthread_mutex_unlock(&g_mutex4DevList);
													setLoadDevice(true);
													break;
												}
												case BUFFER_TASK: {
													size_t nTaskSize = sizeof(EscortTask);
													for (int i = 0; i < nCount; i++) {
														auto pTask = (EscortTask *)zmalloc(nTaskSize);
														memset(pTask, 0, nTaskSize);
														int nFlee = 0;
														char szGuarder[20] = { 0 };
														char szDeviceId[16] = { 0 };
														if (doc["list"][i].HasMember("taskId")) {
															if (doc["list"][i]["taskId"].IsString() && doc["list"][i]["taskId"].GetStringLength()) {
																strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), doc["list"][i]["taskId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("taskType")) {
															if (doc["list"][i]["taskType"].IsUint()) {
																pTask->nTaskType = (uint8_t)doc["list"][i]["taskType"].GetUint();
															}
														}
														if (doc["list"][i].HasMember("taskLimit")) {
															if (doc["list"][i]["taskLimit"].IsUint()) {
																pTask->nTaskLimitDistance = (uint8_t)doc["list"][i]["taskLimit"].GetUint();
															}
														}
														if (doc["list"][i].HasMember("taskFlee")) {
															if (doc["list"][i]["taskFlee"].IsUint()) {
																pTask->nTaskFlee = (uint8_t)doc["list"][i]["taskFlee"].GetUint();
																nFlee = pTask->nTaskFlee;
															}
														}
														if (doc["list"][i].HasMember("guarder")) {
															if (doc["list"][i]["guarder"].IsString() && doc["list"][i]["guarder"].GetStringLength()) {
																strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), doc["list"][i]["guarder"].GetString());
																strcpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder);
															}
														}
														if (doc["list"][i].HasMember("deviceId")) {
															if (doc["list"][i]["deviceId"].IsString() && doc["list"][i]["deviceId"].GetStringLength()) {
																strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), doc["list"][i]["deviceId"].GetString());
																strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
															}
														}
														if (doc["list"][i].HasMember("factoryId")) {
															if (doc["list"][i]["factoryId"].IsString() && doc["list"][i]["factoryId"].GetStringLength()) {
																strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), doc["list"][i]["factoryId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("orgId")) {
															if (doc["list"][i]["orgId"].IsString() && doc["list"][i]["orgId"].GetStringLength()) {
																strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), doc["list"][i]["orgId"].GetString());
															}
														}
														if (doc["list"][i].HasMember("taskStartTime")) {
															if (doc["list"][i]["taskStartTime"].IsString() && doc["list"][i]["taskStartTime"].GetStringLength()) {
																strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime),
																	doc["list"][i]["taskStartTime"].GetString());
															}
														}
														if (doc["list"][i].HasMember("target")) {
															if (doc["list"][i]["target"].IsString() && doc["list"][i]["target"].GetStringLength()) {
																strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), doc["list"][i]["target"].GetString());
															}
														}
														if (doc["list"][i].HasMember("destination")) {
															if (doc["list"][i]["destination"].IsString() && doc["list"][i]["destination"].GetStringLength()) {
																strcpy_s(pTask->szDestination, sizeof(pTask->szDestination),
																	doc["list"][i]["destination"].GetString());
															}
														}
														if (doc["list"][i].HasMember("handset")) {
															if (doc["list"][i]["handset"].IsString() && doc["list"][i]["handset"].GetStringLength()) {
																strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), doc["list"][i]["handset"].GetString());
															}
														}
														if (doc["list"][i].HasMember("taskMode")) {
															if (doc["list"][i]["taskMode"].IsInt()) {
																pTask->nTaskMode = doc["list"][i]["taskMode"].GetInt();
															}
														}
														if (strlen(pTask->szTaskId)) {
															pthread_mutex_lock(&g_mutex4TaskList);
															zhash_update(g_taskList, pTask->szTaskId, pTask);
															zhash_freefn(g_taskList, pTask->szTaskId, free);
															pthread_mutex_unlock(&g_mutex4TaskList);
															if (strlen(szGuarder)) {
																pthread_mutex_lock(&g_mutex4GuarderList);
																auto pGuarder = (Guarder *)zhash_lookup(g_guarderList,
																	szGuarder);
																if (pGuarder) {
																	pGuarder->usState = STATE_GUARDER_DUTY;
																	strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pTask->szTaskId);
																}
																pthread_mutex_unlock(&g_mutex4GuarderList);
															}
															if (strlen(szDeviceId)) {
																pthread_mutex_lock(&g_mutex4DevList);
																auto pDevice = (WristletDevice *)zhash_lookup(
																	g_deviceList, szDeviceId);
																if (pDevice) {
																	strcpy_s(pDevice->szBindGuard,
																		sizeof(pDevice->szBindGuard),
																		szGuarder);
																	if (nFlee == 0) {
																		pDevice->deviceBasic.nStatus = DEV_GUARD;
																	}
																	else {
																		pDevice->deviceBasic.nStatus = DEV_FLEE;
																	}
																	if (pDevice->deviceBasic.nLooseStatus == 1) {
																		pDevice->deviceBasic.nStatus += DEV_LOOSE;
																	}
																	if (pDevice->deviceBasic.nBattery < 20) {
																		pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
																	}
																}
																pthread_mutex_unlock(&g_mutex4DevList);
															}
															sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]load taskId=%s, guarder=%s, deviceId=%s,"
																" startTime=%s, handset=%s, flee=%d\n", __FUNCTION__, __LINE__, pTask->szTaskId, pTask->szGuarder,
																pTask->szDeviceId, pTask->szTaskStartTime, pTask->szHandset, pTask->nTaskFlee);
															LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
														}
														else {
															free(pTask);
															pTask = NULL;
														}
														setLoadTask(true);
													}
													break;
												}
												case BUFFER_PERSON: {
													bool bMatch = false;
													unsigned short usFormat = 0;
													char szLink[40] = { 0 };
													char szEndpoint[32] = { 0 };
													char szSession[20] = { 0 };
													std::lock_guard<std::mutex> lock(m_mutex4QryPersonEventList);
													QueryPersonEventList::iterator iter = m_qryPersonEventList.find(nSequence);
													if (iter != m_qryPersonEventList.end()) {
														access_service::QueryPersonEvent * pEvent = iter->second;
														if (pEvent) {
															bMatch = true;
															usFormat = pEvent->usFormat;
															strcpy_s(szLink, sizeof(szLink), pEvent->szLink);
															strcpy_s(szSession, sizeof(szSession), pEvent->szSession);
															strcpy_s(szEndpoint, sizeof(szEndpoint), pEvent->szEndpoint);
															delete pEvent;
															pEvent = NULL;
														}
														m_qryPersonEventList.erase(iter);
													}
													if (bMatch) {
														std::string strPersonList;
														for (int i = 0; i < nCount; i++) {
															char szCell[128] = { 0 };
															sprintf_s(szCell, sizeof(szCell), "{\"id\":\"%s\",\"name\":\"%s\",\"state\":%d}",
																doc["list"][i]["id"].GetString(), doc["list"][i]["name"].GetString(),
																doc["list"][i]["state"].GetInt());
															if (i == 0) {
																strPersonList = szCell;
															}
															else {
																strPersonList = strPersonList + "," + (std::string)szCell;
															}
														}
														size_t nReplyLen = 256 + strPersonList.size();
														char * pReply = new char[nReplyLen];
														sprintf_s(pReply, nReplyLen, "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%u,\"datetime\":\"%s\","
															"\"count\":%d,\"personList\":[%s]}", access_service::E_CMD_QUERY_PERSON_REPLY, szSession,
															nSequence, szDatetime, nCount, strPersonList.c_str());
														if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
															if (strlen(szEndpoint)) {
																sendDataToEndpoint_v2(pReply, (uint32_t)strlen(pReply), szEndpoint,
																	access_service::E_ACC_DATA_TRANSFER, szLink);
															}
														}
														else if (usFormat == LOGIN_FORMAT_MQ) {
															if (strlen(szLink)) {
																sendExpressMessageToClient(pReply, szLink);
															}
														}
														delete[] pReply;
														pReply = NULL;
													}
													break;
												}
												default: {
													break;
												}
											}
											
										}
									}
									break;
								}
								default: {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message "
										"failed, unsupport type: %d\r\n", __FUNCTION__, __LINE__, nType);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									break;
								}
							}
						}
						else {
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed,"
								" json data miss parameter, type=%d, sequence=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, bValidType ? nType : -1, bValidSeq ? nSequence : -1, 
								bValidTime ? szDatetime : "null");
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed,"
							" verify message head failed\r\n", __FUNCTION__, __LINE__);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
				}
				else {
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]parse interaction message failed, "
						"JSON data parse error: %s\r\n", __FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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

int AccessService::handleTopicDeviceOnlineMsg(TopicOnlineMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[512] = { 0 };
	if (pMsg) {
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic online msg, topic=%s, deviceId=%s\r\n",
			__FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		unsigned short usBattery = 0;
		unsigned short usStatus = 0;
		int nCoordinate = 0;
		if (strlen(pMsg->szDeviceId)) {
			char szGuarder[20] = { 0 };
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pMsg->usBattery > 0) {
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					pDev->deviceBasic.nOnline = 1;
					if (strlen(pDev->szBindGuard)) {
						strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
					}
					nCoordinate = pDev->devicePosition.nCoordinate;
					usStatus = pDev->deviceBasic.nStatus;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
			bool bFindSub = false;
			char szSession[20] = { 0 };
			char szEndpoint[32] = { 0 };
			char szAccFrom[40] = { 0 };
			unsigned short usLoginFormat = 0;
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscribeInfo * pSubInfo;
				pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
				if (pSubInfo) {
					bFindSub = true;
					strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
					strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
					strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
					usLoginFormat = pSubInfo->nFormat;
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[256] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
					"\"battery\":%u,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
					szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg->szDeviceId, usBattery, usStatus, szDatetime);
				if (usLoginFormat == LOGIN_FORMAT_TCP_SOCKET) {
					if (strlen(szEndpoint)) {
						int nRetVal = sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint,
							access_service::E_ACC_DATA_DISPATCH, szAccFrom);
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s, "
							"battery=%u, status=%u, datetime=%s, ret=%d, notify device online\n", __FUNCTION__, __LINE__, 
							szEndpoint, szSession, pMsg->szDeviceId, usBattery, usStatus, szDatetime, nRetVal);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
				}
				else if (usLoginFormat == LOGIN_FORMAT_MQ) {
					if (strlen(szAccFrom)) {
						sendExpressMessageToClient(szMsg, szAccFrom);
					}
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send to %s, session=%s, deviceId=%s, battery=%hu,"
						" status=%hu, online=1\n", __FUNCTION__, __LINE__, szAccFrom, szSession, pMsg->szDeviceId, usBattery,
						usStatus);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicDeviceOfflineMsg(TopicOfflineMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[512] = { 0 };
	if (pMsg) {
		unsigned short usStatus = 0;
		unsigned short usBattery = 0;
		if (strlen(pMsg->szDeviceId)) {
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic offline msg for %s, dev=%s\r\n",
				__FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nOnline == 1) {
					pDev->deviceBasic.nOnline = 0;
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					result = E_OK;
				}
				usStatus = pDev->deviceBasic.nStatus;
				usBattery = pDev->deviceBasic.nBattery;
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
				char szSession[20] = { 0 };
				char szEndpoint[32] = { 0 };
				char szAccFrom[40] = { 0 };
				char szDatetime[20] = { 0 };
				unsigned short usFormat = 0;
				formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
						strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
						strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
						usFormat = pSubInfo->nFormat;
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				bool bSend = false;
				char szMsg[256] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
					"\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession, access_service::E_NOTIFY_DEVICE_OFFLINE,
					pMsg->szDeviceId, szDatetime);
				if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
					if (strlen(szEndpoint)) {
						if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, 
							access_service::E_ACC_DATA_DISPATCH, szAccFrom) == 0) {
							bSend = true;
						}
					}
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send offline, session=%s, deviceId=%s, datetime=%s, "
						"ret=%d\r\n", __FUNCTION__, __LINE__, szSession, pMsg->szDeviceId, szDatetime, bSend ? 0 : -1);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					if (strlen(szSession)) {
						pthread_mutex_lock(&m_mutex4LinkList);
						auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
						if (pLink) {
							if (bSend) {
								pLink->nNotifyOffline = 0;
							}
							else {
								pLink->nNotifyOffline = 1;
							}
							pLink->nNotifyStatus = 0;
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
					}
				}
				else {
					if (strlen(szAccFrom)) {
						sendExpressMessageToClient(szMsg, szAccFrom);
					}
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]send offline to %s, deviceId=%s, status=%hu, battery=%hu\n",
						__FUNCTION__, __LINE__, szAccFrom, pMsg->szDeviceId, usStatus, usBattery);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicDeviceAliveMsg(TopicAliveMessage * pMsg, const char * pMsgSubstitle)
{
	int result = E_DEFAULTERROR;
	char szLog[256] = { 0 };
	if (pMsg) {
		bool bValidMsg = false;
		unsigned short usBattery = 0;
		unsigned short usStatus = 0;
		if (strlen(pMsg->szDeviceId) > 0) {
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					pDev->deviceBasic.nBattery = pMsg->usBattery;
					usBattery = pDev->deviceBasic.nBattery;
					usStatus = pDev->deviceBasic.nStatus;
					bValidMsg = true;
					if (pDev->deviceBasic.nOnline == 0) {
						pDev->deviceBasic.nOnline = 1;
					}
					if (pDev->deviceBasic.nBattery > BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					else {//lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle) > 0) {
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
			char szSession[20] = { 0 };
			char szEndpoint[32] = { 0 };
			char szAccFrom[40] = { 0 };
			unsigned short usFormat = 0;
			bool bFindSub = false;
			unsigned short usBattery = 0;
			unsigned short usStatus = 0;
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				access_service::AppSubscribeInfo * pSubInfo = NULL;
				pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
				if (pSubInfo) {
					bFindSub = true;
					strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
					strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
					strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
					usFormat = pSubInfo->nFormat;
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
				"\"battery\":%d,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
				szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg->szDeviceId, usBattery, usStatus, szDatetime);
			if (bFindSub) {
				if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
					bool bSend = false;
					if (strlen(szEndpoint)) {
						if (strlen(szEndpoint)) {
							if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint,
								access_service::E_ACC_DATA_DISPATCH, szAccFrom) == 0) {
								bSend = true;
							}
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send to Endpoint=%s, session=%s, device=%s, "
							"battery=%u, status=%u, datetime=%s, ret=%d, notify device battery\n", __FUNCTION__, __LINE__, 
							szEndpoint, szSession, pMsg->szDeviceId, usBattery, usStatus, szDatetime, (bSend ? 0 : -1));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
					if (strlen(szSession)) {
						pthread_mutex_lock(&m_mutex4LinkList);
						if (zhash_size(m_linkList)) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
								m_linkList, szSession);
							if (pLink) {
								pLink->nNotifyOffline = 0;
								if (bSend) {
									pLink->nNotifyStatus = 0;
								}
								else {
									pLink->nNotifyStatus = 1;
								}
							}
						}
						pthread_mutex_unlock(&m_mutex4LinkList);
					}
				}
				else if (usFormat == LOGIN_FORMAT_MQ) {
					if (strlen(szAccFrom)) {
						sendExpressMessageToClient(szMsg, szAccFrom);
					}
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device alive to %s, session=%s, device=%s,"
						" battery=%u, status=%u, datetime=%s\n", __FUNCTION__, __LINE__, szAccFrom, szSession, pMsg->szDeviceId,
						usBattery, usStatus, szDatetime);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
	if (pMsg) {
		if (strlen(pMsg->szDeviceId)) {
			bool bValidMsg = false;
			unsigned short usBattery = 0;
			unsigned short usStatus = 0;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					if (pDev->deviceBasic.nOnline == 0) {
						pDev->deviceBasic.nOnline = 1;
					}
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD || 
						(pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						bValidMsg = true;
					}
					if (pDev->ulLastDeviceLocateTime <= pMsg->ulMessageTime) {
						if (pMsg->dLat > 0.00 && pMsg->dLng > 0.00) {
							pDev->devicePosition.dLatitude = pMsg->dLat;
							pDev->devicePosition.dLngitude = pMsg->dLng;
							pDev->devicePosition.usLatType = pMsg->usLatType;
							pDev->devicePosition.usLngType = pMsg->usLngType;
							pDev->devicePosition.nCoordinate = pMsg->nCoordinate;
						}
						pDev->deviceBasic.nBattery = pMsg->usBattery;
					}
					usBattery = pDev->deviceBasic.nBattery;
					if (pDev->deviceBasic.nBattery > BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					else {//lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					usStatus = pDev->deviceBasic.nStatus;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[20] = { 0 };
				formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[32] = { 0 };
				char szAccFrom[40] = { 0 };
				unsigned short usFormat = 0;
				bool bFindSub = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
						bFindSub = true;
						strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
						strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
						usFormat = pSubInfo->nFormat;
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				bool bSend = false;
				if (bFindSub) {
					char szMsg[256] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
						"\"battery\":%u,\"status\":%u,\"lat\":%f,\"lng\":%f,\"coordinate\":%d,\"datetime\":\"%s\"}",
						access_service::E_CMD_MSG_NOTIFY, szSession, access_service::E_NOTIFY_DEVICE_POSITION,
						pMsg->szDeviceId, usBattery, usStatus, pMsg->dLat, pMsg->dLng, pMsg->nCoordinate, szDatetime);
					if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
						if (strlen(szEndpoint)) {
							if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH, 
								szAccFrom) == 0) {
								bSend = true;
							}
							else {
								pthread_mutex_lock(&m_mutex4SubscribeList);
								auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList,
									pMsgSubstitle);
								if (pSubInfo) {
									pSubInfo->szEndpoint[0] = '\0';
								}
								pthread_mutex_unlock(&m_mutex4SubscribeList);
							}

							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send gps position to Endpoint=%s, session=%s,"
								" deviceId=%s, battery=%u, status=%u, lat=%f, lng=%f, coordinate=%d, datetime=%s, ret=%d\r\n",
								__FUNCTION__, __LINE__, szEndpoint, szSession, pMsg->szDeviceId, usBattery, usStatus, pMsg->dLat,
								pMsg->dLng, COORDINATE_WGS84, szDatetime, (bSend ? 0 : -1));
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						if (strlen(szSession)) {
							pthread_mutex_lock(&m_mutex4LinkList);
							if (zhash_size(m_linkList)) {
								access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
									m_linkList, szSession);
								if (pLink) {
									pLink->nNotifyOffline = 0;
									if (bSend) {
										pLink->nNotifyStatus = 0;
									}
									else {
										pLink->nNotifyStatus = 1;
									}
								}
							}
							pthread_mutex_unlock(&m_mutex4LinkList);
						}
					}
					else if (usFormat == LOGIN_FORMAT_MQ) {
						if (strlen(szAccFrom)) {
							sendExpressMessageToClient(szMsg, szAccFrom);
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send gps position to %s, session=%s, deviceId=%s, "
							"battery=%u, status=%u, lat=%f, lng=%f, coordinate=%d, datetime=%s\n", __FUNCTION__, __LINE__, szAccFrom,
							szSession, pMsg->szDeviceId, usBattery, usStatus, pMsg->dLat, pMsg->dLng, COORDINATE_WGS84, szDatetime);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
	if (pMsg) {
		if (strlen(pMsg->szDeviceId) > 0) {
			unsigned short usBattery = 0;
			unsigned short usStatus = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.ulLastActiveTime < pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					pDev->deviceBasic.nBattery = pMsg->usBattery;
					if (pDev->deviceBasic.nBattery > BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					else {//lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					if (pDev->deviceBasic.nOnline == 0) {
						pDev->deviceBasic.nOnline = 1;
					}
					if ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD || 
						(pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
						bValidMsg = true;
					}
					//this code would open if LBS position is correct 
					if (pDev->ulLastDeviceLocateTime <= pMsg->ulMessageTime) {
						if (pMsg->dLat > 0.00 && pMsg->dLng > 0.00) {
							pDev->devicePosition.dLatitude = pMsg->dLat;
							pDev->devicePosition.dLngitude = pMsg->dLng;
							pDev->devicePosition.usLatType = pMsg->usLatType;
							pDev->devicePosition.usLngType = pMsg->usLngType;
							pDev->devicePosition.nPrecision = pMsg->nPrecision;
							pDev->devicePosition.nCoordinate = pMsg->nCoordinate;
						}
						pDev->ulLastDeviceLocateTime = pMsg->ulMessageTime;
					}
					usBattery = pDev->deviceBasic.nBattery;
					usStatus = pDev->deviceBasic.nStatus;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szSession[20] = { 0 };
				char szEndpoint[32] = { 0 };
				char szDatetime[20] = { 0 };
				char szAccFrom[40] = { 0 };
				unsigned short usFormat = 0;
				formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				bool bFindSub = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscribeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
						strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
						strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
						usFormat = pSubInfo->nFormat;
						bFindSub = true;
						
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				bool bSend = false;
				if (bFindSub) {
					char szMsg[512] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
						"\"battery\":%u,\"status\":%u,\"lat\":%f,\"lng\":%f,\"coordinate\":%d,\"datetime\":\"%s\"}",
						access_service::E_CMD_MSG_NOTIFY, szSession, access_service::E_NOTIFY_DEVICE_POSITION, pMsg->szDeviceId, 
						usBattery, usStatus, pMsg->dLat, pMsg->dLng, pMsg->nCoordinate, szDatetime);
					if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
						if (strlen(szEndpoint)) {
							if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH,
								szAccFrom) == 0) {
								bSend = true;
							}
							else {
								pthread_mutex_lock(&m_mutex4SubscribeList);
								auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
								if (pSubInfo) {
									pSubInfo->szEndpoint[0] = '\0';
								}
								pthread_mutex_unlock(&m_mutex4SubscribeList);
							}
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send lbs position to Endpoint=%s, session=%s, "
								"deviceId=%s, battery=%u, status=%u, lat=%f, lng=%f, coordinate=%d, datetime=%s, ret=%d\n",
								__FUNCTION__, __LINE__, szEndpoint, szSession, pMsg->szDeviceId, usBattery, usStatus, pMsg->dLat,
								pMsg->dLng, pMsg->nCoordinate, szDatetime, (bSend ? 0 : -1));
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						if (strlen(szSession)) {
							pthread_mutex_lock(&m_mutex4LinkList);
							if (zhash_size(m_linkList)) {
								auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
								if (pLink) {
									pLink->nNotifyOffline = 0;
									if (bSend) {
										pLink->nNotifyStatus = 0;
									}
									else {
										pLink->nNotifyStatus = 1;
									}
								}
							}
							pthread_mutex_unlock(&m_mutex4LinkList);
						}
					}
					else if (usFormat == LOGIN_FORMAT_MQ) {
						if (strlen(szAccFrom)) {
							sendExpressMessageToClient(szMsg, szAccFrom);
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send lbs position %s, session=%s, deviceId=%s, "
							"battery=%u, status=%u, lat=%f, lng=%f, coordinate=%d, datetime=%s\n", __FUNCTION__, __LINE__,
							szAccFrom, szSession, pMsg->szDeviceId, usBattery, usStatus, pMsg->dLat, pMsg->dLng, pMsg->nCoordinate,
							szDatetime);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
			unsigned short usStatus = 0;
			bool bNotifyMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nOnline == 0) {
					pDev->deviceBasic.nOnline = 1;
				}
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					pDev->deviceBasic.nBattery = pMsg->usBattery;
					usBattery = pDev->deviceBasic.nBattery;
					if (pMsg->usMode == 0) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					else {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					usStatus = pDev->deviceBasic.nStatus;
					bNotifyMsg = true;
					result = E_OK;
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bNotifyMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[20] = { 0 };
				formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[32] = { 0 };
				char szAccFrom[40] = { 0 };
				unsigned short usFormat = 0;
				bool bFindSub = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					access_service::AppSubscribeInfo * pSubInfo = NULL;
					pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bFindSub = true;
						strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
						strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
						strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
						usFormat = pSubInfo->nFormat;
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				bool bSend = false;
				if (bFindSub) {
					char szMsg[256] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
						"\"battery\":%u,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, 
						szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg->szDeviceId, usBattery, usStatus, szDatetime);
					if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
						if (strlen(szEndpoint) > 0) {
							if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint,
								access_service::E_ACC_DATA_DISPATCH, szAccFrom) == 0) {
								bSend = true;
							}
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device status to Endpoint=%s, session=%s,"
								" deviceId=%s, battery=%d, status=%u, datetime=%s, ret=%d\n", __FUNCTION__, __LINE__, szEndpoint,
								szSession, pMsg->szDeviceId, usBattery, usStatus, szDatetime, (bSend ? 0 : -1));
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						if (strlen(szSession)) {
							pthread_mutex_lock(&m_mutex4LinkList);
							if (zhash_size(m_linkList)) {
								auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
								if (pLink) {
									pLink->nNotifyOffline = 0;
									if (bSend) {
										pLink->nNotifyStatus = 0;
									}
									else {
										pLink->nNotifyStatus = 1;
									}
								}
							}
							pthread_mutex_unlock(&m_mutex4LinkList);
						}
					}
					else if (usFormat == LOGIN_FORMAT_MQ) {
						if (strlen(szAccFrom)) {
							sendExpressMessageToClient(szMsg, szAccFrom);
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]send device status to %s, session=%s, deviceId=%s, "
							"battery=%u, status=%u, datetime=%s\n", __FUNCTION__, __LINE__, szAccFrom, szSession, pMsg->szDeviceId,
							usBattery, usStatus, szDatetime);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]deal topic loose msg for %s, dev=%s, "
				"mode=%hu\r\n", __FUNCTION__, __LINE__, pMsgSubstitle, pMsg->szDeviceId, pMsg->usMode);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			unsigned short usBattery = 0;
			unsigned short usStatus = 0;
			bool bValidMsg = false;
			pthread_mutex_lock(&g_mutex4DevList);
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nOnline == 0) {
					pDev->deviceBasic.nOnline = 1;
				}
				if (pDev->deviceBasic.ulLastActiveTime <= pMsg->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pMsg->ulMessageTime;
					pDev->deviceBasic.nBattery = pMsg->usBattery;
					if (pDev->deviceBasic.nBattery > BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					else {//lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					result = E_OK;
				}
				if (pMsg->usMode == 0) {//loose
					if (pDev->deviceBasic.nLooseStatus == 0) {
						pDev->deviceBasic.nLooseStatus = 1;
					}
					if ((pDev->deviceBasic.nStatus & DEV_LOOSE) == 0) {
						pDev->deviceBasic.nStatus += DEV_LOOSE;
					}
					bValidMsg = true;
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]change status: loose, subscriber=%s\r\n",
						__FUNCTION__, __LINE__, pMsgSubstitle);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				else {//loose revoke
					if (pDev->deviceBasic.nLooseStatus == 1) {
						pDev->deviceBasic.nLooseStatus = 0;
					}
					if ((pDev->deviceBasic.nStatus & DEV_LOOSE) == DEV_LOOSE) {
						pDev->deviceBasic.nStatus -= DEV_LOOSE;
					}
					bValidMsg = true;
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]change status: loose revoke, "
						"subscriber=%s\r\n", __FUNCTION__, __LINE__, pMsgSubstitle);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				usBattery = pDev->deviceBasic.nBattery;
				usStatus = pDev->deviceBasic.nStatus;
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			if (bValidMsg && pMsgSubstitle && strlen(pMsgSubstitle)) {
				char szDatetime[20] = { 0 };
				formatDatetime(pMsg->ulMessageTime, szDatetime, sizeof(szDatetime));
				char szSession[20] = { 0 };
				char szEndpoint[32] = { 0 };
				char szAccFrom[40] = { 0 };
				unsigned short usFormat = 0;
				bool bNotifyMsg = false;
				pthread_mutex_lock(&m_mutex4SubscribeList);
				if (zhash_size(m_subscribeList)) {
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, pMsgSubstitle);
					if (pSubInfo) {
						bNotifyMsg = true;
						strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
						strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
						strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
						usFormat = pSubInfo->nFormat;
					}
				}
				pthread_mutex_unlock(&m_mutex4SubscribeList);
				bool bSend = false;
				if (bNotifyMsg) {
					char szMsg[256] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
						"\"battery\":%u,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
						szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg->szDeviceId, usBattery, usStatus, szDatetime);
					if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
						if (strlen(szEndpoint)) {
							if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH,
								szAccFrom) == 0) {
								bSend = true;
							}
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device status to Endpoint=%s, session=%s, "
							"deviceId=%s, battery=%u, status=%u, datetime=%s, ret=%d\n", __FUNCTION__, __LINE__, szEndpoint,
							szSession, pMsg->szDeviceId, usBattery, usStatus, szDatetime, (bSend ? 0 : -1));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						if (strlen(szSession)) {
							pthread_mutex_lock(&m_mutex4LinkList);
							auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
							if (pLink) {
								pLink->nNotifyOffline = 0;
								if (bSend) {
									pLink->nNotifyStatus = 0;
								}
								else {
									pLink->nNotifyStatus = 1;
								}
							}
							pthread_mutex_unlock(&m_mutex4LinkList);
						}
					}
					else if (usFormat == LOGIN_FORMAT_MQ) {
						if (strlen(szAccFrom)) {
							sendExpressMessageToClient(szMsg, szAccFrom);
						}
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device status to %s, session=%s, deviceId=%s, "
							"battery=%u, status=%u, datetime=%s\n", __FUNCTION__, __LINE__, szAccFrom, szSession, pMsg->szDeviceId,
							usBattery, usStatus, szDatetime);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]deal Topic alarm flee for %s, dev=%s, mode=%hu\n",
			__FUNCTION__, __LINE__, pMsgTopic_, pMsg_->szDeviceId, pMsg_->usMode);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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

int AccessService::handleTopicAlarmLocateLostMsg(TopicAlarmMessageLocateLost * pMsg_, 
	const char * pMsgTopic_)
{
	int result = E_OK;
	char szLog[1024] = { 0 };
	if (pMsg_) {
		if (strlen(pMsg_->szOrg) && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szFactoryId)) {
			char szTopic[64] = { 0 };
			sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pMsg_->szOrg, pMsg_->szFactoryId, 
				pMsg_->szDeviceId);
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			char szEndpoint[32] = { 0 };
			char szSession[20] = { 0 };
			char szAccFrom[40] = { 0 };
			bool bFindSub = false;
			pthread_mutex_lock(&m_mutex4SubscribeList);
			if (zhash_size(m_subscribeList)) {
				auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szTopic);
				if (pSubInfo) {
					strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
					strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
					strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
					bFindSub = true;
				}
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[256] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\",\"mode\":0,"
					"\"battery\":%d,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, szSession, 
					access_service::E_NOTIFY_DEVICE_LOCATE_LOST, pMsg_->szDeviceId, pMsg_->usDeviceBattery, szDatetime);
				if (strlen(szEndpoint)) {
					if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH,
						szAccFrom) == 0) {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]notify locate lost alarm to endpoint=%s, deviceId=%s, "
							"orgId=%s, battery=%hu, datetime=%s, session=%s\r\n", __FUNCTION__, __LINE__, szEndpoint, pMsg_->szDeviceId,
							pMsg_->szOrg, pMsg_->usDeviceBattery, szDatetime, szSession);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicAlarmFenceMsg(TopicAlarmMessageFence * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		char szLog[512] = { 0 };
		if (strlen(pMsg_->szFactoryId) && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szOrgId)) {
			unsigned short usDeviceBattery = 0;
			unsigned short usStatus = 0;
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
				if (pDev) {
					usDeviceBattery = pDev->deviceBasic.nBattery;
					usStatus = pDev->deviceBasic.nStatus;
					if (pDev->nDeviceFenceState == 0) {
						if (pMsg_->nMode == 0) {
							pDev->nDeviceFenceState = 1;
						}
					}
					else {
						if (pMsg_->nMode == 1) {
							pDev->nDeviceFenceState = 0;
						}
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);
			char szSubTopic[64] = { 0 };
			sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pMsg_->szOrgId, pMsg_->szFactoryId,
				pMsg_->szDeviceId);
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			char szEndpoint[32] = { 0 };
			char szSession[20] = { 0 };
			char szAccFrom[40] = { 0 };
			unsigned short usFormat = 0;
			bool bFindSub = false;
			pthread_mutex_lock(&m_mutex4SubscribeList);
			auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szSubTopic);
			if (pSubInfo) {
				bFindSub = true;
				strcpy_s(szEndpoint, sizeof(szEndpoint), pSubInfo->szEndpoint);
				strcpy_s(szSession, sizeof(szSession), pSubInfo->szSession);
				strcpy_s(szAccFrom, sizeof(szAccFrom), pSubInfo->szAccSource);
				usFormat = pSubInfo->nFormat;
			}
			else {
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]not find subscriber for %s\r\n",
					__FUNCTION__, __LINE__, szSubTopic);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
			pthread_mutex_unlock(&m_mutex4SubscribeList);
			if (bFindSub) {
				char szMsg[512] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
					"\"battery\":%u,\"status\":%u,\"mode\":%d,\"lat\":%f,\"lng\":%f,\"coordinate\":%d,\"datetime\":\"%s\"}",
					access_service::E_CMD_MSG_NOTIFY, szSession, access_service::E_ALARM_DEVICE_FENCE, pMsg_->szDeviceId,
					usDeviceBattery, usStatus, pMsg_->nMode, pMsg_->dLatitude, pMsg_->dLngitude, pMsg_->nCoordinate, szDatetime);
				if (usFormat == LOGIN_FORMAT_TCP_SOCKET) {
					if (strlen(szEndpoint)) {
						bool bSend = false;
						if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH,
							szAccFrom) == 0) {
							bSend = true;
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send fence alarm to endpoint=%s, session=%s, orgId=%s,"
								" deviceId=%s, factoryId=%s, policy=%d, mode=%d, fenceId=%s, fenceTaskId=%s, lat=%f, lng=%f, coordinate=%d,"
								" datetime=%s\r\n", __FUNCTION__, __LINE__, szEndpoint, szSession, pMsg_->szOrgId, pMsg_->szDeviceId,
								pMsg_->szFactoryId, pMsg_->nPolicy, pMsg_->nMode, pMsg_->szFenceId, pMsg_->szFenceTaskId, pMsg_->dLatitude,
								pMsg_->dLngitude, pMsg_->nCoordinate, szDatetime);
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						}
						if (bSend) {
							access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
							if (pLink) {
								if (pLink->nNotifyStatus == 1) {
									sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
										"\"battery\":%d,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
										szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg_->szDeviceId, usDeviceBattery, usStatus, 
										szDatetime);
									if (sendDataToEndpoint_v2(szMsg, (uint32_t)strlen(szMsg), szEndpoint, access_service::E_ACC_DATA_DISPATCH,
										szAccFrom) == 0) {
										pLink->nNotifyStatus = 0;
										sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device status to endpoint=%s,"
											" session=%s, deviceId=%s, battery=%u, status=%u, datetime=%s\n", __FUNCTION__, __LINE__, szEndpoint,
											szSession, pMsg_->szDeviceId, usDeviceBattery, usStatus, szDatetime);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
							}
						}
					}
				}
				else if (usFormat == LOGIN_FORMAT_MQ) {
					if (strlen(szAccFrom)) {
						sendExpressMessageToClient(szMsg, szAccFrom);
						char szDevMsg[256] = { 0 };
						sprintf_s(szDevMsg, sizeof(szDevMsg), "{\"cmd\":%d,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\","
							"\"battery\":%d,\"status\":%u,\"online\":1,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY,
							szSession, access_service::E_NOTIFY_DEVICE_INFO, pMsg_->szDeviceId, usDeviceBattery, usStatus, szDatetime);
						sendExpressMessageToClient(szDevMsg, szAccFrom);
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device status to %s, session=%s, deviceId=%s, "
							"battery=%u, stuatus=%u, datetime=%s\n", __FUNCTION__, __LINE__, szAccFrom, szSession, pMsg_->szDeviceId,
							usDeviceBattery, usStatus, szDatetime);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
				}
			}
		}
	}
	return result;
}

int AccessService::handleTopicTaskSubmitMsg(TopicTaskMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szTaskId) && strlen(pMsg_->szDeviceId) && strlen(pMsg_->szGuarder)) {
			char szEndpoint[40] = { 0 };
			char szSession[40] = { 0 };
			char szAccFrom[40] = { 0 };
			char szDatetime[20] = { 0 };
			formatDatetime(pMsg_->ulMessageTime, szDatetime, sizeof(szDatetime));
			bool bExists = false;
			EscortTask * pTask = (EscortTask *)zmalloc(sizeof(EscortTask));
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = pMsg_->usTaskLimit;
			pTask->nTaskState = 0;
			pTask->nTaskType = pMsg_->usTaskType;
			strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pMsg_->szDestination);
			strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pMsg_->szDeviceId);
			strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pMsg_->szFactoryId);
			strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pMsg_->szGuarder);
			strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
			strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), pMsg_->szOrg);
			strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pMsg_->szTarget);
			strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pMsg_->szTaskId);
			strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), szDatetime);
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
					if (strlen(pGuarder->szLink)) {
						strcpy_s(szEndpoint, sizeof(szEndpoint), pGuarder->szLink);
					}
					if (strlen(pGuarder->szCurrentSession)) {
						strcpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession);
					}
					strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pMsg_->szTaskId);
					pGuarder->usState = STATE_GUARDER_DUTY;
					strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pMsg_->szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
				if (pDevice) {
					changeDeviceStatus(DEV_GUARD, pDevice->deviceBasic.nStatus);
					strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), pMsg_->szGuarder);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			if (strlen(szSession)) {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLinkInfo = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
				if (pLinkInfo) {
					strcpy_s(pLinkInfo->szDeviceId, sizeof(pLinkInfo->szDeviceId), pMsg_->szDeviceId);
					strcpy_s(pLinkInfo->szGuarder, sizeof(pLinkInfo->szGuarder), pMsg_->szGuarder);
					strcpy_s(pLinkInfo->szTaskId, sizeof(pLinkInfo->szTaskId), pMsg_->szTaskId);
					if (pLinkInfo->nActivated == 1) {
						if (strcmp(pLinkInfo->szEndpoint, szEndpoint) != 0) {
							strcpy_s(szEndpoint, sizeof(szEndpoint), pLinkInfo->szEndpoint);
						}
					}
					strcpy_s(pLinkInfo->szOrg, sizeof(pLinkInfo->szOrg), pMsg_->szOrg);
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
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
						strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
						pTask->nTaskMode = 1;
					}
				}
				else {
					if (strlen(pMsg_->szHandset) == 0) {
						pTask->szHandset[0] = '\0';
						pTask->nTaskMode = 0;
					}
					else {
						if (strlen(pMsg_->szHandset) && strcmp(pTask->szHandset, pMsg_->szHandset) != 0) {
							strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pMsg_->szHandset);
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
			char szEndpoint[40] = { 0 };
			char szSession[40] = { 0 };
			pthread_mutex_lock(&g_mutex4TaskList);
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pMsg_->szTaskId);
			if (pTask) {
				strcpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder);
				strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId); 
				zhash_delete(g_taskList, pMsg_->szTaskId);
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
			if (strlen(szGuarder)) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->szTaskId[0] = '\0';
					pGuarder->usState = STATE_GUARDER_FREE;
					if (strlen(pGuarder->szLink)) {
						strcpy_s(szEndpoint, sizeof(szEndpoint), pGuarder->szLink);
					}
					if (strlen(pGuarder->szCurrentSession)) {
						strcpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession);
					}
					pGuarder->szBindDevice[0] = '\0';
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}
			if (strlen(szDeviceId)) {
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					pDevice->deviceBasic.nStatus = DEV_ONLINE;
					if (pDevice->deviceBasic.nBattery < BATTERY_THRESHOLD) {
						pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
					}
					if (pDevice->deviceBasic.nLooseStatus == 1) {
						pDevice->deviceBasic.nStatus += DEV_LOOSE;
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			if (strlen(szSession)) {
				pthread_mutex_lock(&m_mutex4LinkList);
				access_service::AppLinkInfo * pLinkInfo = (access_service::AppLinkInfo *)zhash_lookup(
					m_linkList, szSession);
				if (pLinkInfo) {
					pLinkInfo->szTaskId[0] = '\0';
					pLinkInfo->szDeviceId[0] = '\0';
					pLinkInfo->szFactoryId[0] = '\0';
					pLinkInfo->szOrg[0] = '\0';
					if (pLinkInfo->nActivated) {
						if (strlen(pLinkInfo->szEndpoint) && strcmp(szEndpoint, pLinkInfo->szEndpoint) != 0) {
							strcpy_s(szEndpoint, sizeof(szEndpoint), pLinkInfo->szEndpoint);
						}
					}
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
			}
		}
	}
	return result;
}

int AccessService::handleTopicDeviceBindMsg(TopicBindMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		char szSession[40] = { 0 };
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
				if (strlen(pGuarder->szCurrentSession)) {
					strcpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (strlen(szSession)) {
			pthread_mutex_lock(&m_mutex4LinkList);
			access_service::AppLinkInfo * pLinkInfo = (access_service::AppLinkInfo *)zhash_lookup(
				m_linkList, szSession);
			if (pLinkInfo) {
				if (pMsg_->usMode == 0) {
					strcpy_s(pLinkInfo->szDeviceId, sizeof(pLinkInfo->szDeviceId), pMsg_->szDeviceId);
					strcpy_s(pLinkInfo->szFactoryId, sizeof(pLinkInfo->szFactoryId), pMsg_->szFactoryId);
				}
				else {
					pLinkInfo->szFactoryId[0] = '\0';
					pLinkInfo->szDeviceId[0] = '\0';
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
		}
	}
	return result;
}

int AccessService::handleTopicUserLoginMsg(TopicLoginMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szSession) && strlen(pMsg_->szAccount)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szAccount);
			if (pGuarder) {
				strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), pMsg_->szSession);
				pGuarder->usLoginFormat = pMsg_->usLoginFormat;
				pGuarder->ullActiveTime = time(NULL);
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
	return result;
}

int AccessService::handleTopicUserLogoutMsg(TopicLogoutMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szSession) && strlen(pMsg_->szAccount)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szAccount);
			if (pGuarder) {
				pGuarder->szCurrentSession[0] = '\0';
				pGuarder->ullActiveTime = time(NULL);
				pGuarder->usLoginFormat = 0;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
	return result;
}

int AccessService::handleTopicUserAliveMsg(TopicUserAliveMessage * pMsg_, const char * pMsgTopic_)
{
	int result = E_OK;
	if (pMsg_) {
		if (strlen(pMsg_->szAccount) && strlen(pMsg_->szSession)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pMsg_->szAccount);
			if (pGuarder) {
				if (strcmp(pGuarder->szCurrentSession, pMsg_->szSession) == 0) {
					pGuarder->ullActiveTime = time(NULL);
					pGuarder->usLoginFormat = pMsg_->usFormat;
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	}
	return result;
}

int AccessService::handleTopicDeviceChargeMsg(TopicDeviceChargeMessage * pMsg_, const char * pTopic_)
{
	int result = E_OK;
	if (pMsg_ && strlen(pMsg_->szDeviceId)) {
		pthread_mutex_lock(&g_mutex4DevList);
		auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pMsg_->szDeviceId);
		if (pDevice) {
			pDevice->deviceBasic.ulLastActiveTime = pMsg_->ullMsgTime;
			pDevice->nDeviceInCharge = pMsg_->nState;
		}
		pthread_mutex_unlock(&g_mutex4DevList);
	}
	return result;
}

void AccessService::initZookeeper()
{
	int nTimeout = 60000;
	if (!m_zkHandle) {
		m_zkHandle = zookeeper_init(m_szHost, zk_server_watcher, nTimeout, NULL, this, 0);
	}
	if (m_zkHandle) {
		zoo_acreate(m_zkHandle, "/escort", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 
			zk_escort_create_completion, this);
		zoo_acreate(m_zkHandle, "/escort/access", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 
			zk_access_create_completion, this);
		zoo_acreate(m_zkHandle, "/escort/session", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 
			zk_session_create_completion, this);
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
		int ret = zoo_aset(m_zkHandle, path, pBuf, (int)data_size, -1, zk_access_set_completion, this);
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
		int nBufLen = (int)nSize;
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
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
				m_linkList, pSession_);
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
				sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", pLinkInfo->szGuarder);
				const char * pBuf = szBuf;
				ZKSessionNode * pNode = new ZKSessionNode();
				strcpy_s(pNode->szSession, sizeof(pNode->szSession), pLinkInfo->szSession);
				strcpy_s(pNode->szUserId, sizeof(pNode->szUserId), pLinkInfo->szGuarder);
				pNode->data = this;
				const void * pSessionNode = pNode;
				zoo_acreate(m_zkHandle, szPath, pBuf, 1024, &ZOO_OPEN_ACL_UNSAFE, 0, 
					zk_session_child_create_completion, pSessionNode);
			}
			free(pLinkInfo);
			pLinkInfo = NULL;
		}
	}
}

void AccessService::zkRemoveSession(const char * pSession_)
{
	if (m_zkHandle && pSession_ && strlen(pSession_)) {
		char szUserId[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkList);
		access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(
			m_linkList, pSession_);
		if (pLink) {
			strcpy_s(szUserId, sizeof(szUserId), pLink->szGuarder);
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (strlen(szUserId)) {
			char szFullPath[128] = { 0 };
			sprintf_s(szFullPath, sizeof(szFullPath), "/escort/session/%s", szUserId);
			zoo_adelete(m_zkHandle, szFullPath, -1, zk_session_child_delete_completion, this);
		}
	}
}

void AccessService::zkSetSession(const char * pSession_)
{
	if (pSession_ && strlen(pSession_)) {
		char szUserId[20] = { 0 };
		access_service::AppLinkInfo * pLinkInfo = NULL;
		size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pSession_);
			if (pLink) {
				pLinkInfo = new access_service::AppLinkInfo();
				memcpy_s(pLinkInfo, nLinkInfoSize, pLink, nLinkInfoSize);
				strcpy_s(szUserId, sizeof(szUserId), pLinkInfo->szGuarder);
			}
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (pLinkInfo) {
			if (m_zkHandle) {
				char szBuf[1024] = { 0 };
				memcpy_s(szBuf, 1024, pLinkInfo, nLinkInfoSize);
				char szPath[128] = { 0 };
				sprintf_s(szPath, sizeof(szPath), "/escort/session/%s", szUserId);
				const char * pBuf = szBuf;
				ZKSessionNode * pNode = new ZKSessionNode();
				strncpy_s(pNode->szSession, sizeof(pNode->szSession), pSession_, strlen(pSession_));
				strcpy_s(pNode->szUserId, sizeof(pNode->szUserId), szUserId);
				pNode->data = this;
				const void * pSessionNode = pNode;
				zoo_aset(m_zkHandle, szPath, pBuf, 1024, -1, zk_session_child_set_completion, pSessionNode);
			}
		}
		delete pLinkInfo;
		pLinkInfo = NULL;
	}
}

void AccessService::loadSessionList()
{
	if (m_zkHandle) {
		struct String_vector childrenList;
		zoo_wget_children(m_zkHandle, "/escort/session", zk_session_get_children_watcher, 
			NULL, &childrenList);
		if (childrenList.count > 0) {
			char szLog[512] = { 0 };
			for (int i = 0; i < childrenList.count; i++) {
				char szChildFullPath[128] = { 0 };
				sprintf_s(szChildFullPath, sizeof(szChildFullPath), "/escort/session/%s", 
					childrenList.data[i]);
				char szBuf[512] = { 0 };
				int nBufLen = 512;
				size_t nAppLinkInfoSize = sizeof(access_service::AppLinkInfo);
				Stat stat;
				if (zoo_get(m_zkHandle, szChildFullPath, -1, szBuf, &nBufLen, &stat) == ZOK) {
					auto pLinkInfo = (access_service::AppLinkInfo *)zmalloc(nAppLinkInfoSize);
					memcpy_s(pLinkInfo, nAppLinkInfoSize, szBuf, nAppLinkInfoSize);
					pLinkInfo->nActivated = 0;
					pLinkInfo->szEndpoint[0] = '\0';
					bool bLoad = false;
					char szUser[20] = { 0 };
					char szSession[20] = { 0 };
					strcpy_s(szUser, sizeof(szUser), pLinkInfo->szGuarder);
					strcpy_s(szSession, sizeof(szSession), pLinkInfo->szSession);
					if (strlen(pLinkInfo->szTaskId)) {
						pthread_mutex_lock(&g_mutex4TaskList);
						if (zhash_size(g_taskList)) {
							auto pTask = (EscortTask *)zhash_lookup(g_taskList, pLinkInfo->szTaskId);
							if (!pTask) {
								pLinkInfo->szDeviceId[0] = '\0';
								pLinkInfo->szTaskId[0] = '\0';
								pLinkInfo->szFactoryId[0] = '\0';
								pLinkInfo->szOrg[0] = '\0';
							}
						}
						pthread_mutex_unlock(&g_mutex4TaskList);
					}
					else {
						pLinkInfo->szDeviceId[0] = '\0';
						pLinkInfo->szFactoryId[0] = '\0';
						pLinkInfo->szOrg[0] = '\0';
					}
					
					pthread_mutex_lock(&m_mutex4LinkList);
					zhash_update(m_linkList, pLinkInfo->szSession, pLinkInfo);
					zhash_freefn(m_linkList, pLinkInfo->szSession, free);
					pthread_mutex_unlock(&m_mutex4LinkList);
					sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]load session=%s, guarder=%s, "
						"device=%s, org=%s, taskId=%s, handset=%s\r\n", __FUNCTION__, __LINE__, 
						pLinkInfo->szSession, pLinkInfo->szGuarder, pLinkInfo->szDeviceId, pLinkInfo->szOrg,
						pLinkInfo->szTaskId, pLinkInfo->szHandset);
					LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);

					if (strlen(pLinkInfo->szDeviceId) && strlen(pLinkInfo->szFactoryId) 
						&& strlen(pLinkInfo->szOrg)) {
						char szTopicFilter[64] = { 0 };
						sprintf_s(szTopicFilter, sizeof(szTopicFilter), "%s_%s_%s", pLinkInfo->szOrg, 
							pLinkInfo->szFactoryId, pLinkInfo->szDeviceId);
						size_t nSubInfoSize = sizeof access_service::AppSubscribeInfo;
						auto pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
						strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopicFilter);
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
					
					pthread_mutex_lock(&g_mutex4GuarderList);
					if (zhash_size(g_guarderList)) {
						Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szUser);
						if (pGuarder) {
							strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szSession);
						}
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
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

int AccessService::sendDatatoEndpoint(char * pData, uint32_t nDataLen, const char * pEndpoint)
{
	return 0;
}

int AccessService::sendDataToEndpoint_v2(char * pData_, uint32_t nDataLen_, const char * pEndpoint_,
	uint32_t nDataType_, const char * pFrom_)
{
	int result = -1;
	if (strlen(pEndpoint_)) {
		unsigned char * pFrameData = NULL;
		size_t nFrameDataLen = 0;
		if (pData_ && nDataLen_ > 0) {
			size_t nHeadSize = sizeof(access_service::AppMessageHead);
			access_service::AppMessageHead head;
			head.marker[0] = 'E';
			head.marker[1] = 'C';
			head.version[0] = '1';
			head.version[1] = '0';
			char * pUtf8Data = ansiToUtf8(pData_);
			size_t nUtf8DataLen = strlen(pUtf8Data);
			head.uiDataLen = (unsigned int)nUtf8DataLen;
			nFrameDataLen = nHeadSize + nUtf8DataLen;
			pFrameData = new unsigned char[nFrameDataLen + 1];
			memcpy_s(pFrameData, nFrameDataLen + 1, &head, nHeadSize);
			memcpy_s(pFrameData + nHeadSize, nUtf8DataLen + 1, pUtf8Data, nUtf8DataLen);
			pFrameData[nFrameDataLen] = '\0';
			encryptMessage(pFrameData, (unsigned int)nHeadSize, (unsigned int)nFrameDataLen);
			delete[] pUtf8Data;
			pUtf8Data = NULL;
		}
		zmsg_t * msg = zmsg_new();
		zframe_t * frame_id = zframe_from(pFrom_);
		char szMsgType[16] = { 0 };
		sprintf_s(szMsgType, sizeof(szMsgType), "%u", nDataType_);
		zframe_t * frame_type = zframe_from(szMsgType);
		zframe_t * frame_endpoint = zframe_from(pEndpoint_);
		char szMsgTime[20] = { 0 };
		formatDatetime(time(NULL), szMsgTime, sizeof(szMsgTime));
		zframe_t * frame_time = zframe_from(szMsgTime);
		zframe_t * frame_data = zframe_new(pFrameData, nFrameDataLen);
		zmsg_append(msg, &frame_id);
		zmsg_append(msg, &frame_type);
		zmsg_append(msg, &frame_endpoint);
		zmsg_append(msg, &frame_time);
		zmsg_append(msg, &frame_data);
		if (1) {
			std::lock_guard<std::mutex> lk(m_mutex4AccessSock);
			zmsg_send(&msg, m_accessSock);
		}
		result = 0;
		if (pFrameData) {
			delete[] pFrameData;
			pFrameData = NULL;
		}
	}
	return result;
}

int AccessService::sendDataViaInteractor_v2(const char * pData_, uint32_t nDataLen_)
{
	int result = -1;
	if (pData_ && nDataLen_ > 0) {
		zmsg_t * msg = zmsg_new();
		zframe_t * frame = zframe_new(pData_, nDataLen_);
		zmsg_append(msg, &frame);
		zmsg_send(&msg, m_interactorSock);
		result = 0;
	}
	return result;
}

void AccessService::handleLinkDisconnect(const char * pLink_, const char * pUser_, bool bFlag_)
{
	char szLog[512] = { 0 };
	if (pLink_) {
		std::string strLinkId = pLink_;
		char szGuarder[20] = { 0 };
		char szSession[20] = { 0 };
		bool bSubscriber = false;
		char szOrg[40] = { 0 };
		char szDeviceId[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkDataList);
		std::map<std::string, access_service::LinkDataInfo *>::iterator iter = m_linkDataList.find(strLinkId);
		if (iter != m_linkDataList.end()) {
			access_service::LinkDataInfo * pLinkData = iter->second;
			if (pLinkData) {
				pLinkData->nLinkState = 1;
				if (strlen(pLinkData->szUser)) {
					strncpy_s(szGuarder, sizeof(szGuarder), pLinkData->szUser, strlen(pLinkData->szUser));
				}
				if (pLinkData->pLingeData && pLinkData->uiTotalDataLen) {
					free(pLinkData->pLingeData);
					pLinkData->pLingeData = NULL;
					pLinkData->uiTotalDataLen = 0;
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
						strncpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession, 
							strlen(pGuarder->szCurrentSession));
						if (pGuarder->usState == STATE_GUARDER_DUTY || pGuarder->usState == STATE_GUARDER_BIND) {
							bSubscriber = true;
							strncpy_s(szOrg, sizeof(szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
							strncpy_s(szDeviceId, sizeof(szDeviceId), pGuarder->szBindDevice, 
								strlen(pGuarder->szBindDevice));
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
				access_service::AppLinkInfo * pAppLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList,
					szSession);
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
					if (strlen(pDevice->deviceBasic.szOrgId)) {
						strncpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId, strlen(pDevice->deviceBasic.szOrgId));
					}
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, pDevice->deviceBasic.szFactoryId, szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				if (strlen(szTopic)) {
					pthread_mutex_lock(&m_mutex4SubscribeList);
					access_service::AppSubscribeInfo * pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(
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
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]link=%s disconnect, guarder=%s\r\n", 
			__FUNCTION__, __LINE__, pLink_, szGuarder);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::loadOrgList(bool bFlag_)
{
	pthread_mutex_lock(&g_mutex4OrgList);
	if (!g_orgList2.empty()) {
		std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.begin();
		std::map<std::string, OrganizationEx *>::iterator iter_end = g_orgList2.end();
		if (bFlag_) {
			for (; iter != iter_end; iter++) {
				OrganizationEx * pOrg = iter->second;
				if (pOrg) {
					pOrg->childList.clear();
				}
			}
			iter = g_orgList2.begin();
		}
		for (; iter != iter_end; iter++) {
			OrganizationEx * pOrg = iter->second;
			if (pOrg) {
				std::string strOrgId = pOrg->org.szOrgId;
				if (strlen(pOrg->org.szParentOrgId)) {
					std::string strParentOrgId = pOrg->org.szParentOrgId;
					std::map<std::string, OrganizationEx *>::iterator it = g_orgList2.find(strParentOrgId);
					if (it != iter_end) {
						OrganizationEx * pParentOrg = it->second;
						if (pParentOrg) {
							if (pParentOrg->childList.count(strOrgId) == 0) {
								pParentOrg->childList.emplace(strOrgId);
							}
						}
					}
				}
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4OrgList);
}

void AccessService::findOrgChild(std::string strOrgId_, std::set<std::string> & childList_)
{
	std::map<std::string, OrganizationEx *>::iterator iter = g_orgList2.find(strOrgId_);
	if (iter != g_orgList2.end()) {
		OrganizationEx * pOrg = iter->second;
		if (pOrg) {
			if (!pOrg->childList.empty()) {
				std::set<std::string>::iterator it = pOrg->childList.begin();
				std::set<std::string>::iterator it_end = pOrg->childList.end();
				for (; it != it_end; ++it) {
					std::string strChildOrgId = *it;
					if (childList_.count(strChildOrgId) == 0) {
						childList_.emplace(strChildOrgId);
					}
					findOrgChild(strChildOrgId, childList_);
				}
			}
		}
	}
}

char * AccessService::utf8ToAnsi(const char * pUtf8_)
{
	char szLog[256] = { 0 };
	if (pUtf8_ && strlen(pUtf8_)) {
		int WLen = MultiByteToWideChar(CP_UTF8, 0, pUtf8_, -1, nullptr, 0);
		wchar_t * pWData = new wchar_t [WLen + 1];
		MultiByteToWideChar(CP_UTF8, 0, pUtf8_, -1, pWData, WLen);
		pWData[WLen] = '\0';

		int ALen = WideCharToMultiByte(CP_ACP, 0, pWData, -1, nullptr, 0, nullptr, nullptr);
		char * pData = new char [ALen + 1];
		WideCharToMultiByte(CP_ACP, 0, pWData, -1, pData, ALen, nullptr, nullptr);
		pData[ALen] = '\0';

		delete [] pWData;
		pWData = nullptr;
	
		return pData;
	}
	return nullptr;
}

char * AccessService::ansiToUtf8(const char * pAnsi_)
{
	if (pAnsi_ && strlen(pAnsi_)) {
		int WLen = MultiByteToWideChar(CP_ACP, 0, pAnsi_, -1, nullptr, 0);
		wchar_t * pWData = new wchar_t [WLen + 1];
		MultiByteToWideChar(CP_ACP, 0, pAnsi_, -1, pWData, WLen);
		pWData[WLen] = '\0';
		int ALen = WideCharToMultiByte(CP_UTF8, 0, pWData, -1, nullptr, 0, nullptr, nullptr);
		char * pData = new char[ALen + 1];
		WideCharToMultiByte(CP_UTF8, 0, pWData, -1, pData, ALen, nullptr, nullptr);
		pData[ALen] = '\0';
		delete []pWData;
		pWData = nullptr;
		return pData;
	}
	return nullptr;
}

unsigned long long AccessService::makeDatetime(const char * strDatetime_)
{
	struct tm tm_curr;
	sscanf_s(strDatetime_, "%04d%02d%02d%02d%02d%02d", &tm_curr.tm_year, &tm_curr.tm_mon, &tm_curr.tm_mday,
		&tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
	tm_curr.tm_year -= 1900;
	tm_curr.tm_mon -= 1;
	return (unsigned long long)mktime(&tm_curr);
}

void AccessService::formatDatetime(unsigned long long ulSrcTime_, char * pStrDatetime_,
	size_t nStrDatetimeLen_)
{
	tm tm_time;
	time_t srcTime = ulSrcTime_;
	localtime_s(&tm_time, &srcTime);
	char szDatetime[20] = { 0 };
	sprintf_s(szDatetime, sizeof(szDatetime), "%04d%02d%02d%02d%02d%02d", tm_time.tm_year + 1900,
		tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	size_t nLen = strlen(szDatetime);
	if (pStrDatetime_ && nStrDatetimeLen_ >= nLen) {
		strncpy_s(pStrDatetime_, nStrDatetimeLen_, szDatetime, nLen);
	}
}

bool AccessService::addDisconnectEvent(const char * pEndpoint_, const char * pEventTime_)
{
	bool result = false;
	if (pEndpoint_ && pEventTime_ && strlen(pEndpoint_) && strlen(pEventTime_)) {
		auto pDisconnEvnet = new access_service::DisconnectEvent();
		strcpy_s(pDisconnEvnet->szEndpoint, sizeof(pDisconnEvnet->szEndpoint), pEndpoint_);
		strcpy_s(pDisconnEvnet->szEventTime, sizeof(pDisconnEvnet->szEventTime), pEventTime_);
		std::lock_guard<std::mutex> lock(m_mutex4DisconnEventQue);
		m_disconnEventQue.emplace(pDisconnEvnet);
		if (m_disconnEventQue.size() == 1) {
			m_cond4DissconnEventQue.notify_one();
		}
		result = true;
	}
	return result;
}

void AccessService::handleDisconnectEvent()
{
	do {
		std::unique_lock<std::mutex> lock(m_mutex4DisconnEventQue);
		m_cond4DissconnEventQue.wait(lock, [&] {
			return (!m_nRun || !m_disconnEventQue.empty());
		});
		if (!m_nRun && m_disconnEventQue.empty()) {
			break;
		}
		access_service::DisconnectEvent * pEvent = m_disconnEventQue.front();
		m_disconnEventQue.pop();
		lock.unlock();
		if (pEvent) {
			bool bExistsLink = false;
			pthread_mutex_lock(&m_mutex4LinkDataList);
			char szGuarder[20] = { 0 };
			LinkDataList::iterator iter = m_linkDataList.find((std::string)pEvent->szEndpoint);
			if (iter != m_linkDataList.end()) {
				access_service::LinkDataInfo * pLinkData = iter->second;
				if (pLinkData) {
					bExistsLink = true;
					strcpy_s(szGuarder, sizeof(szGuarder), pLinkData->szUser);
					if (pLinkData->pLingeData) {
						delete[] pLinkData->pLingeData;
						pLinkData->pLingeData = NULL;
					}
					delete pLinkData;
					pLinkData = NULL;
				}
				m_linkDataList.erase(iter);
			}
			pthread_mutex_unlock(&m_mutex4LinkDataList);
			if (m_nRun) {
				//guarder
				char szSession[20] = { 0 };
				char szSubTopic[64] = { 0 };

				if (strlen(szGuarder)) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (strcmp(pGuarder->szLink, pEvent->szEndpoint) == 0) {
							strcpy_s(szSession, sizeof(szSession), pGuarder->szCurrentSession);
							pGuarder->szLink[0] = '\0';
						}
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}

				//appLinkInfo
				if (strlen(szSession)) {
					pthread_mutex_lock(&m_mutex4LinkList);
					auto pLinkInfo = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, szSession);
					if (pLinkInfo) {
						pLinkInfo->szEndpoint[0] = '\0';
						pLinkInfo->nActivated = 0;
						//strcpy_s(szGuarder, sizeof(szGuarder), pLinkInfo->szGuarder);
						if (strlen(pLinkInfo->szDeviceId) && strlen(pLinkInfo->szOrg) && strlen(pLinkInfo->szFactoryId)) {
							sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pLinkInfo->szOrg, pLinkInfo->szFactoryId,
								pLinkInfo->szDeviceId);
						}
					}
					pthread_mutex_unlock(&m_mutex4LinkList);
				}
				//subscriberList
				if (strlen(szSubTopic)) {
					pthread_mutex_lock(&m_mutex4SubscribeList);
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szSubTopic);
					if (pSubInfo) {
						if (strcmp(pSubInfo->szEndpoint, pEvent->szEndpoint) == 0) {
							pSubInfo->szEndpoint[0] = '\0';
							pSubInfo->szAccSource[0] = '\0';
						}
					}
					pthread_mutex_unlock(&m_mutex4SubscribeList);
				}
			}
			if (bExistsLink) {
				char szLog[256] = { 0 };
				sprintf_s(szLog, sizeof(szLog), "[AccessService]%s[%d]endpoint=%s disconnect, guarder=%s, datetime=%s\n", 
					__FUNCTION__, __LINE__, pEvent->szEndpoint, szGuarder, pEvent->szEventTime);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			}
			delete pEvent;
			pEvent = NULL;
		}
	} while (1);
}

void AccessService::checkAccClientList()
{
	char szLog[256] = { 0 };
	std::vector<std::string> endpointList;
	time_t checkTime = time(NULL);
	{
		std::lock_guard<std::mutex> lock(m_mutex4AccClientList);
		if (!m_accClientList.empty()) {
			AccessClientList::iterator iter = m_accClientList.begin();
			while (iter != m_accClientList.end()) {
				auto pAccClient = iter->second;
				if (pAccClient) {
					double dInterval = difftime(checkTime, (time_t)pAccClient->ullLastActivatedTime);
					if (dInterval > 60.0) {
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]accessClientId=%s lost connection, interval=%.4f\n",
							__FUNCTION__, __LINE__, pAccClient->szAccClientId, dInterval);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
						if (!pAccClient->proxySet.empty()) {
							std::set<std::string>::iterator it = pAccClient->proxySet.begin();
							while (it != pAccClient->proxySet.end()) {
								std::string strProxyEndpoint = *it;
								endpointList.emplace_back(strProxyEndpoint);
								it = pAccClient->proxySet.erase(it);
							}
						}
						delete pAccClient;
						pAccClient = NULL;
						iter = m_accClientList.erase(iter);
						continue;
					}
				}
				iter++;
			}
		}
	}
	if (!endpointList.empty()) {
		char szCheckTime[20] = { 0 };
		formatDatetime(checkTime, szCheckTime, sizeof(szCheckTime));
		std::vector<std::string>::iterator iter = endpointList.begin();
		while (iter != endpointList.end()) {
			std::string strEndpoint = *iter;
			addDisconnectEvent(strEndpoint.c_str(), szCheckTime);
			iter = endpointList.erase(iter);
		}
	}
}

void AccessService::updateAccClient(const char * pAccCltId_, const char * pAccFrom_)
{
	if (pAccCltId_ && strlen(pAccCltId_)) {
		time_t nCurrentTime = time(NULL);
		std::lock_guard<std::mutex> lock(m_mutex4AccClientList);
		if (m_accClientList.empty()) {
			auto pAccClient = new access_service::AccessClientInfo();
			strcpy_s(pAccClient->szAccClientId, sizeof(pAccClient->szAccClientId), pAccCltId_);
			pAccClient->ullLastActivatedTime = nCurrentTime;
			if (pAccFrom_ && strlen(pAccFrom_)) {
				pAccClient->proxySet.emplace((std::string)pAccFrom_);
			}
			m_accClientList.emplace((std::string)pAccCltId_, pAccClient);
		}
		else {
			AccessClientList::iterator iter = m_accClientList.find((std::string)pAccCltId_);
			if (iter != m_accClientList.end()) {
				auto pAccClient = iter->second;
				if (pAccClient) {
					pAccClient->ullLastActivatedTime = nCurrentTime;
					if (pAccFrom_ && strlen(pAccFrom_)) {
						if (!pAccClient->proxySet.empty()) {
							if (pAccClient->proxySet.count((std::string)pAccFrom_) == 0) {
								pAccClient->proxySet.emplace((std::string)pAccFrom_);
							}
						}
						else {
							pAccClient->proxySet.emplace((std::string)pAccFrom_);
						}
					}
				}
			}
			else {
				auto pAccClient = new access_service::AccessClientInfo();
				strcpy_s(pAccClient->szAccClientId, sizeof(pAccClient->szAccClientId), pAccCltId_);
				pAccClient->ullLastActivatedTime = nCurrentTime;
				if (pAccFrom_ && strlen(pAccFrom_)) {
					pAccClient->proxySet.emplace((std::string)pAccFrom_);
				}
				m_accClientList.emplace((std::string)pAccCltId_, pAccClient);
			}
		}
	}
}

void AccessService::checkAppLinkList()
{
	std::vector<std::string> lingerList;
	std::vector<std::string> guarderList;
	time_t nCheckTime = time(NULL);
	pthread_mutex_lock(&m_mutex4LinkList);
	if (zhash_size(m_linkList)) {
		auto pLink = (access_service::AppLinkInfo *)zhash_first(m_linkList);
		while (pLink) {
			if (strlen(pLink->szEndpoint)) {
				double dInterval = difftime(nCheckTime, pLink->ulActivateTime);
				if (pLink->nActivated == 1) {
					if (dInterval > 60.0) { //1min
						pLink->nActivated = 0;
					}
				}
				else { 
					if (dInterval > 120.0) { //2min
						if (pLink->nLinkFormat == LOGIN_FORMAT_TCP_SOCKET) {
							pLink->szEndpoint[0] = '\0';
							lingerList.emplace_back((std::string)pLink->szEndpoint);
						}
						else if (pLink->nLinkFormat == LOGIN_FORMAT_MQ) {
							pLink->szEndpoint[0] = '\0';
							guarderList.emplace_back((std::string)pLink->szGuarder);
						}
					}
				}
			}
			pLink = (access_service::AppLinkInfo *)zhash_next(m_linkList);
		}
	}
	pthread_mutex_unlock(&m_mutex4LinkList);
	if (!lingerList.empty()) {
		char szCheckTime[20] = { 0 };
		formatDatetime(nCheckTime, szCheckTime, sizeof(szCheckTime));
		std::vector<std::string>::iterator iter = lingerList.begin();
		while (iter != lingerList.end()) {
			std::string strEndpoint = *iter;
			addDisconnectEvent(strEndpoint.c_str(), szCheckTime);
			iter = lingerList.erase(iter);
		}
	}
	if (!guarderList.empty()) {
		pthread_mutex_lock(&g_mutex4GuarderList);
		std::vector<std::string>::iterator iter = guarderList.begin();
		for (; iter != guarderList.end(); iter++) {
			auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, (*iter).c_str());
			if (pGuarder) {
				pGuarder->szLink[0] = '\0';
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
	}
}

void AccessService::sendInteractAlive()
{
	bool bSendAlive = false;
	time_t nCheckTime = time(NULL);
	pthread_mutex_lock(&m_mutex4RemoteLink);
	if (m_remoteMsgSrvLink.nActive == 0) {
		if (m_remoteMsgSrvLink.nSendKeepAlive == 0) {
			bSendAlive = true;
			m_remoteMsgSrvLink.nSendKeepAlive = 1;
		}
		else {
			unsigned int uiInterval = (unsigned int)(nCheckTime - m_remoteMsgSrvLink.ulLastActiveTime);
			if (uiInterval > 300) {
				m_remoteMsgSrvLink.nSendKeepAlive = 0;
				m_remoteMsgSrvLink.ulLastActiveTime = nCheckTime;
			}
		}
	}
	else {
		unsigned int uiInterval = (unsigned int)(nCheckTime - m_remoteMsgSrvLink.ulLastActiveTime);
		if (uiInterval > 30) {
			bSendAlive = true;
			if (uiInterval > 180) {
				m_remoteMsgSrvLink.nActive = 0;
				m_remoteMsgSrvLink.nSendKeepAlive = 0;
			}
		}
	}
	pthread_mutex_unlock(&m_mutex4RemoteLink);
	if (bSendAlive) {
		char szDatetime[20] = { 0 };
		formatDatetime(nCheckTime, szDatetime, sizeof(szDatetime));
		char szMsg[256] = { 0 };
		sprintf_s(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\"}", MSG_SUB_ALIVE, getNextRequestSequence(), szDatetime);
		sendDataViaInteractor_v2(szMsg, (uint32_t)strlen(szMsg));
	}
}

bool AccessService::addExpressMessage(access_service::ClientExpressMessage * pExpressMsg_)
{
	bool result = false;
	if (pExpressMsg_) {
		if (pExpressMsg_->pExpressData && pExpressMsg_->uiExpressDataLen > 0) {
			std::lock_guard<std::mutex> lock(m_mutex4CltExpressMsgQue);
			m_cltExpressMsgQue.emplace(pExpressMsg_);
			if (m_cltExpressMsgQue.size() == 1) {
				m_cond4CltExpressMsgQue.notify_one();
			}
			result = true;
		}
	}
	return result;
}

void AccessService::dealExpressMessage()
{
	do {
		std::unique_lock<std::mutex> lock(m_mutex4CltExpressMsgQue);
		m_cond4CltExpressMsgQue.wait(lock, [&] {
			return (m_nRun == 0 || !m_cltExpressMsgQue.empty());
		});
		if (m_nRun == 0 && m_cltExpressMsgQue.empty()) {
			break;
		}
		access_service::ClientExpressMessage * pExpressMsg = m_cltExpressMsgQue.front();
		m_cltExpressMsgQue.pop();
		if (pExpressMsg) {
			if (pExpressMsg->pExpressData && pExpressMsg->uiExpressDataLen > 0) {
				parseExpressMessage(pExpressMsg);
				delete [] pExpressMsg->pExpressData;
				pExpressMsg->pExpressData = NULL;
				pExpressMsg->uiExpressDataLen = 0;
			}
			delete pExpressMsg;
			pExpressMsg = NULL;
		}
	} while (1);
}

void AccessService::parseExpressMessage(const access_service::ClientExpressMessage * pMsg_)
{
	if (pMsg_) {
		char szLog[512] = { 0 };
		if (pMsg_->pExpressData && pMsg_->uiExpressDataLen > 0) {
			unsigned int uiBufLen = pMsg_->uiExpressDataLen;
			unsigned char * pBuf = new unsigned char[uiBufLen + 1];
			memcpy_s(pBuf, uiBufLen + 1, pMsg_->pExpressData, uiBufLen);
			pBuf[uiBufLen] = '\0';

			unsigned int uiIndex = 0, uiBegin = 0, uiEnd = 0, uiUnitLen = 0;
			do {
				int rc = getWholeMessage(pBuf, uiBufLen, uiIndex, uiBegin, uiEnd, uiUnitLen);
				if (rc == 0) {
					break;
				}
				else if (rc == 1) {


					break;
				}
				else if (rc == 2) {
					uiIndex = uiEnd + 1;
					decryptMessage(pBuf, uiBegin, uiEnd);
					char * pContent = new char[uiUnitLen + 1];
					memcpy_s(pContent, uiUnitLen + 1, pBuf + uiBegin, uiUnitLen);
					pContent[uiUnitLen] = '\0';
					char * pContent2 = utf8ToAnsi(pContent);
					rapidjson::Document doc;
					if (!doc.Parse(pContent2).HasParseError()) {
						unsigned int nCmd = access_service::E_CMD_UNDEFINE;
						if (doc.HasMember("cmd")) {
							if (doc["cmd"].IsUint()) {
								nCmd = doc["cmd"].GetUint();
							}
						}
						switch (nCmd) {
							case access_service::E_CMD_LOGIN: {
								access_service::AppLoginInfo loginInfo;
								memset(&loginInfo, 0, sizeof(access_service::AppLoginInfo));
								if (doc.HasMember("account")) {
									if (doc["account"].IsString() && doc["account"].GetStringLength()) {
										strcpy_s(loginInfo.szUser, sizeof(loginInfo.szUser), doc["account"].GetString());
									}
								}
								if (doc.HasMember("passwd")) {
									if (doc["passwd"].IsString() && doc["passwd"].GetStringLength()) {
										strcpy_s(loginInfo.szPasswd, sizeof(loginInfo.szPasswd), doc["passwd"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(loginInfo.szDateTime, sizeof(loginInfo.szDateTime), doc["datetime"].GetString());
									}
								}
								if (doc.HasMember("handset")) {
									if (doc["handset"].IsString() && doc["handset"].GetStringLength()) {
										strcpy_s(loginInfo.szHandset, sizeof(loginInfo.szHandset), doc["handset"].GetString());
									}
								}
								if (strlen(loginInfo.szUser) && strlen(loginInfo.szPasswd)) {
									handleExpressAppLogin(&loginInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									char szReply[256] = {};
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
										access_service::E_CMD_LOGIN_REPLY, E_INVALIDPARAMETER);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request from %s, one or more parameter "
										"needed, account=%s, passwd=%s, datetime=%s, handset=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, loginInfo.szUser, loginInfo.szPasswd, loginInfo.szDateTime, loginInfo.szHandset);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_LOGOUT: {
								access_service::AppLogoutInfo logoutInfo;
								memset(&logoutInfo, 0, sizeof(access_service::AppLogoutInfo));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(logoutInfo.szSession, sizeof(logoutInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(logoutInfo.szDateTime, sizeof(logoutInfo.szDateTime), doc["datetime"].GetString());
									}
								}
								if (strlen(logoutInfo.szSession)) {
									handleExpressAppLogout(&logoutInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout request from %s, one or more "
										"parameter needed, session=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										logoutInfo.szSession, logoutInfo.szDateTime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
										access_service::E_CMD_LOGOUT_REPLY, E_INVALIDPARAMETER, logoutInfo.szSession);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_BIND_REPORT: {
								access_service::AppBindInfo bindInfo;
								memset(&bindInfo, 0, sizeof(access_service::AppBindInfo));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), doc["datetime"].GetString());
									}
								}
								if (strlen(bindInfo.szSesssion) && strlen(bindInfo.szDeviceId)) {
									handleExpressAppBind(&bindInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]bind request from %s, one or more "
										"parameter needed, session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, bindInfo.szSesssion, bindInfo.szDeviceId, bindInfo.szDateTime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"battery\":0}",
										access_service::E_CMD_BIND_REPLY, E_INVALIDPARAMETER, bindInfo.szSesssion);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_UNBIND_REPORT: {
								access_service::AppBindInfo bindInfo;
								memset(&bindInfo, 0, sizeof(access_service::AppBindInfo));
								bindInfo.nMode = 1;
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(bindInfo.szSesssion, sizeof(bindInfo.szSesssion), doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(bindInfo.szDeviceId, sizeof(bindInfo.szDeviceId), doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(bindInfo.szDateTime, sizeof(bindInfo.szDateTime), doc["datetime"].GetString());
									}
								}
								if (strlen(bindInfo.szDeviceId) && strlen(bindInfo.szSesssion)) {
									handleExpressAppBind(&bindInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]unbind request from %s, one or more parameter"
										" needed, session=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										bindInfo.szSesssion, bindInfo.szDeviceId, bindInfo.szDateTime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
										access_service::E_CMD_UNBIND_REPLY, E_INVALIDPARAMETER, bindInfo.szSesssion);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_TASK: {
								access_service::AppSubmitTaskInfo taskInfo;
								memset(&taskInfo, 0, sizeof(access_service::AppSubmitTaskInfo));
								bool bValidTarget = false;
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("type")) {
									if (doc["type"].IsInt()) {
										taskInfo.usTaskType = (unsigned short)doc["type"].GetInt();
									}
								}
								if (doc.HasMember("limit")) {
									if (doc["limit"].IsInt()) {
										taskInfo.usTaskLimit = (unsigned short)doc["limit"].GetInt();
									}
								}
								if (doc.HasMember("destination")) {
									if (doc["destination"].IsString() && doc["destination"].GetStringLength()) {
										size_t nSize = doc["destination"].GetStringLength();
										size_t nFieldSize = sizeof(taskInfo.szDestination);
										strncpy_s(taskInfo.szDestination, nFieldSize, doc["destination"].GetString(),
											(nSize < nFieldSize) ? nSize : nFieldSize - 1);
									}
								}
								if (doc.HasMember("target")) {
									if (doc["target"].IsString()) {
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
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(taskInfo.szSession) && taskInfo.usTaskLimit > 0 && taskInfo.usTaskType > 0 && bValidTarget) {
									handleExpressAppSubmitTask(&taskInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit task request from %s, one or more "
										"parameter needed, session=%s, type=%d, limit=%d, target=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, taskInfo.szSession, taskInfo.usTaskType, taskInfo.usTaskLimit, taskInfo.szTarget,
										taskInfo.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}",
										access_service::E_CMD_TASK_REPLY, E_INVALIDPARAMETER, taskInfo.szSession);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_TASK_CLOSE: {
								access_service::AppCloseTaskInfo taskInfo;
								memset(&taskInfo, 0, sizeof(access_service::AppCloseTaskInfo));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(taskInfo.szSession, sizeof(taskInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(taskInfo.szTaskId, sizeof(taskInfo.szTaskId), doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("closeType")) {
									if (doc["closeType"].IsUint()) {
										taskInfo.nCloseType = doc["closeType"].GetUint();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(taskInfo.szDatetime, sizeof(taskInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(taskInfo.szSession) && strlen(taskInfo.szTaskId)) {
									handleExpressAppCloseTask(&taskInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]close task request from %s, one or more "
										"parameter needed, session=%s, taskId=%s, closeType=%d, datetime=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, taskInfo.szSession, taskInfo.szTaskId, taskInfo.nCloseType, taskInfo.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
										access_service::E_CMD_TASK_CLOSE_REPLY, E_INVALIDPARAMETER, taskInfo.szSession, taskInfo.szTaskId);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_POSITION_REPORT: {
								access_service::AppPositionInfo posInfo;
								memset(&posInfo, 0, sizeof(access_service::AppPositionInfo));
								posInfo.nCoordinate = COORDINATE_BD09;
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(posInfo.szSession, sizeof(posInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(posInfo.szTaskId, sizeof(posInfo.szTaskId), doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("lat") && doc.HasMember("lng") && doc.HasMember("coordinate")) {
									if (doc["lat"].IsDouble()) {
										posInfo.dLat = doc["lat"].GetDouble();
									}
									if (doc["lng"].IsDouble()) {
										posInfo.dLng = doc["lng"].GetDouble();
									}
									if (doc.HasMember("coordinate")) {
										if (doc["coordinate"].IsInt()) {
											posInfo.nCoordinate = doc["coordinate"].GetInt();
										}
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(posInfo.szDatetime, sizeof(posInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(posInfo.szSession) && strlen(posInfo.szTaskId)) {
									handleExpressAppPosition(&posInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]position report from %s, one or more"
										" parameter needed, session=%s, taskId=%s, lat=%f, lng=%f, datetime=%s\r\n", __FUNCTION__,
										__LINE__, pMsg_->szClientId, posInfo.szSession, posInfo.szTaskId, posInfo.dLat,
										posInfo.dLng, posInfo.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_FLEE_REPORT: {
								access_service::AppSubmitFleeInfo fleeInfo;
								memset(&fleeInfo, 0, sizeof(access_service::AppSubmitFleeInfo));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(fleeInfo.szSession, sizeof(fleeInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(fleeInfo.szTaskId, sizeof(fleeInfo.szTaskId), doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(fleeInfo.szDatetime, sizeof(fleeInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(fleeInfo.szSession) && strlen(fleeInfo.szTaskId)) {
									fleeInfo.nMode = 0;
									handleExpressAppFlee(&fleeInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee report from %s, one or more parameter "
										"needed, session=%s, taskId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										fleeInfo.szSession, fleeInfo.szTaskId, fleeInfo.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
										access_service::E_CMD_FLEE_REPLY, E_INVALIDPARAMETER, fleeInfo.szSession, fleeInfo.szTaskId);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_FLEE_REVOKE_REPORT: {
								access_service::AppSubmitFleeInfo fleeInfo;
								memset(&fleeInfo, 0, sizeof(access_service::AppSubmitFleeInfo));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(fleeInfo.szSession, sizeof(fleeInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(fleeInfo.szTaskId, sizeof(fleeInfo.szTaskId), doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										strcpy_s(fleeInfo.szDatetime, sizeof(fleeInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(fleeInfo.szSession) && strlen(fleeInfo.szTaskId)) {
									fleeInfo.nMode = 1;
									handleExpressAppFlee(&fleeInfo, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]flee revoke report from %s, one or more "
										"parameter needed, session=%s, taskId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, fleeInfo.szSession, fleeInfo.szTaskId, fleeInfo.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
										access_service::E_CMD_FLEE_REVOKE_REPLY, E_INVALIDPARAMETER, fleeInfo.szSession, fleeInfo.szTaskId);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_KEEPALIVE: {
								access_service::AppKeepAlive keepAlive;
								memset(&keepAlive, 0, sizeof(access_service::AppKeepAlive));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(keepAlive.szSession, sizeof(keepAlive.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsUint()) {
										keepAlive.uiSeq = doc["seq"].GetUint();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(keepAlive.szDatetime, sizeof(keepAlive.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(keepAlive.szSession) && strlen(keepAlive.szDatetime)) {
									handleExpressAppKeepAlive(&keepAlive, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]keep alive from %s, one or more parameter"
										" needed, session=%s, seq=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										keepAlive.szSession, keepAlive.uiSeq, keepAlive.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case access_service::E_CMD_MODIFY_PASSWD: {
								access_service::AppModifyPassword modifyPasswd;
								memset(&modifyPasswd, 0, sizeof(access_service::AppModifyPassword));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(modifyPasswd.szSession, sizeof(modifyPasswd.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("curPasswd") && doc.HasMember("newPasswd")) {
									if (doc["currPasswd"].IsString() && doc["currPasswd"].GetStringLength()) {
										strcpy_s(modifyPasswd.szCurrPassword, sizeof(modifyPasswd.szCurrPassword),
											doc["currPasswd"].GetString());
									}
									if (doc["newPasswd"].IsString() && doc["newPasswd"].GetStringLength()) {
										strcpy_s(modifyPasswd.szNewPassword, sizeof(modifyPasswd.szNewPassword),
											doc["newPasswd"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(modifyPasswd.szDatetime, sizeof(modifyPasswd.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(modifyPasswd.szSession) && strlen(modifyPasswd.szCurrPassword)
									&& strlen(modifyPasswd.szNewPassword)) {
									handleExpressAppModifyAccountPassword(&modifyPasswd, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, one or more parameter "
										"needed, session=%s, currPasswd=%s, newPasswd=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
										pMsg_->szClientId, modifyPasswd.szSession, modifyPasswd.szCurrPassword, modifyPasswd.szNewPassword,
										modifyPasswd.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\"}",
										access_service::E_CMD_MODIFY_PASSWD_REPLY, E_INVALIDPARAMETER, modifyPasswd.szSession,
										modifyPasswd.szDatetime);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK: {
								access_service::AppQueryTask queryTask;
								memset(&queryTask, 0, sizeof(access_service::AppQueryTask));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(queryTask.szSession, sizeof(queryTask.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
										strcpy_s(queryTask.szTaskId, sizeof(queryTask.szTaskId), doc["taskId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(queryTask.szDatetime, sizeof(queryTask.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(queryTask.szSession) && strlen(queryTask.szTaskId)) {
									handleExpressAppQueryTask(&queryTask, pMsg_->ullMessageTime, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]query task from %s, one or more parameter "
										"miss, session=%s, task=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										queryTask.szSession, queryTask.szTaskId, queryTask.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\","
										"\"datetime\":\"%s\",\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, E_INVALIDPARAMETER,
										queryTask.szSession, queryTask.szDatetime);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_DEVICE_COMMAND: {
								access_service::AppDeviceCommandInfo devCmdInfo;
								memset(&devCmdInfo, 0, sizeof(devCmdInfo));

								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(devCmdInfo.szSession, sizeof(devCmdInfo.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(devCmdInfo.szDeviceId, sizeof(devCmdInfo.szDeviceId), doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(devCmdInfo.szDatetime, sizeof(devCmdInfo.szDatetime), doc["datetime"].GetString());
									}
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										devCmdInfo.nSeq = doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("param1")) {
									if (doc["param1"].IsInt()) {
										devCmdInfo.nParam1 = doc["param1"].GetInt();
									}
								}
								if (doc.HasMember("param2")) {
									if (doc["param2"].IsInt()) {
										devCmdInfo.nParam2 = doc["param2"].GetInt();
									}
								}
								if (strlen(devCmdInfo.szSession) && strlen(devCmdInfo.szDeviceId)) {
									handleExpressAppDeviceCommand(&devCmdInfo, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, miss parameter\n",
										__FUNCTION__, __LINE__, pMsg_->szClientId);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"device\":\"\","
										"\"seq\":0,\"datetime\":\"\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY, E_INVALIDPARAMETER);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_QUERY_PERSON: {
								access_service::AppQueryPerson qryPerson;
								memset(&qryPerson, 0, sizeof(access_service::AppQueryPerson));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(qryPerson.szSession, sizeof(qryPerson.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("queryPid")) {
									if (doc["queryPid"].IsString() && doc["queryPid"].GetStringLength()) {
										strcpy_s(qryPerson.szQryPersonId, sizeof(qryPerson.szQryPersonId), doc["queryPid"].GetString());
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(qryPerson.szQryDatetime, sizeof(qryPerson.szQryDatetime), doc["datetime"].GetString());
									}
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										qryPerson.uiQeurySeq = (unsigned int)doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("queryMode")) {
									if (doc["queryMode"].IsInt()) {
										qryPerson.nQryMode = doc["queryMode"].GetInt();
									}
								}
								if (strlen(qryPerson.szSession) && strlen(qryPerson.szQryPersonId)) {
									handleExpressAppQueryPerson(&qryPerson, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send query person from %s miss parameter,"
										"session=%s, qryPersonId=%s, qryMode=%u\n", __FUNCTION__, __LINE__, pMsg_->szClientId,
										qryPerson.szSession, qryPerson.szQryPersonId, qryPerson.nQryMode);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"\",\"datetime\":\"\",\"seq\":0,"
										"\"personList\":[]}", access_service::E_CMD_QUERY_PERSON_REPLY);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_QUERY_TASK_LIST: {
								access_service::AppQueryTaskList qryTaskList;
								memset(&qryTaskList, 0, sizeof(access_service::AppQueryTaskList));
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString() && doc["orgId"].GetStringLength()) {
										strcpy_s(qryTaskList.szOrgId, sizeof(qryTaskList.szOrgId), doc["orgId"].GetString());
									}
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										qryTaskList.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(qryTaskList.szDatetime, sizeof(qryTaskList.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(qryTaskList.szDatetime)) {
									handleExpressAppQueryTaskList(&qryTaskList, pMsg_->szClientId);
								}
								break;
							}
							case access_service::E_CMD_QUERY_DEVICE_STATUS: {
								access_service::AppQueryDeviceStatus qryDevStatus;
								memset(&qryDevStatus, 0, sizeof(qryDevStatus));
								if (doc.HasMember("session")) {
									if (doc["session"].IsString() && doc["session"].GetStringLength()) {
										strcpy_s(qryDevStatus.szSession, sizeof(qryDevStatus.szSession), doc["session"].GetString());
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDeviceId, sizeof(qryDevStatus.szDeviceId), doc["deviceId"].GetString());
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
										strcpy_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId), doc["factoryId"].GetString());
									}
								}
								else {
									sprintf_s(qryDevStatus.szFactoryId, sizeof(qryDevStatus.szFactoryId), "01");
								}
								if (doc.HasMember("seq")) {
									if (doc["seq"].IsInt()) {
										qryDevStatus.uiQrySeq = (unsigned int)doc["seq"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
										strcpy_s(qryDevStatus.szDatetime, sizeof(qryDevStatus.szDatetime), doc["datetime"].GetString());
									}
								}
								if (strlen(qryDevStatus.szSession) && strlen(qryDevStatus.szDeviceId) && strlen(qryDevStatus.szDatetime)) {
									handleExpressAppQueryDeviceStatus(&qryDevStatus, pMsg_->szClientId);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]recv app query device status data miss parameter,"
										" deviceId=%s, session=%s, seq=%u, datetime=%s\r\n", __FUNCTION__, __LINE__, qryDevStatus.szDeviceId,
										qryDevStatus.szSession, qryDevStatus.uiQrySeq, qryDevStatus.szDatetime);
									LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
									char szReply[256] = { 0 };
									sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"deviceId\":\"%s\","
										"\"status\":%d,\"battery\":%d,\"seq\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_QUERY_DEVICE_STATUS,
										qryDevStatus.szSession, E_INVALIDPARAMETER, qryDevStatus.szDeviceId, 0, 0, qryDevStatus.uiQrySeq,
										qryDevStatus.szDatetime);
									sendExpressMessageToClient(szReply, pMsg_->szClientId);
								}
								break;
							}
							default: {
								sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]can't recognise command: %d\r\n",
									__FUNCTION__, __LINE__, nCmd);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								char szReply[256] = { 0 };
								sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d}", access_service::E_CMD_DEFAULT_REPLY,
									E_INVALIDCOMMAND);
								sendExpressMessageToClient(szReply, pMsg_->szClientId);
								break;
							}
						}
					}
					delete[] pContent;
					pContent = NULL;
					delete[] pContent2;
					pContent2 = NULL;
				}
			} while (1);
			delete[] pBuf;
			pBuf = NULL;
		}
	}
}

void AccessService::sendExpressMessageToClient(const char * pMsgContent_, const char * pClientId_)
{
	if (pMsgContent_ && strlen(pMsgContent_)) {
		size_t nHeadSize = sizeof(access_service::AppMessageHead);
		access_service::AppMessageHead head;
		head.marker[0] = 'E';
		head.marker[1] = 'C';
		head.version[0] = '1';
		head.version[1] = '0';
		char * pUtf8Data = ansiToUtf8(pMsgContent_);
		size_t nUtf8DataLen = strlen(pUtf8Data);
		head.uiDataLen = (unsigned int)nUtf8DataLen;
		size_t nFrameDataLen = nHeadSize + nUtf8DataLen;
		unsigned char * pFrameData = new unsigned char[nFrameDataLen + 1];
		memcpy_s(pFrameData, nFrameDataLen + 1, &head, nHeadSize);
		memcpy_s(pFrameData + nHeadSize, nUtf8DataLen + 1, pUtf8Data, nUtf8DataLen);
		pFrameData[nFrameDataLen] = '\0';
		encryptMessage(pFrameData, (unsigned int)nHeadSize, (unsigned int)nFrameDataLen);
		delete[] pUtf8Data;
		pUtf8Data = NULL;
		zmsg_t * msg = zmsg_new();
		zframe_t * frame_id = zframe_from(pClientId_);
		char szMsgType[16] = { 0 };
		sprintf_s(szMsgType, sizeof(szMsgType), "%u", access_service::E_ACC_APP_EXPRESS);
		zframe_t * frame_type = zframe_from(szMsgType);
		zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
		zmsg_append(msg, &frame_id);
		zmsg_append(msg, &frame_type);
		zmsg_append(msg, &frame_body);
		if (1) {
			std::lock_guard<std::mutex> lk(m_mutex4AccessSock);
			zmsg_send(&msg, m_accessSock);
		}
		delete[] pFrameData;
		pFrameData = NULL;
		unsigned int uiLogLen = (unsigned int)strlen(pMsgContent_) + 256;
		char * pLog = new char[uiLogLen];
		sprintf_s(pLog, uiLogLen, "[access_service]%s[%u]send to id=%s, %s\n", __FUNCTION__, __LINE__, pClientId_, pMsgContent_);
		LOG_Log(m_ullLogInst, pLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		delete[] pLog;
		pLog = NULL;
	}
}

void AccessService::handleExpressAppLogin(access_service::AppLoginInfo * pLoginInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	char szLog[512] = { 0 };
	if (pLoginInfo_) {
		if (!getLoadSessionFlag()) {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
				access_service::E_CMD_LOGIN_REPLY, E_SERVER_RESOURCE_NOT_READY);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login request received from %s, user=%s, server not ready "
				"yet, wait load session information\r\n", __FUNCTION__, __LINE__, pClientId_, pLoginInfo_->szUser);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		int nErr = E_OK;
		bool bHaveTask = false;
		char szCurrentTaskId[20] = { 0 };
		char szCurrentBindDeviceId[20] = { 0 };
		char szCurrentSession[20] = { 0 };
		char szLastestSession[20] = { 0 };
		bool bNeedGenerateSession = false;
		unsigned short usDeviceBattery = 0;
		unsigned short usDeviceStatus = 0;
		unsigned short usDeviceOnline = 0;
		pthread_mutex_lock(&g_mutex4GuarderList);
		if (zhash_size(g_guarderList)) {
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLoginInfo_->szUser);
			if (pGuarder) {
				if ((pGuarder->usRoleType & USER_ROLE_OPERATOR) != 0) {
					if (strlen(pGuarder->szCurrentSession) > 0) {
						strcpy_s(szCurrentSession, sizeof(szCurrentSession), pGuarder->szCurrentSession);
						if (pGuarder->usLoginFormat == LOGIN_FORMAT_MQ) {
							bool bContinue = true;
							if (strlen(pGuarder->szLink) > 0) {
								if (strcmp(pGuarder->szLink, pClientId_) != 0) {
									if (difftime(time(NULL), pGuarder->ullActiveTime) < 120) {
										nErr = E_ACCOUNTINUSE;
										bContinue = false;
									}
									else {
										bNeedGenerateSession = true;
									}
								}
								else {
									bNeedGenerateSession = true;
								}
							} 
							else {
								if (difftime(time(NULL), pGuarder->ullActiveTime) < 120) {
									nErr = E_ACCOUNTINUSE;
									bContinue = false;
								}
								else {
									bNeedGenerateSession = true;
								}
							}
							if (bContinue) {
								if (strcmp(pGuarder->szPassword, pLoginInfo_->szPasswd) == 0) {
									if (strlen(pGuarder->szBindDevice)) {
										strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
									}
									if (pGuarder->usState == STATE_GUARDER_DUTY) {
										if (strlen(pGuarder->szTaskId)) {
											strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
											bHaveTask = true;
										}
									}
								}
								else {
									nErr = E_INVALIDPASSWD;
								}
							}
						}
						else { //use tcp
							if (strlen(pGuarder->szLink) > 0) {
								nErr = E_ACCOUNTINUSE;
							}
							else { //link disconnect
								if (strcmp(pGuarder->szPassword, pLoginInfo_->szPasswd) == 0) {
									if (pGuarder->usState == STATE_GUARDER_BIND) {
										if (strlen(pGuarder->szBindDevice)) {
											strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
										}
									}
									else if (pGuarder->usState == STATE_GUARDER_DUTY) {
										if (strlen(pGuarder->szTaskId)) {
											strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
											bHaveTask = true;
										}
										if (strlen(pGuarder->szBindDevice)) {
											strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
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
					else { //no session
						if (pGuarder->usState == STATE_GUARDER_DEACTIVATE) {
							nErr = E_UNAUTHORIZEDACCOUNT;
						}
						else {
							if (strcmp(pGuarder->szPassword, pLoginInfo_->szPasswd) == 0) {
								if (pGuarder->usState == STATE_GUARDER_DUTY) {
									if (strlen(pGuarder->szTaskId) > 0) {
										strcpy_s(szCurrentTaskId, sizeof(szCurrentTaskId), pGuarder->szTaskId);
										strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pGuarder->szBindDevice);
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
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]guarder list is empty, need reload data\r\n",
				__FUNCTION__, __LINE__);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_WARN, m_usLogType);
			getUserList();
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
					strcpy_s(szLastestSession, sizeof(szLastestSession), szCurrentSession);
				}
			}
			EscortTask * pCurrentTask = NULL;
			bool bModifyTask = false;
			if (strlen(szCurrentTaskId)) {
				pthread_mutex_lock(&g_mutex4TaskList);
				if (zhash_size(g_taskList)) {
					EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szCurrentTaskId);
					if (pTask) {
						strcpy_s(szCurrentBindDeviceId, sizeof(szCurrentBindDeviceId), pTask->szDeviceId);
						if (pTask->nTaskMode == 0) {
							if (strlen(pLoginInfo_->szHandset)) {
								pTask->nTaskMode = 1;
								strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
								bModifyTask = true;
							}
						}
						else if (pTask->nTaskMode == 1) {
							if (strlen(pLoginInfo_->szHandset)) {//current login with handset
								if (strlen(pTask->szHandset)) { //task last handset is not empty
									if (strcmp(pTask->szHandset, pLoginInfo_->szHandset) != 0) { //change task handset
										bModifyTask = true;
										strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
									}
								}
								else { //task last handset is empty
									bModifyTask = true;
									strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pLoginInfo_->szHandset);
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
			if (strlen(szCurrentBindDeviceId)) {
				pthread_mutex_lock(&g_mutex4DevList);
				if (zhash_size(g_deviceList)) {
					WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szCurrentBindDeviceId);
					if (pDevice) {
						usDeviceStatus = pDevice->deviceBasic.nStatus;
						usDeviceBattery = pDevice->deviceBasic.nBattery;
						usDeviceOnline = pDevice->deviceBasic.nOnline;
						strcpy_s(szFactoryId, sizeof(szFactoryId), pDevice->deviceBasic.szFactoryId);
						strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			if (bHaveTask && pCurrentTask) {
				sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%d,\"limit\":%d,"
					"\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"battery\":%hu,\"deviceState\":%hu,"
					"\"online\":%hu,\"handset\":\"%s\"}", pCurrentTask->szTaskId, pCurrentTask->szDeviceId, pCurrentTask->nTaskType + 1,
					pCurrentTask->nTaskLimitDistance, pCurrentTask->szDestination, pCurrentTask->szTarget, pCurrentTask->szTaskStartTime,
					usDeviceBattery, usDeviceStatus, usDeviceOnline, pLoginInfo_->szHandset);
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}",
					access_service::E_CMD_LOGIN_REPLY, szLastestSession, szTaskInfo);
				sendExpressMessageToClient(szReply, pClientId_);
			}
			else {
				if (strlen(szCurrentBindDeviceId)) {
					sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"deviceId\":\"%s\",\"battery\":%hu,\"deviceState\":%hu,\"online\":%hu}",
						szCurrentBindDeviceId, usDeviceBattery, usDeviceStatus, usDeviceOnline);
					sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[%s]}",
						access_service::E_CMD_LOGIN_REPLY, szLastestSession, "");
					sendExpressMessageToClient(szReply, pClientId_);
				}
				else {
					sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":0,\"session\":\"%s\",\"taskInfo\":[]}",
						access_service::E_CMD_LOGIN_REPLY, szLastestSession);
					sendExpressMessageToClient(szReply, pClientId_);
				}
			}
			if (bModifyTask) {
				char szBody[512] = { 0 };
				sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
					"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"handset\":\"%s\",\"session\":\"%s\"}]}", MSG_SUB_REPORT, 
					pLoginInfo_->uiReqSeq, pLoginInfo_->szDateTime, SUB_REPORT_TASK_MODIFY, pCurrentTask->szTaskId, pLoginInfo_->szHandset,
					szLastestSession);
				sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));
			}
			//update guarder
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLoginInfo_->szUser);
			if (pGuarder) {
				strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szLastestSession);
				strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pClientId_);
				pGuarder->usLoginFormat = LOGIN_FORMAT_MQ;
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			//update link
			size_t nLinkInfoSize = sizeof(access_service::AppLinkInfo);
			access_service::AppLinkInfo * pLink = (access_service::AppLinkInfo *)zmalloc(nLinkInfoSize);
			memset(pLink, 0, nLinkInfoSize);
			strcpy_s(pLink->szGuarder, sizeof(pLink->szGuarder), pLoginInfo_->szUser);
			strcpy_s(pLink->szSession, sizeof(pLink->szSession), szLastestSession);
			strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
			pLink->nLinkFormat = LOGIN_FORMAT_MQ;
			if (strlen(pLoginInfo_->szHandset)) {
				strcpy_s(pLink->szHandset, sizeof(pLink->szHandset), pLoginInfo_->szHandset);
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
			pLink->ulActivateTime = ullMsgTime_;

			pthread_mutex_lock(&m_mutex4LinkList);
			if (bNeedGenerateSession && strlen(szCurrentSession)) {
				zhash_delete(m_linkList, szCurrentSession);
			}
			zhash_update(m_linkList, szLastestSession, pLink);
			zhash_freefn(m_linkList, szLastestSession, free);
			pthread_mutex_unlock(&m_mutex4LinkList);

			zkAddSession(szLastestSession);

			char szUploadMsg[512] = { 0 };
			sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\":\"%s\","
				"\"loginFormat\":%u}]}", MSG_SUB_REPORT, getNextRequestSequence(), pLoginInfo_->szDateTime, 
				SUB_REPORT_GUARDER_LOGIN, pLoginInfo_->szUser, szLastestSession, pLoginInfo_->szHandset, LOGIN_FORMAT_MQ);
			sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));

			if (pCurrentTask) {
				char szTopic[64] = { 0 };
				sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pCurrentTask->szOrg, pCurrentTask->szFactoryId, 
					pCurrentTask->szDeviceId);
				access_service::AppSubscribeInfo * pSubInfo;
				size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
				pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
				memset(pSubInfo, 0, nSubInfoSize);
				pSubInfo->nFormat = LOGIN_FORMAT_MQ;
				strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), pCurrentTask->szGuarder);
				strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), szLastestSession);
				strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
				strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pClientId_);

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
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"\",\"taskInfo\":[]}",
				access_service::E_CMD_LOGIN_REPLY, nErr);
			sendExpressMessageToClient(szReply, pClientId_);
		}
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]login user=%s, datetime=%s, handset=%s, result=%d, "
			"session=%s, from=%s\r\n", __FUNCTION__, __LINE__, pLoginInfo_->szUser, pLoginInfo_->szDateTime,
			pLoginInfo_->szHandset, nErr, szLastestSession, pClientId_);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppLogout(access_service::AppLogoutInfo * pLogoutInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	char szLog[512] = { 0 };
	int nErr = E_OK;
	bool bFindLink = false;
	char szReply[256] = { 0 };
	char szGuarder[20] = { 0 };
	char szDevKey[20] = { 0 };
	char szHandset[64] = { 0 };
	if (pLogoutInfo_) {
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
				access_service::E_CMD_LOGOUT_REPLY, E_SERVER_RESOURCE_NOT_READY, pLogoutInfo_->szSession);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive logout from %s, session=%s, server not ready\r\n",
				__FUNCTION__, __LINE__, pClientId_, pLogoutInfo_->szSession);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		pthread_mutex_lock(&m_mutex4LinkList);
		//logoutInfo.szSession
		if (zhash_size(m_linkList)) {
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pLogoutInfo_->szSession);
			if (pLink) {
				pLink->ulActivateTime = ullMsgTime_;
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
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
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
			access_service::E_CMD_LOGOUT_REPLY, nErr, pLogoutInfo_->szSession);
		sendExpressMessageToClient(szReply, pClientId_);

		if (nErr == E_OK) {
			//delete session link
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pLogoutInfo_->szSession);
			if (pLink) {
				strcpy_s(szDevKey, sizeof(szDevKey), pLink->szDeviceId);
				strcpy_s(szHandset, sizeof(szHandset), pLink->szHandset);
			}
			zhash_delete(m_linkList, pLogoutInfo_->szSession);
			pthread_mutex_unlock(&m_mutex4LinkList);

			zkRemoveSession(pLogoutInfo_->szSession);

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
			sprintf_s(szUpdateMsg, sizeof(szUpdateMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"handset\":\"%s\","
				"\"loginFormat\":%hu}]}", MSG_SUB_REPORT, getNextRequestSequence(), pLogoutInfo_->szDateTime,
				SUB_REPORT_GUARDER_LOGOUT, szGuarder, pLogoutInfo_->szSession, szHandset, LOGIN_FORMAT_MQ);
			sendDataViaInteractor_v2(szUpdateMsg, (uint32_t)strlen(szUpdateMsg));
		}

		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]logout session=%s, datetime=%s, user=%s, result=%d, "
			"from=%s\r\n", __FUNCTION__, __LINE__, pLogoutInfo_->szSession, pLogoutInfo_->szDateTime, szGuarder, nErr,
			pClientId_);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppBind(access_service::AppBindInfo * pBindInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pBindInfo_) {
		char szLog[512] = { 0 };
		char szGuarder[20] = { 0 };
		char szFactory[4] = { 0 };
		char szOrg[40] = { 0 };
		char szReply[256] = { 0 };
		int nErr = E_OK;
		if (!getLoadSessionFlag()) {
			if (pBindInfo_->nMode == 0) {
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"guarderId\":\"\","
					"\"deviceId\":\"%s\",\"battery\":0,\"status\":0}", access_service::E_CMD_BIND_REPLY, E_SERVER_RESOURCE_NOT_READY,
					pBindInfo_->szSesssion, pBindInfo_->szDeviceId);
			}
			else {
				sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\"}",
					access_service::E_CMD_UNBIND_REPLY, E_SERVER_RESOURCE_NOT_READY, pBindInfo_->szSesssion);
			}
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive bind request from %s, deviceId=%s, session=%s, "
				"server not ready yet, still wait for session information\r\n", __FUNCTION__, __LINE__, pClientId_, 
				pBindInfo_->szDeviceId, pBindInfo_->szSesssion);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		if (pBindInfo_->nMode == 0) { //bind
			bool bValidSession = false;
			bool bBindAlready = false;
			bool bValidGuarder = false;
			unsigned short usDeviceBattery = 0;
			unsigned short usDeviceStatus = 0;
			if (strlen(pBindInfo_->szSesssion)) {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
				if (pLink) {
					pLink->nActivated = 1;
					pLink->ulActivateTime = time(NULL);
					strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
					bValidSession = true;
					pLink->nLinkFormat = LOGIN_FORMAT_MQ;
					if (strcmp(pLink->szEndpoint, pClientId_) != 0) {
						strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
					}
				}
				else {
					nErr = E_INVALIDSESSION;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
			}
			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (escort::Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strlen(pGuarder->szTaskId)) {
						nErr = E_GUARDERINDUTY;
					}
					else {
						if (strlen(pGuarder->szBindDevice) && strcmp(pGuarder->szBindDevice, pBindInfo_->szDeviceId) == 0) {
							bBindAlready = true;
						}
						bValidGuarder = true;
					}
				}
				else {
					nErr = E_INVALIDACCOUNT;
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (bValidGuarder) {
					pthread_mutex_lock(&g_mutex4DevList);
					auto pDevice = (escort::WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
					if (pDevice) {
						if (pDevice->deviceBasic.nOnline == 0) {
							nErr = E_UNABLEWORKDEVICE;
						}
						else {
							if ((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
								|| (pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
								nErr = E_ALREADYBINDDEVICE;
							}
						}
						usDeviceBattery = pDevice->deviceBasic.nBattery;
						usDeviceStatus = pDevice->deviceBasic.nStatus;
						strcpy_s(szFactory, sizeof(szFactory), pDevice->deviceBasic.szFactoryId);
						strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
					}
					else {
						nErr = E_INVALIDDEVICE;
					}
					pthread_mutex_unlock(&g_mutex4DevList);
				}
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%u,\"session\":\"%s\",\"guarderId\":\"%s\","
				"\"deviceId\":\"%s\",\"battery\":%u,\"status\":%u}", access_service::E_CMD_BIND_REPLY, nErr, pBindInfo_->szSesssion,
				szGuarder, pBindInfo_->szDeviceId, usDeviceBattery, usDeviceStatus);
			sendExpressMessageToClient(szReply, pClientId_);
			if (nErr == E_OK) {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
				if (pLink) {
					strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), pBindInfo_->szDeviceId);
					strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactory);
					strcpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg);
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				//pthread_mutex_lock(&g_mutex4GuarderList);
				//auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				//if (pGuarder) {
				//	strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pBindInfo_->szDeviceId);
				//	pGuarder->usState = STATE_GUARDER_BIND;
				//	pGuarder->ullActiveTime = time(NULL);
				//}
				//pthread_mutex_unlock(&g_mutex4GuarderList);
				//pthread_mutex_lock(&g_mutex4DevList);
				//auto pDevice = (escort::WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
				//if (pDevice) {
				//	strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), szGuarder);
				//}
				//pthread_mutex_unlock(&g_mutex4DevList);
				//if (!bBindAlready) {
				//	char szMsg[512] = { 0 };
				//	sprintf_s(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%u,\"sequence\":%u,\"datetime\":\"%s\","
				//		"\"report\":[{\"subType\":%u,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\",\"session\":\"%s\"}]}",
				//		MSG_SUB_REPORT, getNextRequestSequence(), pBindInfo_->szDateTime, SUB_REPORT_DEVICE_BIND, szFactory, 
				//		pBindInfo_->szDeviceId, szGuarder, pBindInfo_->szSesssion);
				//	sendDataViaInteractor_v2(szMsg, (uint32_t)strlen(szMsg));
				//	char szMsgTopic[64] = { 0 };
				//	sprintf_s(szMsgTopic, sizeof(szMsgTopic), "%s_%s_%s", szOrg, szFactory, pBindInfo_->szDeviceId);
				//	size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
				//	auto pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
				//	memset(pSubInfo, 0, nSubInfoSize);
				//	strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szMsgTopic);
				//	strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), szGuarder);
				//	strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), pBindInfo_->szSesssion);
				//	pSubInfo->nFormat = LOGIN_FORMAT_MQ;
				//	strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pClientId_);
				//	pthread_mutex_lock(&m_mutex4SubscribeList);
				//	zhash_update(m_subscribeList, szMsgTopic, pSubInfo);
				//	zhash_freefn(m_subscribeList, szMsgTopic, free);
				//	pthread_mutex_unlock(&m_mutex4SubscribeList);
				//	//zkSetSession(pBindInfo_->szSesssion);
				//	sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]add new subscriber for %s, clientId=%s\n",
				//		__FUNCTION__, __LINE__, szMsgTopic, pClientId_);
				//	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				//}
			}
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]bind deviceId=%s, session=%s, guarder=%s, datetime=%s, "
				"org=%s, result=%d, from=%s\n", __FUNCTION__, __LINE__, pBindInfo_->szDeviceId, pBindInfo_->szSesssion,
				szGuarder, pBindInfo_->szDateTime, szOrg, nErr, pClientId_);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else { //unbind
			bool bValidSession = false;
			bool bValidGuarder = false;
			if (strlen(pBindInfo_->szSesssion)) {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
				if (pLink) {
					pLink->nActivated = 1;
					pLink->ulActivateTime = time(NULL);
					strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
					bValidSession = true;
					pLink->nLinkFormat = LOGIN_FORMAT_MQ;
					if (strcmp(pLink->szEndpoint, pClientId_) != 0) {
						strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
					}
				}
				else {
					nErr = E_INVALIDSESSION;
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
			}
			if (bValidSession) {
				if (strlen(szGuarder)) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						if (strlen(pGuarder->szTaskId)) {
							nErr = E_GUARDERINDUTY;
						}
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}
				if (strlen(pBindInfo_->szDeviceId)) {
					pthread_mutex_lock(&g_mutex4DevList);
					auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
					if (pDevice) {
						if ((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD
							|| (pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							nErr = E_DEVICEINDUTY;
						}
						else {
							pDevice->szBindGuard[0] = '\0';
						}
						strcpy_s(szFactory, sizeof(szFactory), pDevice->deviceBasic.szFactoryId);
						strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
					}
					else {
						nErr = E_INVALIDDEVICE;
					}
					pthread_mutex_unlock(&g_mutex4DevList);
				}
				
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\"}",
				access_service::E_CMD_UNBIND_REPLY, nErr, pBindInfo_->szSesssion);
			sendExpressMessageToClient(szReply, pClientId_);
			if (nErr == E_OK) {
				pthread_mutex_lock(&m_mutex4LinkList);
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pBindInfo_->szSesssion);
				if (pLink) {
					pLink->szDeviceId[0] = '\0';
					pLink->szFactoryId[0] = '\0';
					pLink->szOrg[0] = '\0';
				}
				pthread_mutex_unlock(&m_mutex4LinkList);
				//pthread_mutex_lock(&g_mutex4GuarderList);
				//auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				//if (pGuarder) {
				//	pGuarder->szBindDevice[0] = '\0';
				//	pGuarder->usState = STATE_GUARDER_FREE;
				//	pGuarder->ullActiveTime = time(NULL);
				//}
				//pthread_mutex_unlock(&g_mutex4GuarderList);
				//pthread_mutex_lock(&g_mutex4DevList);
				//auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pBindInfo_->szDeviceId);
				//if (pDevice) {
				//	pDevice->szBindGuard[0] = '\0';
				//}
				//pthread_mutex_unlock(&g_mutex4DevList);
				//char szTopic[64] = { 0 };
				//sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, pBindInfo_->szDeviceId);
				//pthread_mutex_lock(&m_mutex4SubscribeList);
				//zhash_delete(m_subscribeList, szTopic);
				//pthread_mutex_unlock(&m_mutex4SubscribeList);
				//zkSetSession(pBindInfo_->szSesssion);
			}
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]unbind deviceId=%s, session=%s, guarder=%s, datetime=%s, "
				"org=%s, result=%d, from=%s\n", __FUNCTION__, __LINE__, pBindInfo_->szDeviceId, pBindInfo_->szSesssion, szGuarder, 
				pBindInfo_->szDateTime, szOrg, nErr, pClientId_);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void AccessService::handleExpressAppSubmitTask(access_service::AppSubmitTaskInfo * pSubTaskInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pSubTaskInfo_) {
		char szReply[256] = { 0 };
		char szLog[512] = { 0 };
		int nErr = E_OK;
		char szGuarder[20] = { 0 };
		char szHandset[64] = { 0 };
		char szTaskId[16] = { 0 };
		char szDeviceId[20] = { 0 };
		char szFactory[4] = { 0 };
		char szOrg[40] = { 0 };
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"\"}",
				access_service::E_CMD_TASK_REPLY, E_SERVER_RESOURCE_NOT_READY, pSubTaskInfo_->szSession);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv submit task from %s, session=%s, server "
				"not ready\n", __FUNCTION__, __LINE__, pClientId_, pSubTaskInfo_->szSession);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			return;
		}
		bool bValidSession = false;
		bool bValidGuarder = false;
		pthread_mutex_lock(&m_mutex4LinkList);
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pSubTaskInfo_->szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = time(NULL);
			pLink->nLinkFormat = LOGIN_FORMAT_MQ;
			strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			strcpy_s(szHandset, sizeof(szHandset), pLink->szHandset);
			strcpy_s(szDeviceId, sizeof(szDeviceId), pLink->szDeviceId);
			bValidSession = true;
		}
		else {
			nErr = E_INVALIDSESSION;
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bValidSession) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY) {
					nErr = E_GUARDERINDUTY;
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (strlen(szDeviceId)) {
			pthread_mutex_lock(&g_mutex4DevList);
			auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDevice) {
				strcpy_s(szFactory, sizeof(szFactory), pDevice->deviceBasic.szFactoryId);
				strcpy_s(szOrg, sizeof(szOrg), pDevice->deviceBasic.szOrgId);
			}
			pthread_mutex_unlock(&g_mutex4DevList);
		}
		else {
			nErr = E_INVALIDDEVICE;
		}
		if (nErr == E_OK) {
			generateTaskId(szTaskId, sizeof(szTaskId));
			size_t nTaskSize = sizeof(escort::EscortTask);
			auto pTask = (escort::EscortTask *)zmalloc(nTaskSize);
			memset(pTask, 0, nTaskSize);
			strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), szTaskId);
			strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), szDeviceId);
			strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), szFactory);
			strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), szGuarder);
			strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), szOrg);
			if (strlen(szHandset)) {
				pTask->nTaskMode = 1;
				strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), szHandset);
			}
			pTask->nTaskState = 0;
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = pSubTaskInfo_->usTaskLimit;
			pTask->nTaskType = pSubTaskInfo_->usTaskType - 1;
			if (strlen(pSubTaskInfo_->szDestination)) {
				strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pSubTaskInfo_->szDestination);
			}
			if (strlen(pSubTaskInfo_->szTarget)) {
				strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pSubTaskInfo_->szTarget);
			}
			strcpy_s(pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime), pSubTaskInfo_->szDatetime);

			pthread_mutex_lock(&g_mutex4TaskList);
			//zhash_insert(g_taskList, szTaskId, pTask);
			zhash_update(g_taskList, szTaskId, pTask);
			zhash_freefn(g_taskList, szTaskId, free);
			pthread_mutex_unlock(&g_mutex4TaskList);

			pthread_mutex_lock(&g_mutex4GuarderList);
			auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				pGuarder->ullActiveTime = time(NULL);
				pGuarder->usLoginFormat = LOGIN_FORMAT_TCP_SOCKET;
				strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), szTaskId);
				pGuarder->usState = STATE_GUARDER_DUTY;
				strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), szDeviceId);
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);

			pthread_mutex_lock(&g_mutex4DevList);
			auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
			if (pDevice) {
				changeDeviceStatus(DEV_GUARD, pDevice->deviceBasic.nStatus);
				strcpy_s(pDevice->szBindGuard, sizeof(pDevice->szBindGuard), szGuarder);
			}
			pthread_mutex_unlock(&g_mutex4DevList);

			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
				access_service::E_CMD_TASK_REPLY, nErr, pSubTaskInfo_->szSession, szTaskId);
			sendExpressMessageToClient(szReply, pClientId_);

			char szBody[512] = { 0 };
			sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
				"\"report\":[{\"subType\":%u,\"taskId\":\"%s\",\"taskType\":%u,\"limit\":%u,\"factoryId\":\"%s\",\"deviceId\":\"%s\","
				"\"guarder\":\"%s\",\"destination\":\"%s\",\"target\":\"%s\",\"handset\":\"%s\",\"session\":\"%s\"}]}", MSG_SUB_REPORT,
				getNextRequestSequence(), pSubTaskInfo_->szDatetime, SUB_REPORT_TASK, szTaskId, pSubTaskInfo_->usTaskType - 1, 
				pSubTaskInfo_->usTaskLimit, szFactory, szDeviceId, szGuarder, pSubTaskInfo_->szDestination, pSubTaskInfo_->szTarget,
				szHandset, pSubTaskInfo_->szSession);
			sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));

			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pSubTaskInfo_->szSession);
			if (pLink) {
				strcpy_s(pLink->szTaskId, sizeof(pLink->szTaskId), szTaskId);
				strcpy_s(pLink->szDeviceId, sizeof(pLink->szDeviceId), szDeviceId);
				strcpy_s(pLink->szFactoryId, sizeof(pLink->szFactoryId), szFactory);
				strcpy_s(pLink->szOrg, sizeof(pLink->szOrg), szOrg);
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			zkSetSession(pSubTaskInfo_->szSession);

			char szTopic[80] = { 0 };
			sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", szOrg, szFactory, szDeviceId);
			size_t nSubInfoSize = sizeof(access_service::AppSubscribeInfo);
			access_service::AppSubscribeInfo * pSubInfo = (access_service::AppSubscribeInfo *)zmalloc(nSubInfoSize);
			memset(pSubInfo, 0, nSubInfoSize);
			pSubInfo->nFormat = LOGIN_FORMAT_MQ;
			strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pClientId_);
			strcpy_s(pSubInfo->szGuarder, sizeof(pSubInfo->szGuarder), szGuarder);
			strcpy_s(pSubInfo->szSession, sizeof(pSubInfo->szSession), pSubTaskInfo_->szSession);
			strcpy_s(pSubInfo->szSubFilter, sizeof(pSubInfo->szSubFilter), szTopic);
			pthread_mutex_lock(&m_mutex4SubscribeList);
			zhash_update(m_subscribeList, szTopic, pSubInfo);
			zhash_freefn(m_subscribeList, szTopic, free);
			pthread_mutex_unlock(&m_mutex4SubscribeList);



		}
		else {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
				access_service::E_CMD_TASK_REPLY, nErr, pSubTaskInfo_->szSession, szTaskId);
			sendExpressMessageToClient(szReply, pClientId_);
		}
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]submit task from %s, session=%s, type=%u, limit=%u,"
			"target=%s, datetime=%s, result=%d, handset=%s\n", __FUNCTION__, __LINE__, pClientId_,
			pSubTaskInfo_->szSession, pSubTaskInfo_->usTaskType, pSubTaskInfo_->usTaskLimit, pSubTaskInfo_->szTarget,
			pSubTaskInfo_->szDatetime, nErr, szHandset);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppCloseTask(access_service::AppCloseTaskInfo * pCloseTaskInfo_, 
	unsigned long long ullMsgTime_, const char * pClientId_)
{
	if (pCloseTaskInfo_) {
		char szLog[512] = { 0 };
		int nErr = E_OK;
		bool bValidLink = false;
		bool bValidTask = false;
		char szGuarder[20] = { 0 };
		char szReply[256] = { 0 };
		char szFilter[64] = { 0 };
		char szDeviceId[20] = { 0 };
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
				access_service::E_CMD_TASK_CLOSE_REPLY, E_SERVER_RESOURCE_NOT_READY, pCloseTaskInfo_->szSession,
				pCloseTaskInfo_->szTaskId);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive close task from %s, session=%s, taskId=%s"
				", server not ready\r\n", __FUNCTION__, __LINE__, pClientId_, pCloseTaskInfo_->szSession, 
				pCloseTaskInfo_->szTaskId);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}

		pthread_mutex_lock(&m_mutex4LinkList);
		if (zhash_size(m_linkList)) {
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pCloseTaskInfo_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				pLink->nLinkFormat = LOGIN_FORMAT_MQ;
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
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
			pthread_mutex_lock(&g_mutex4TaskList);
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pCloseTaskInfo_->szTaskId);
			if (pTask) {
				strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				if (strlen(pTask->szGuarder) > 0 && strcmp(pTask->szGuarder, szGuarder) == 0) {
					if (m_nTaskCloseCheckStatus) {
						if (pTask->nTaskFlee == 1) {
							nErr = E_TASK_IN_FLEE_STATUS;
						}
						else {
							bValidTask = true;
							pTask->nTaskState = (pCloseTaskInfo_->nCloseType == 0) ? 2 : 1;
							strcpy_s(pTask->szTaskStopTime, sizeof(pTask->szTaskStopTime), pCloseTaskInfo_->szDatetime);
						}
					}
					else {
						bValidTask = true;
						pTask->nTaskState = (pCloseTaskInfo_->nCloseType == 0) ? 2 : 1;
						strcpy_s(pTask->szTaskStopTime, sizeof(pTask->szTaskStopTime), pCloseTaskInfo_->szDatetime);
					}
				}
				else {
					nErr = E_GUARDERNOTMATCH;
				}
			}
			else {
				nErr = E_INVALIDTASK;
			}
			
			pthread_mutex_unlock(&g_mutex4TaskList);
		}
		if (bValidTask && strlen(szDeviceId)) {
			if (m_nEnableTaskLooseCheck) {
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					if (pDevice->deviceBasic.nLooseStatus == 1) {
						nErr = E_TASK_IN_LOOSE_STATUS;
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
			access_service::E_CMD_TASK_CLOSE_REPLY, nErr, pCloseTaskInfo_->szSession, pCloseTaskInfo_->szTaskId);
		sendExpressMessageToClient(szReply, pClientId_);
		if (nErr == E_OK) {
			char szBody[256] = { 0 };
			sprintf_s(szBody, sizeof(szBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
				"\"report\":[{\"subType\":%d,\"taskId\":\"%s\",\"closeType\":%d,\"session\":\"%s\"}]}", MSG_SUB_REPORT, 
				getNextRequestSequence(), pCloseTaskInfo_->szDatetime, SUB_REPORT_TASK_CLOSE, pCloseTaskInfo_->szTaskId, 
				pCloseTaskInfo_->nCloseType, pCloseTaskInfo_->szSession);
			sendDataViaInteractor_v2(szBody, (uint32_t)strlen(szBody));

			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pCloseTaskInfo_->szSession);
			if (pLink) {
				pLink->szTaskId[0] = '\0';
				pLink->szDeviceId[0] = '\0';
				pLink->szFactoryId[0] = '\0';
				pLink->szOrg[0] = '\0';
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			
			char szTopic[80] = { 0 };
			pthread_mutex_lock(&g_mutex4DevList);
			if (zhash_size(g_deviceList)) {
				WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDev) {
					pDev->deviceBasic.nStatus = DEV_ONLINE;
					if (pDev->deviceBasic.nLooseStatus == 1) {
						pDev->deviceBasic.nStatus += DEV_LOOSE;
					}
					if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
						pDev->deviceBasic.nStatus += DEV_LOWPOWER;
					}
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pDev->deviceBasic.szOrgId, pDev->deviceBasic.szFactoryId,
						pDev->deviceBasic.szDeviceId);
				}
			}
			pthread_mutex_unlock(&g_mutex4DevList);

			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				pGuarder->ullActiveTime = time(NULL);
				pGuarder->usState = STATE_GUARDER_FREE;
				pGuarder->szTaskId[0] = '\0';
				pGuarder->szBindDevice[0] = '\0';
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
			pthread_mutex_lock(&g_mutex4TaskList);
			zhash_delete(g_taskList, pCloseTaskInfo_->szTaskId);
			pthread_mutex_unlock(&g_mutex4TaskList);

			zkSetSession(pCloseTaskInfo_->szSession);

			if (strlen(szTopic)) {
				pthread_mutex_lock(&m_mutex4SubscribeList);
				zhash_delete(m_subscribeList, szTopic);
				pthread_mutex_unlock(&m_mutex4SubscribeList);
			}

		}
		//publishNotifyMessage(taskInfo.szSession, pEndpoint, szDeviceId);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]submit close task from %s, session=%s, taskId=%s, datetime=%s,"
			" deviceId=%s, guarder=%s, result=%d\n", __FUNCTION__, __LINE__, pClientId_, pCloseTaskInfo_->szSession, 
			pCloseTaskInfo_->szTaskId, pCloseTaskInfo_->szDatetime, szDeviceId, szGuarder, nErr);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppPosition(access_service::AppPositionInfo * pPosInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pPosInfo_) {
		char szLog[512] = { 0 };
		bool bValidSession = false;
		char szGuarder[20] = { 0 };
		char szDeviceId[20] = { 0 };
		char szReply[256] = { 0 };
		bool bUpdate = false;
		int nErr = E_OK;
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\",\"datetime\":\"%s\"}",
				access_service::E_CMD_POSITION_REPLY, pPosInfo_->szSession, E_SERVER_RESOURCE_NOT_READY, pPosInfo_->szTaskId,
				pPosInfo_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[message]%s[%u]recv app position from %s, session=%s, taskId=%s, datetime=%s, "
				"server not ready\n", __FUNCTION__, __LINE__, pClientId_, pPosInfo_->szSession, pPosInfo_->szTaskId,
				pPosInfo_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		else {
			char szDatetime[20] = { 0 };
			formatDatetime(ullMsgTime_, szDatetime, sizeof(szDatetime));
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pPosInfo_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				bValidSession = true;
				pLink->nLinkFormat = LOGIN_FORMAT_MQ;
				if (strcmp(pLink->szEndpoint, pClientId_) != 0) {
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
					bUpdate = true;
				}
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4TaskList);
				auto pTask = (escort::EscortTask *)zhash_lookup(g_taskList, pPosInfo_->szTaskId);
				if (pTask) {
					strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				}
				else {
					nErr = E_INVALIDTASK;
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"session\":\"%s\",\"retcode\":%d,\"taskId\":\"%s\",\"datetime\":\"%s\"}",
				access_service::E_CMD_POSITION_REPLY, pPosInfo_->szSession, nErr, pPosInfo_->szTaskId, pPosInfo_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv app position from %s, session=%s, taskId=%s, guarder=%s, "
				"deviceId=%s, lat=%f, lon=%f, coordinate=%u, datetime=%s, result=%d\n", __FUNCTION__, __LINE__, pClientId_,
				pPosInfo_->szSession, pPosInfo_->szTaskId, szGuarder, szDeviceId, pPosInfo_->dLat, pPosInfo_->dLng, pPosInfo_->nCoordinate,
				pPosInfo_->szDatetime, nErr);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (nErr == E_OK) {
				unsigned short usStatus = 0;
				unsigned short usBattery = 0;
				unsigned short usOnline = 0;
				char szTopic[80] = { 0 };
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					pDevice->guardPosition.dLatitude = pPosInfo_->dLat;
					pDevice->guardPosition.dLngitude = pPosInfo_->dLng;
					pDevice->guardPosition.nCoordinate = pPosInfo_->nCoordinate;
					usStatus = pDevice->deviceBasic.nStatus;
					usBattery = pDevice->deviceBasic.nBattery;
					usOnline = pDevice->deviceBasic.nOnline;
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pDevice->deviceBasic.szOrgId, pDevice->deviceBasic.szFactoryId,
						pDevice->deviceBasic.szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->usLoginFormat = LOGIN_FORMAT_MQ;
					pGuarder->ullActiveTime = time(NULL);
					if (strcmp(pGuarder->szLink, pClientId_) != 0) {
						strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pClientId_);
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);

				char szMsg[256] = { 0 };
				sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%u,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\",\"battery\":%hu,"
					"\"status\":%hu,\"online\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, pPosInfo_->szSession,
					access_service::E_NOTIFY_DEVICE_INFO, szDeviceId, usBattery, usStatus, usOnline, szDatetime);
				sendExpressMessageToClient(szMsg, pClientId_);

				if (bUpdate) {
					if (strlen(szTopic)) {
						pthread_mutex_lock(&m_mutex4SubscribeList);
						auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szTopic);
						if (pSubInfo) {
							strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pClientId_);
							pSubInfo->nFormat = LOGIN_FORMAT_MQ;
						}
						pthread_mutex_unlock(&m_mutex4SubscribeList);
					}
				}
			}
		}
	}
}

void AccessService::handleExpressAppFlee(access_service::AppSubmitFleeInfo * pFleeInfo_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pFleeInfo_) {
		char szLog[512] = { 0 };
		char szReply[512] = { 0 };
		char szGuarder[20] = { 0 };
		char szDeviceId[20] = { 0 };
		int nErr = E_OK;
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
				pFleeInfo_->nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
				E_SERVER_RESOURCE_NOT_READY, pFleeInfo_->szSession, pFleeInfo_->szTaskId);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive flee from %s, mode=%d, session=%s, "
				"server not ready\r\n", __FUNCTION__, __LINE__, pClientId_, pFleeInfo_->nMode, pFleeInfo_->szSession);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		else {
			bool bValidSession = false;
			bool bUpdate = false;
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pFleeInfo_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				bValidSession = true;
				pLink->nLinkFormat = LOGIN_FORMAT_MQ;
				if (strcmp(pLink->szEndpoint, pClientId_) != 0) {
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
				}
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			bool bValidTask = false;
			if (strlen(pFleeInfo_->szTaskId)) {
				pthread_mutex_lock(&g_mutex4TaskList);
				auto pTask = (escort::EscortTask *)zhash_lookup(g_taskList, pFleeInfo_->szTaskId);
				if (pTask) {
					bValidTask = true;
					strcpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId);
				}
				else {
					nErr = E_INVALIDTASK;
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			if (strlen(szDeviceId)) {
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					if (pDevice->deviceBasic.nOnline == 0) {
						nErr = E_UNABLEWORKDEVICE;
					}
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\",\"taskId\":\"%s\"}",
				pFleeInfo_->nMode == 0 ? access_service::E_CMD_FLEE_REPLY : access_service::E_CMD_FLEE_REVOKE_REPLY,
				nErr, pFleeInfo_->szSession, pFleeInfo_->szTaskId);
			sendExpressMessageToClient(szReply, pClientId_);

			if (nErr == E_OK) {
				char szTopic[80] = { 0 };
				bool bNotify = false;
				pthread_mutex_lock(&g_mutex4TaskList);
				auto pTask = (escort::EscortTask *)zhash_lookup(g_taskList, pFleeInfo_->szTaskId);
				if (pTask) {
					if (pFleeInfo_->nMode == 0) { //on
						if (pTask->nTaskFlee == 0) {
							pTask->nTaskFlee = 1;
							bNotify = true;
						}
					}
					else { //off
						if (pTask->nTaskFlee == 1) {
							pTask->nTaskFlee = 0;
							bNotify = true;
						}
					}
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
				if (pDevice) {
					if (pFleeInfo_->nMode == 0) { //on
						if ((pDevice->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD) {
							changeDeviceStatus(DEV_FLEE, pDevice->deviceBasic.nStatus);
						}
					}
					else {
						if ((pDevice->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE) {
							changeDeviceStatus(DEV_GUARD, pDevice->deviceBasic.nStatus);
						}
					}
					sprintf_s(szTopic, sizeof(szTopic), "%s_%s_%s", pDevice->deviceBasic.szOrgId, pDevice->deviceBasic.szFactoryId,
						pDevice->deviceBasic.szDeviceId);
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->ullActiveTime = time(NULL);
					pGuarder->usLoginFormat = LOGIN_FORMAT_MQ;
					if (strcmp(pGuarder->szLink, pClientId_) != 0) {
						strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pClientId_);
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
				if (m_nTaskFleeReplicatedReport) {
					bNotify = true;
				}
				if (bNotify) {
					char szMsg[512] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,\"datetime\":\"%s\","
						"\"report\":[{\"subType\":%u,\"taskId\":\"%s\",\"session\":\"%s\"}]}", MSG_SUB_REPORT, getNextRequestSequence(),
						pFleeInfo_->szDatetime, pFleeInfo_->nMode == 0 ? SUB_REPORT_DEVICE_FLEE: SUB_REPORT_DEVICE_FLEE_REVOKE, 
						pFleeInfo_->szTaskId, pFleeInfo_->szSession);
					sendDataViaInteractor_v2(szMsg, (uint32_t)strlen(szMsg));
				}
				if (bUpdate && strlen(szTopic)) {
					pthread_mutex_lock(&m_mutex4SubscribeList);
					auto pSubInfo = (access_service::AppSubscribeInfo *)zhash_lookup(m_subscribeList, szTopic);
					if (pSubInfo) {
						pSubInfo->nFormat = LOGIN_FORMAT_MQ;
						strcpy_s(pSubInfo->szAccSource, sizeof(pSubInfo->szAccSource), pClientId_);
					}
					pthread_mutex_unlock(&m_mutex4SubscribeList);
				}
			}
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv %s from %s, session=%s, taskId=%s, deviceId=%s,"
				" datetime=%s, result=%d\n", __FUNCTION__, __LINE__, pFleeInfo_->nMode == 0 ? "FLEE" : "FLEE REVOKE",
				pClientId_, pFleeInfo_->szSession, pFleeInfo_->szTaskId, szDeviceId, pFleeInfo_->szDatetime, nErr);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void AccessService::handleExpressAppKeepAlive(access_service::AppKeepAlive * pKeepAlive_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pKeepAlive_) {
		char szLog[512] = { 0 };
		int nErr = E_OK;
		char szGuarder[20] = { 0 };
		char szReply[256] = { 0 };
		char szDatetime[20] = { 0 };
		formatDatetime(ullMsgTime_, szDatetime, sizeof(szDatetime));
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"session\":\"%s\",\"seq\":%u,\"retcode\":%d,\"datetime\":\"%s\"}",
				access_service::E_CMD_KEEPALIVE_REPLY, pKeepAlive_->szSession, pKeepAlive_->uiSeq, E_SERVER_RESOURCE_NOT_READY,
				pKeepAlive_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv keep alive from %s, session=%s, seq=%u, datetime=%s, "
				"server not ready\n", __FUNCTION__, __LINE__, pClientId_, pKeepAlive_->szSession, pKeepAlive_->uiSeq,
				pKeepAlive_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		else {
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pKeepAlive_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%u,\"retcode\":%d}",
				access_service::E_CMD_KEEPALIVE_REPLY, pKeepAlive_->szSession, pKeepAlive_->uiSeq, nErr);
			sendExpressMessageToClient(szReply, pClientId_);
			if (nErr == E_OK) {
				char szDeviceId[20] = { 0 };
				if (strlen(szGuarder)) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
					if (pGuarder) {
						pGuarder->ullActiveTime = time(NULL);
						strcpy_s(szDeviceId, sizeof(szDeviceId), pGuarder->szBindDevice);
					}
					pthread_mutex_unlock(&g_mutex4GuarderList);
				}
				if (strlen(szDeviceId)) {
					unsigned short usBattery = 0;
					unsigned short usStatus = 0;
					unsigned short usOnline = 0;
					pthread_mutex_lock(&g_mutex4DevList);
					auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
					if (pDevice) {
						usBattery = pDevice->deviceBasic.nBattery;
						usStatus = pDevice->deviceBasic.nStatus;
						usOnline = pDevice->deviceBasic.nOnline;
					}
					pthread_mutex_unlock(&g_mutex4DevList);

					char szMsg[256] = { 0 };
					sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":%u,\"session\":\"%s\",\"msgType\":%d,\"deviceId\":\"%s\",\"battery\":%hu,"
						"\"status\":%hu,\"online\":%hu,\"datetime\":\"%s\"}", access_service::E_CMD_MSG_NOTIFY, pKeepAlive_->szSession,
						access_service::E_NOTIFY_DEVICE_INFO, szDeviceId, usBattery, usStatus, usOnline, szDatetime);
					sendExpressMessageToClient(szMsg, pClientId_);
				}

				char szUploadMsg[512] = { 0 };
				sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
					"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"session\":\"%s\",\"loginFormat\":%u}]}",
					MSG_SUB_REPORT, getNextRequestSequence(), szDatetime, SUB_REPORT_GUARDER_ALIVE, szGuarder, pKeepAlive_->szSession,
					LOGIN_FORMAT_MQ);
				sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));
			}
		}
	}
}

void AccessService::handleExpressAppModifyAccountPassword(access_service::AppModifyPassword * pModify_, 
	unsigned long long ullMsgTime_, const char * pClientId_)
{
	if (pModify_) {
		char szLog[512] = { 0 };
		char szReply[256] = { 0 };
		int nErr = E_OK;
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"datetime\":\"%s\"}",
				access_service::E_CMD_MODIFY_PASSWD_REPLY, pModify_->szSession, E_SERVER_RESOURCE_NOT_READY, pModify_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive modify password from %s, session=%s, server not "
				"ready\r\n", __FUNCTION__, __LINE__, pClientId_, pModify_->szSession);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			return;
		}
		bool bValidLink = false;
		bool bUpdateLink = false;
		char szLastEndpoint[32] = { 0 };
		char szGuarder[20] = { 0 };
		char szSubTopic[64] = { 0 };
		bool bHaveSubscribe = false;
		char szDeviceId[20] = { 0 };
		pthread_mutex_lock(&m_mutex4LinkList);
		auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pModify_->szSession);
		if (pLink) {
			pLink->nActivated = 1;
			pLink->ulActivateTime = time(NULL);
			bValidLink = true;
		}
		else {
			nErr = E_INVALIDSESSION;
		}
		pthread_mutex_unlock(&m_mutex4LinkList);
		if (bValidLink) {
			if (strcmp(pModify_->szCurrPassword, pModify_->szNewPassword) != 0) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->ullActiveTime = time(NULL);
					if (strcmp(pGuarder->szPassword, pModify_->szCurrPassword) == 0) {
						strcpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword), pModify_->szNewPassword);
					}
					else {
						nErr = E_INVALIDPASSWD;
						sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s, datetime=%s, "
							"current password is incorrect, can not execute modify, input=%s\r\n", __FUNCTION__, __LINE__, 
							pClientId_, szGuarder, pModify_->szDatetime, pModify_->szCurrPassword);
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}
			else {
				nErr = E_INVALIDPASSWD;
				sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]modify passwd from %s, account=%s, "
					"datetime=%s, new password is same with the current password, nothing to modify\r\n",
					__FUNCTION__, __LINE__, pClientId_, szGuarder, pModify_->szDatetime);
				LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
			}
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"retcode\":%d,\"datetime\":\"%s\"}",
			access_service::E_CMD_MODIFY_PASSWD_REPLY, pModify_->szSession, nErr, pModify_->szDatetime);
		sendExpressMessageToClient(szReply, pClientId_);
		if (nErr == E_OK) {
			char szMsgBody[512] = { 0 };
			sprintf_s(szMsgBody, sizeof(szMsgBody), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%u,"
				"\"datetime\":\"%s\",\"report\":[{\"subType\":%d,\"guarder\":\"%s\",\"currPasswd\":\"%s\",\"newPasswd\":\"%s\","
				"\"session\":\"%s\"}]}", MSG_SUB_REPORT, getNextRequestSequence(), pModify_->szDatetime, 
				SUB_REPORT_MODIFY_USER_PASSWD, szGuarder, pModify_->szCurrPassword, pModify_->szNewPassword, pModify_->szSession);
			sendDataViaInteractor_v2(szMsgBody, (uint32_t)strlen(szMsgBody));
		}
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]report modify password from %s, session=%s, seq=%u, "
			"datetime=%s, result=%d\r\n", __FUNCTION__, __LINE__, pClientId_, pModify_->szSession, pModify_->uiSeq,
			pModify_->szDatetime, nErr);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppQueryTask(access_service::AppQueryTask * pQryTask_, unsigned long long ullMsgTime_,
	const char * pClientId_)
{
	if (pQryTask_) {
		char szLog[512] = { 0 };
		int nErr = E_OK;
		char szGuarder[20] = { 0 };
		char szHandset[64] = { 0 };
		if (!getLoadSessionFlag()) {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\","
				"\"taskInfo\":[]}", access_service::E_CMD_QUERY_TASK_REPLY, E_SERVER_RESOURCE_NOT_READY, 
				pQryTask_->szSession, pQryTask_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv query task from %s, session=%s, taskId=%s, datetime=%s,"
				" service not ready\n", __FUNCTION__, __LINE__, pClientId_, pQryTask_->szSession, pQryTask_->szTaskId,
				pQryTask_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			return;
		}
		else {
			bool bValidSession = false;
			escort::EscortTask * pCurrTask = NULL;
			char szTaskInfo[512] = { 0 };
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pQryTask_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				strcpy_s(szHandset, sizeof(szHandset), pLink->szHandset);
				bValidSession = true;
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4TaskList);
				auto pTask = (EscortTask *)zhash_lookup(g_taskList, pQryTask_->szTaskId);
				if (pTask) {
					size_t nSize = sizeof(escort::EscortTask);
					pCurrTask = new escort::EscortTask();
					memcpy_s(pCurrTask, nSize, pTask, nSize);
				}
				else {
					nErr = E_INVALIDTASK;
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			if (strlen(szGuarder)) {
				pthread_mutex_lock(&g_mutex4GuarderList);
				auto pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					pGuarder->ullActiveTime = time(NULL);
					strcpy_s(pGuarder->szLink, sizeof(pGuarder->szLink), pClientId_);
				}
				pthread_mutex_unlock(&g_mutex4GuarderList);
			}
			if (pCurrTask) {
				unsigned short usStatus = 0;
				unsigned short usBattery = 0;
				unsigned short usOnline = 0;
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pCurrTask->szDeviceId);
				if (pDevice) {
					usOnline = pDevice->deviceBasic.nOnline;
					usStatus = pDevice->deviceBasic.nStatus;
					usBattery = pDevice->deviceBasic.nBattery;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
				sprintf_s(szTaskInfo, sizeof(szTaskInfo), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"type\":%u,\"limit\":%u,"
					"\"destination\":\"%s\",\"target\":\"%s\",\"startTime\":\"%s\",\"deviceState\":%hu,\"online\":%hu,"
					"\"battery\":%hu,\"handset\":\"%s\"}", pCurrTask->szTaskId, pCurrTask->szDeviceId, pCurrTask->nTaskType,
					pCurrTask->nTaskLimitDistance, pCurrTask->szDestination, pCurrTask->szTarget, pCurrTask->szTaskStartTime,
					usStatus, usOnline, usBattery, szHandset);
				delete pCurrTask;
				pCurrTask = NULL;
			}
			size_t nReplyLen = strlen(szTaskInfo) + 256;
			char * pReply = new char[nReplyLen + 1];
			sprintf_s(pReply, nReplyLen + 1, "{\"cmd\":%u,\"retcode\":%d,\"session\":\"%s\",\"datetime\":\"%s\","
				"\"taskInfo\":[%s]}", access_service::E_CMD_QUERY_TASK_REPLY, nErr, pQryTask_->szSession, pQryTask_->szDatetime,
				szTaskInfo);
			sendExpressMessageToClient(pReply, pClientId_);
			delete[] pReply;
			pReply = NULL;
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv query task from %s, session=%s, taskId=%s, datetime=%s,"
				" retcode=%d\n", __FUNCTION__, __LINE__, pClientId_, pQryTask_->szSession, pQryTask_->szTaskId, 
				pQryTask_->szDatetime, nErr);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void AccessService::handleExpressAppDeviceCommand(access_service::AppDeviceCommandInfo * pCmdInfo_, 
	const char * pClientId_)
{
	if (pCmdInfo_) {
		char szLog[512] = { 0 };
		int nErr = E_OK;
		char szReply[256] = { 0 };
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%u,\"retcode\":%d,\"datetime\":\"%s\","
				"\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY, pCmdInfo_->szSession, pCmdInfo_->nSeq, 
				E_SERVER_RESOURCE_NOT_READY, pCmdInfo_->szDatetime, pCmdInfo_->szDeviceId);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]receive device command from %s, session=%s, seq=%u, "
				"datetime=%s, deviceId=%s, server not ready, wait to load session\r\n", __FUNCTION__, __LINE__, pClientId_, 
				pCmdInfo_->szSession, pCmdInfo_->nSeq, pCmdInfo_->szDatetime, pCmdInfo_->szDeviceId);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			return;
		}
		if (strlen(pCmdInfo_->szSession) > 0 && strlen(pCmdInfo_->szDeviceId) && pCmdInfo_->nParam1 > 0) {
			bool bValidLink = false;
			bool bValidDevice = false;
			char szSubTopic[64] = { 0 };
			char szGuarder[20] = { 0 };
			pthread_mutex_lock(&m_mutex4LinkList);
			if (zhash_size(m_linkList)) {
				auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pCmdInfo_->szSession);
				if (pLink) {
					bValidLink = true;
					pLink->nActivated = 1;
					pLink->ulActivateTime = time(NULL);
					pLink->nLinkFormat = LOGIN_FORMAT_MQ;
					strcpy_s(pLink->szEndpoint, sizeof(pLink->szEndpoint), pClientId_);
					strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				}
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidLink) {
				pthread_mutex_lock(&g_mutex4DevList);
				WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pCmdInfo_->szDeviceId);
				if (pDevice) {
					sprintf_s(szSubTopic, sizeof(szSubTopic), "%s_%s_%s", pDevice->deviceBasic.szOrgId,
						pDevice->deviceBasic.szFactoryId, pDevice->deviceBasic.szDeviceId);
					if (pDevice->deviceBasic.nOnline == 0) {
						nErr = E_UNABLEWORKDEVICE;
					}
					else {
						if (strlen(pDevice->szBindGuard)) {
							bValidDevice = true;
						}
						else {
							nErr = E_DEVICENOTMATCH;
						}
					}
				}
				else {
					nErr = E_INVALIDDEVICE;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			if (bValidDevice) {
				int nUploadCmd = 0;
				if (pCmdInfo_->nParam1 == access_service::E_DEV_CMD_ALARM) {
					nUploadCmd = PROXY_NOTIFY_DEVICE_FLEE;
				}
				else if (pCmdInfo_->nParam1 == access_service::E_DEV_CMD_RESET) {
					nUploadCmd = PROXY_CONF_DEVICE_REBOOT;
				}
				else if (pCmdInfo_->nParam1 == access_service::E_DEV_CMD_QUERY_POSITION) {
					nUploadCmd = PROXY_QUERY_DEVICE_POSITION;
				}
				char szUploadMsg[512] = { 0 };
				sprintf_s(szUploadMsg, sizeof(szUploadMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
					"\"datetime\":\"%s\",\"request\":[{\"subType\":%d,\"factoryId\":\"%s\",\"deviceId\":\"%s\",\"param\":%d,"
					"\"session\":\"%s\"}]}", MSG_SUB_REQUEST, getNextRequestSequence(), pCmdInfo_->szDatetime, nUploadCmd, 
					pCmdInfo_->szFactoryId, pCmdInfo_->szDeviceId, pCmdInfo_->nParam2, pCmdInfo_->szSession);
				sendDataViaInteractor_v2(szUploadMsg, (uint32_t)strlen(szUploadMsg));
			}
		}
		sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"session\":\"%s\",\"seq\":%lu,\"retcode\":%d,\"datetime\":\"%s\","
			"\"deviceId\":\"%s\"}", access_service::E_CMD_DEVICE_COMMAND_REPLY, pCmdInfo_->szSession, pCmdInfo_->nSeq, nErr, 
			pCmdInfo_->szDatetime, pCmdInfo_->szDeviceId);
		sendExpressMessageToClient(szReply, pClientId_);
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]send device command from %s, deviceId=%s, session=%s, "
			"param1=%d, param2=%d, seq=%d, datetime=%s, retcode=%d\n", __FUNCTION__, __LINE__, pClientId_, 
			pCmdInfo_->szDeviceId, pCmdInfo_->szSession, pCmdInfo_->nParam1, pCmdInfo_->nParam2, pCmdInfo_->nSeq, 
			pCmdInfo_->szDatetime, nErr);
		LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	}
}

void AccessService::handleExpressAppQueryPerson(access_service::AppQueryPerson * pQryPerson_ , const char * pClientId_)
{
	if (pQryPerson_) {
		char szDatetime[20] = { 0 };
		formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
		char szDevMsg[256] = { 0 };
		sprintf_s(szDevMsg, sizeof(szDevMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"%s\",\"param2\":\"\",\"param3\":0,\"param4\":%d}",
			MSG_SUB_GETINFO, getNextRequestSequence(), szDatetime, BUFFER_PERSON, pQryPerson_->szQryPersonId, 
			pQryPerson_->nQryMode);
		sendDataViaInteractor_v2(szDevMsg, (uint32_t)strlen(szDevMsg));

		access_service::QueryPersonEvent * pEvent = new access_service::QueryPersonEvent();
		memset(pEvent, 0, sizeof(access_service::QueryPersonEvent));
		pEvent->uiQrySeq = pQryPerson_->uiQeurySeq;
		strcpy_s(pEvent->szSession, sizeof(pEvent->szSession), pQryPerson_->szSession);
		strcpy_s(pEvent->szLink, sizeof(pEvent->szLink), pClientId_);
		pEvent->usFormat = LOGIN_FORMAT_MQ;
		std::lock_guard<std::mutex> lock(m_mutex4QryPersonEventList);
		m_qryPersonEventList.emplace(pQryPerson_->uiQeurySeq, pEvent);
	}
}

void AccessService::handleExpressAppQueryTaskList(access_service::AppQueryTaskList * pQryTaskList_, const char * pClientId_)
{
	if (pQryTaskList_) {
		char szLog[512] = { 0 };
		if (!getLoadSessionFlag()) {
			char szReply[256] = { 0 };
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%d,\"orgId\":\"%s\",\"seq\":%u,\"datetime\":\"%s\",\"count\":0,"
				"\"list\":[]}", access_service::E_CMD_QUERY_TASK_LIST_REPLY, pQryTaskList_->szOrgId, pQryTaskList_->uiQrySeq, 
				pQryTaskList_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv qeury task list from %s, orgId=%s, seq=%u, datetime=%s,"
				" server not ready\n", __FUNCTION__, __LINE__, pClientId_, pQryTaskList_->szOrgId, pQryTaskList_->uiQrySeq,
				pQryTaskList_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadUser()) {
				getUserList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			if (!getLoadTask()) {
				getTaskList();
			}
			return;
		}
		else {
			std::string strTaskList;
			unsigned int uiListCount = 0;
			if (strlen(pQryTaskList_->szOrgId)) {
				std::set<std::string> orgList;
				std::string strOrgId = pQryTaskList_->szOrgId;
				pthread_mutex_lock(&g_mutex4OrgList);
				findOrgChild(strOrgId, orgList);
				pthread_mutex_unlock(&g_mutex4OrgList);
				orgList.emplace(strOrgId);
				pthread_mutex_lock(&g_mutex4TaskList);
				EscortTask * pTask = (EscortTask *)zhash_first(g_taskList);
				while (pTask) {
					if (orgList.count(pTask->szOrg) > 0) {
						char szTaskCell[512] = { 0 };
						sprintf_s(szTaskCell, sizeof(szTaskCell), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\","
							"\"target\":\"%s\",\"destination\":\"%s\",\"type\":%hu,\"limit\":%hu,\"startTime\":\"%s\"}", 
							pTask->szTaskId, pTask->szDeviceId, pTask->szGuarder, pTask->szTarget, pTask->szDestination, 
							pTask->nTaskType, pTask->nTaskLimitDistance, pTask->szTaskStartTime);
						if (strTaskList.empty()) {
							strTaskList = (std::string)szTaskCell;
						}
						else {
							strTaskList = strTaskList + "," + (std::string)szTaskCell;
						}
						uiListCount++;
					}
					pTask = (EscortTask *)zhash_next(g_taskList);
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			else {
				pthread_mutex_lock(&g_mutex4TaskList);
				uiListCount = (unsigned int)zhash_size(g_taskList);
				EscortTask * pTask = (EscortTask *)zhash_first(g_taskList);
				while (pTask) {
					char szTaskCell[512] = { 0 };
					sprintf_s(szTaskCell, sizeof(szTaskCell), "{\"taskId\":\"%s\",\"deviceId\":\"%s\",\"guarder\":\"%s\","
						"\"target\":\"%s\",\"destination\":\"%s\",\"type\":%hu,\"limit\":%hu,\"startTime\":\"%s\"}", 
						pTask->szTaskId, pTask->szDeviceId, pTask->szGuarder, pTask->szTarget, pTask->szDestination, 
						pTask->nTaskType, pTask->nTaskLimitDistance, pTask->szTaskStartTime);
					if (strTaskList.empty()) {
						strTaskList = (std::string)szTaskCell;
					}
					else {
						strTaskList = strTaskList + "," + (std::string)szTaskCell;
					}
					pTask = (EscortTask *)zhash_next(g_taskList);
				}
				pthread_mutex_unlock(&g_mutex4TaskList);
			}
			size_t nReplyLen = 256 + strTaskList.size();
			char * pReply = new char[nReplyLen];
			sprintf_s(pReply, nReplyLen, "{\"cmd\":%u,\"orgId\":\"%s\",\"seq\":%u,\"datetime\":\"%s\",\"count\":%u,"
				"\"list\":[%s]}", access_service::E_CMD_QUERY_TASK_LIST_REPLY, pQryTaskList_->szOrgId, pQryTaskList_->uiQrySeq,
				pQryTaskList_->szDatetime, uiListCount, strTaskList.c_str());
			sendExpressMessageToClient(pReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv qeury task list from %s, seq=%u, datetime=%s, "
				"count=%u\n", __FUNCTION__, __LINE__, pClientId_, pQryTaskList_->uiQrySeq, pQryTaskList_->szDatetime, 
				uiListCount);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void AccessService::handleExpressAppQueryDeviceStatus(access_service::AppQueryDeviceStatus * pQryDev_, 
	const char * pClientId_)
{
	if (pQryDev_) {
		char szLog[512] = { 0 };
		char szReply[512] = { 0 };
		unsigned short usBattery = 0;
		unsigned short usStatus = 0;
		unsigned short usOnline = 0;
		int nErr = E_OK;
		if (!getLoadSessionFlag()) {
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"session\":\"%s\",\"deviceId\":\"%s\",\"retcode\":%d,\"status\":0,"
				"\"online\":0,\"battery\":0,\"seq\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_QUERY_DEVICE_STATUS_REPLY,
				pQryDev_->szSession, pQryDev_->szDeviceId, E_SERVER_RESOURCE_NOT_READY, pQryDev_->uiQrySeq, pQryDev_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv query device from %s, deviceId=%s, seq=%u, datetime=%s, "
				"server not ready\n", __FUNCTION__, __LINE__, pClientId_, pQryDev_->szDeviceId, pQryDev_->uiQrySeq,
				pQryDev_->szDatetime);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (!getLoadOrg()) {
				getOrgList();
			}
			if (!getLoadDevice()) {
				getDeviceList();
			}
			return;
		}
		else {
			char szGuarder[20] = { 0 };
			bool bValidSession = false;
			pthread_mutex_lock(&m_mutex4LinkList);
			auto pLink = (access_service::AppLinkInfo *)zhash_lookup(m_linkList, pQryDev_->szSession);
			if (pLink) {
				pLink->nActivated = 1;
				pLink->ulActivateTime = time(NULL);
				strcpy_s(szGuarder, sizeof(szGuarder), pLink->szGuarder);
				bValidSession = true;
			}
			else {
				nErr = E_INVALIDSESSION;
			}
			pthread_mutex_unlock(&m_mutex4LinkList);
			if (bValidSession) {
				pthread_mutex_lock(&g_mutex4DevList);
				auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pQryDev_->szDeviceId);
				if (pDevice) {
					usBattery = pDevice->deviceBasic.nBattery;
					usStatus = pDevice->deviceBasic.nStatus;
					usOnline = pDevice->deviceBasic.nOnline;
				}
				else {
					nErr = E_INVALIDDEVICE;
				}
				pthread_mutex_unlock(&g_mutex4DevList);
			}
			sprintf_s(szReply, sizeof(szReply), "{\"cmd\":%u,\"session\":\"%s\",\"deviceId\":\"%s\",\"retcode\":%d,\"status\":%hu,"
				"\"online\":%hu,\"battery\":%hu,\"seq\":%u,\"datetime\":\"%s\"}", access_service::E_CMD_QUERY_DEVICE_STATUS_REPLY,
				pQryDev_->szSession, pQryDev_->szDeviceId, nErr, usStatus, usOnline, usBattery, pQryDev_->uiQrySeq, 
				pQryDev_->szDatetime);
			sendExpressMessageToClient(szReply, pClientId_);
			sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%u]recv query device status from %s, session=%s, deviceId=%s, "
				"seq=%u, datetime=%s, retcode=%d, battery=%hu, status=%hu, online=%lu\n", __FUNCTION__, __LINE__, pClientId_,
				pQryDev_->szSession, pQryDev_->szDeviceId, pQryDev_->uiQrySeq, pQryDev_->szDatetime, nErr, usBattery, usStatus,
				usOnline);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
	}
}

void * dealAppMsgThread(void * param_)
{
	AccessService * pService = (AccessService *)param_;
	if (pService) {
		char szLog[256] = { 0 };
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]start thread: %llu\r\n", __FUNCTION__,
			__LINE__, (uint64_t)pService->m_pthdAppMsg.p);
		LOG_Log(pService->m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, pService->m_usLogType);
		pService->dealAppMsg();
		sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]stop thread\r\n", __FUNCTION__,
			__LINE__);
		LOG_Log(pService->m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, pService->m_usLogType);
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealTopicMsgThread(void * param_)
{
	AccessService * pService = (AccessService *)param_;
	if (pService) {
		pService->dealTopicMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealInteractionMsgThread(void * param_)
{
	AccessService * pService = (AccessService *)param_;
	if (pService) {
		pService->dealInteractionMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void loadSessionThread(void * param_)
{
	AccessService * pService = (AccessService *)param_;
	if (pService) {
		do {
			std::unique_lock<std::mutex> lock(pService->m_mutex4LoadSessionSignal);
			pService->m_cond4LoadSessionSignal.wait_for(lock, 
				std::chrono::milliseconds(500), [&] {
				return (!pService->m_nRun || (pService->getLoadDevice() 
					&& pService->getLoadUser() && pService->getLoadTask()));
			});
			if (!pService->m_nRun) {
				lock.unlock();
				break;
			}
			else {
				if (!pService->getLoadSessionFlag()) {
					if (pService->getLoadDevice() && pService->getLoadUser()
						&& pService->getLoadTask()) {
						pService->loadSessionList();
						break;
					}
				}
			}
		} while (1);
	}
}

void dealDisconnectEventThread(void * param_)
{
	auto pService = (AccessService *)param_;
	if (pService) {
		pService->handleDisconnectEvent();
	}
}

void dealAppAccMsgThread(void * param_)
{
	auto pService = (AccessService *)param_;
	if (pService) {
		pService->dealAccAppMsg();
	}
}

void dealExpressMsgThread(void * param_)
{
	auto pService = (AccessService *)param_;
	if (pService) {
		pService->dealExpressMessage();
	}
}

void * superviseThread(void * param_)
{
	AccessService * pService = (AccessService *)param_;
	if (pService) {
		zloop_start(pService->m_loop);
	}
	pthread_exit(NULL);
	return NULL;
}

int timerCb(zloop_t * loop_, int nTimerId_, void * arg_)
{
	auto pService = (AccessService *)arg_;
	if (pService) {
		if (pService->m_nRun) {
			if (pService->m_nTimerTickCount > 0) {
				if (pService->m_nTimerTickCount % 10 == 0) { //20s|per
					pService->checkAccClientList();
					pService->checkAppLinkList();
				}
				else if (pService->m_nTimerTickCount % 15 == 0) { //30s|per
					pService->sendInteractAlive();
				}
			}
			pService->m_nTimerTickCount++;
			if (pService->m_nTimerTickCount == 7201) {
				pService->m_nTimerTickCount = 1;
			}
			Sleep(100);
		}
		else {
			zloop_reader_end(loop_, pService->m_accessSock);
			zloop_reader_end(loop_, pService->m_interactorSock);
			zloop_reader_end(loop_, pService->m_subscriberSock);
			zloop_timer_end(loop_, nTimerId_);
			return -1;
		}
	}
	return 0;
}

int readAccInteract(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pService = (AccessService *)arg_;
	if (pService) {
		if (pService->m_nRun) {
			char szLog[256] = { 0 };
			zmsg_t * msg;
			zsock_recv(reader_, "m", &msg);
			if (msg) {
				if (zmsg_size(msg) > 2) {
					zframe_t * frame_id = zmsg_pop(msg);
					zframe_t * frame_type = zmsg_pop(msg);

					char szId[40] = { 0 };
					memcpy_s(szId, sizeof(szId), zframe_data(frame_id), zframe_size(frame_id));
					char szMsgType[20] = { 0 };
					memcpy_s(szMsgType, sizeof(szMsgType), zframe_data(frame_type), zframe_size(frame_type));
					unsigned int uiMsgType = 0;
					if (strlen(szMsgType)) {
						char * end;
						uiMsgType = (unsigned int)strtol(szMsgType, &end, 10);
					}
					switch (uiMsgType) {
						case access_service::E_ACC_DATA_TRANSFER: {//data transfer
							zframe_t * frame_endpoint = zmsg_pop(msg);
							zframe_t * frame_time = zmsg_pop(msg);
							zframe_t * frame_data = zmsg_pop(msg);
							auto pAccAppMsg = new access_service::AccessAppMessage();
							memset(pAccAppMsg, 0, sizeof(access_service::AccessAppMessage));
							char szEndpoint[32] = { 0 };
							memcpy_s(szEndpoint, sizeof(szEndpoint), zframe_data(frame_endpoint), zframe_size(frame_endpoint));
							strcpy_s(pAccAppMsg->szMsgFrom, sizeof(pAccAppMsg->szMsgFrom), szEndpoint);
							char szMsgTime[20] = { 0 };
							memcpy_s(szMsgTime, sizeof(szMsgTime), zframe_data(frame_time), zframe_size(frame_time));
							pAccAppMsg->ullMsgTime = pService->makeDatetime(szMsgTime);
							pAccAppMsg->uiMsgDataLen = (unsigned int)zframe_size(frame_data);
							strcpy_s(pAccAppMsg->szAccessFrom, sizeof(pAccAppMsg->szAccessFrom), szId);
							if (pAccAppMsg->uiMsgDataLen) {
								pAccAppMsg->pMsgData = new unsigned char[pAccAppMsg->uiMsgDataLen + 1];
								memcpy_s(pAccAppMsg->pMsgData, pAccAppMsg->uiMsgDataLen + 1,
									zframe_data(frame_data), zframe_size(frame_data));
								pAccAppMsg->pMsgData[pAccAppMsg->uiMsgDataLen] = '\0';
							}
							if (!pService->addAccAppMsg(pAccAppMsg)) {
								if (pAccAppMsg) {
									if (pAccAppMsg->pMsgData) {
										delete[] pAccAppMsg->pMsgData;
										pAccAppMsg->pMsgData = NULL;
									}
									delete pAccAppMsg;
									pAccAppMsg = NULL;
								}
							}
							pService->updateAccClient(szId, szEndpoint);
							zframe_destroy(&frame_id);
							zframe_destroy(&frame_type);
							zframe_destroy(&frame_endpoint);
							zframe_destroy(&frame_time);
							zframe_destroy(&frame_data);
							break;
						}
						case access_service::E_ACC_KEEP_ALIVE: { //keep alive
							zframe_t * frame_time = zmsg_pop(msg);

							zmsg_t * reply_msg = zmsg_new();
							zframe_t * frame_reply_type = zframe_from(szMsgType);
							zmsg_append(reply_msg, &frame_id);
							zmsg_append(reply_msg, &frame_reply_type);
							zmsg_append(reply_msg, &frame_time);
							if (1) {
								std::lock_guard<std::mutex> lk(pService->m_mutex4AccessSock);
								zmsg_send(&reply_msg, pService->m_accessSock);
							}
							pService->updateAccClient(szId, NULL);

							zframe_destroy(&frame_type);
							break;
						}
						case access_service::E_ACC_DATA_DISPATCH: { //recv dispatch result
							zframe_destroy(&frame_id);
							zframe_destroy(&frame_type);
							break;
						}
						case access_service::E_ACC_LINK_STATUS: { //link status
							zframe_t * frame_time = zmsg_pop(msg);
							zframe_t * frame_endpoint = zmsg_pop(msg);
							char szEndpoint[32] = { 0 };
							memcpy_s(szEndpoint, sizeof(szEndpoint), zframe_data(frame_endpoint), zframe_size(frame_endpoint));
							char szMsgTime[20] = { 0 };
							memcpy_s(szMsgTime, sizeof(szMsgTime), zframe_data(frame_time), zframe_size(frame_time));
							pService->addDisconnectEvent(szEndpoint, szMsgTime);
							pService->updateAccClient(szId, NULL);
							zframe_destroy(&frame_id);
							zframe_destroy(&frame_time);
							zframe_destroy(&frame_endpoint);
							zframe_destroy(&frame_type);
							break;
						}
						case access_service::E_ACC_APP_EXPRESS: {
							zframe_t * frame_body = zmsg_pop(msg);
							unsigned int uiFrameDataLen = (unsigned int)zframe_size(frame_body);
							sprintf_s(szLog, sizeof(szLog), "[access_service]%s[%d]recv from %s, data = %u\n", __FUNCTION__, __LINE__,
								szId, uiFrameDataLen);
							LOG_Log(pService->m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, pService->m_usLogType);

							if (uiFrameDataLen > 0) {
								access_service::ClientExpressMessage * pExpressMsg = new access_service::ClientExpressMessage();
								pExpressMsg->uiExpressDataLen = uiFrameDataLen;
								pExpressMsg->pExpressData = new unsigned char[uiFrameDataLen + 1];
								memcpy_s(pExpressMsg->pExpressData, uiFrameDataLen + 1, zframe_data(frame_body), uiFrameDataLen);
								pExpressMsg->pExpressData[uiFrameDataLen] = '\0';
								pExpressMsg->ullMessageTime = time(NULL);
								strcpy_s(pExpressMsg->szClientId, sizeof(pExpressMsg->szClientId), szId);
								if (!pService->addExpressMessage(pExpressMsg)) {
									if (pExpressMsg) {
										if (pExpressMsg->pExpressData && pExpressMsg->uiExpressDataLen > 0) {
											delete[] pExpressMsg->pExpressData;
											pExpressMsg->pExpressData = NULL;
											pExpressMsg->uiExpressDataLen = 0;
										}
										delete pExpressMsg;
										pExpressMsg = NULL;
									}
								}
							}
							zframe_destroy(&frame_id);
							zframe_destroy(&frame_type);
							zframe_destroy(&frame_body);
							break;
						}
						default: {
							zframe_destroy(&frame_id);
							zframe_destroy(&frame_type);
							break;
						}
					}

				}
				zmsg_destroy(&msg);
			}
		}
		else {
			return -1;
		}
	}
	return 0;
}

int readMsgSubscriber(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pService = (AccessService *)arg_;
	if (pService) {
		if (pService->m_nRun) {
			zmsg_t * subMsg;
			zsock_recv(reader_, "m", &subMsg);
			if (subMsg) {
				if (zmsg_size(subMsg)) {
					zframe_t * frame_mark = zmsg_pop(subMsg);
					zframe_t * frame_seq = zmsg_pop(subMsg);
					zframe_t * frame_type = zmsg_pop(subMsg);
					zframe_t * frame_uuid = zmsg_pop(subMsg);
					zframe_t * frame_body = zmsg_pop(subMsg);
					zframe_t * frame_from = zmsg_pop(subMsg);
					char szMark[64] = { 0 };
					char szSeq[20] = { 0 };
					char szType[16] = { 0 };
					char szUuid[64] = { 0 };
					char szBody[1024] = { 0 };
					char szFrom[64] = { 0 };
					memcpy_s(szMark, sizeof(szMark), zframe_data(frame_mark), zframe_size(frame_mark));
					memcpy_s(szSeq, sizeof(szSeq), zframe_data(frame_seq), zframe_size(frame_seq));
					memcpy_s(szType, sizeof(szType), zframe_data(frame_type), zframe_size(frame_type));
					memcpy_s(szUuid, sizeof(szUuid), zframe_data(frame_uuid), zframe_size(frame_uuid));
					memcpy_s(szBody, sizeof(szBody), zframe_data(frame_body), zframe_size(frame_body));
					memcpy_s(szFrom, sizeof(szFrom), zframe_data(frame_from), zframe_size(frame_from));
					TopicMessage * pTopicMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
					if (pTopicMsg) {
						strcpy_s(pTopicMsg->szMsgMark, sizeof(pTopicMsg->szMsgMark), szMark);
						pTopicMsg->uiMsgSequence = (unsigned int)atoi(szSeq);
						pTopicMsg->uiMsgType = (unsigned int)atoi(szType);
						strcpy_s(pTopicMsg->szMsgUuid, sizeof(pTopicMsg->szMsgUuid), szUuid);
						strcpy_s(pTopicMsg->szMsgBody, sizeof(pTopicMsg->szMsgBody), szBody);
						strcpy_s(pTopicMsg->szMsgFrom, sizeof(pTopicMsg->szMsgFrom), szFrom);
						if (!pService->addTopicMsg(pTopicMsg)) {
							free(pTopicMsg);
						}
					}
					zframe_destroy(&frame_mark);
					zframe_destroy(&frame_seq);
					zframe_destroy(&frame_type);
					zframe_destroy(&frame_uuid);
					zframe_destroy(&frame_body);
					zframe_destroy(&frame_from);
				}
				zmsg_destroy(&subMsg);
				pthread_mutex_lock(&pService->m_mutex4RemoteLink);
				pService->m_remoteMsgSrvLink.nActive = 1;
				pService->m_remoteMsgSrvLink.ulLastActiveTime = time(NULL);
				pthread_mutex_unlock(&pService->m_mutex4RemoteLink);
			}
		}
		else {
			return -1;
		}
	}
	return 0;
}

int readMsgInteractor(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pService = (AccessService *)arg_;
	if (pService) {
		if (pService->m_nRun != 0) {
			zmsg_t * msg;
			zsock_recv(reader_, "m", &msg);
			if (msg) {
				size_t nCount = zmsg_size(msg);
				if (nCount) {
					zframe_t ** frame_replys = (zframe_t **)zmalloc(nCount * sizeof(zframe_t *));
					InteractionMessage * pMsg = (InteractionMessage *)zmalloc(sizeof(InteractionMessage));
					pMsg->uiContentCount = (unsigned int)nCount;
					pMsg->pMsgContents = (char **)zmalloc(sizeof(char *) * nCount);
					pMsg->uiContentLens = (unsigned int *)zmalloc(sizeof(unsigned int) * nCount);
					for (size_t i = 0; i < nCount; i++) {
						frame_replys[i] = zmsg_pop(msg);
						size_t nFrameLen = zframe_size(frame_replys[i]);
						pMsg->uiContentLens[i] = (unsigned int)nFrameLen;
						pMsg->pMsgContents[i] = (char *)zmalloc(nFrameLen + 1);
						memcpy_s(pMsg->pMsgContents[i], nFrameLen + 1, zframe_data(frame_replys[i]),
							zframe_size(frame_replys[i]));
						pMsg->pMsgContents[i][nFrameLen] = '\0';
						zframe_destroy(&frame_replys[i]);
					}
					if (!pService->addInteractionMsg(pMsg)) {
						for (size_t i = 0; i < nCount; i++) {
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
					}
					free(frame_replys);
					frame_replys = NULL;
				}
				zmsg_destroy(&msg);
			}
		}
		else {
			return -1;
		}
	}
	return 0;
}

