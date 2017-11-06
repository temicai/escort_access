#ifndef ACCESS_CONCRETE_H
#define ACCESS_CONCRETE_H

#include <WS2tcpip.h>
#include <WinSock2.h>
#include <queue>
#include <map>
#include <string>

#define _TIMESPEC_DEFINED
#include "pthread.h"
#include "pfLog.hh"
#include "zmq.h"
#include "czmq.h"
#include "document.h" //rapidjson
#include "zookeeper.h"
#include "sodium.h"

#include "zk_escort.h"
#include "escort_common.h"
#include "escort_error.h"
#include "EscortDbCommon.h"

const char gSecret = '8';

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "pthreadVC2.lib")
#pragma comment(lib, "pfLog.lib")
#pragma comment(lib, "libzmq.lib")
#pragma comment(lib, "czmq.lib")

#define USE_VPN 1

#if USE_VPN
#include "tcp_server.h"
#pragma comment(lib, "tcp_server.lib")
#else
#include "iocp_tcp_server.h"
#pragma comment(lib, "iocp_tcp_server.lib")
#endif

#pragma comment(lib, "zookeeper.lib")
#pragma comment(lib, "libsodium.lib")


namespace access_service
{
	enum eAppCommnad
	{
		E_CMD_UNDEFINE = 0,
		E_CMD_LOGIN = 1,
		E_CMD_LOGOUT = 2,
		E_CMD_BIND_REPORT = 3,
		E_CMD_UNBIND_REPORT = 4,
		E_CMD_TASK = 5,
		E_CMD_TASK_CLOSE = 6,
		E_CMD_POSITION_REPORT = 7,
		E_CMD_FLEE_REPORT = 8,
		E_CMD_FLEE_REVOKE_REPORT = 9,
		E_CMD_MSG_NOTIFY = 10,
		E_CMD_KEEPALIVE = 11,
		E_CMD_MODIFY_PASSWD = 12,
		E_CMD_QUERY_TASK = 13,
		E_CMD_DEVICE_COMMAND = 14, 
		E_CMD_QUERY_PERSON = 15,
		E_CMD_QUERY_TASK_LIST = 16,
		E_CMD_NOTIFY_TASK_START = 17,
		E_CMD_NOTIFY_TASK_STOP = 18,
    E_CMD_QUERY_DEVICE_STATUS = 19,
		E_CMD_DEFAULT_REPLY = 100, 
		E_CMD_LOGIN_REPLY = 101,
		E_CMD_LOGOUT_REPLY = 102,
		E_CMD_BIND_REPLY = 103,
		E_CMD_UNBIND_REPLY = 104,
		E_CMD_TASK_REPLY = 105,
		E_CMD_TAKK_CLOSE_REPLY = 106,
		E_CMD_POSITION_REPLY = 107,
		E_CMD_FLEE_REPLY = 108,
		E_CMD_FLEE_REVOKE_REPLY = 109,
		E_CMD_KEEPALIVE_REPLY = 111,
		E_CMD_MODIFY_PASSWD_REPLY = 112,
		E_CMD_QUERY_TASK_REPLY = 113,
		E_CMD_DEVICE_COMMAND_REPLY = 114,
		E_CMD_QUERY_PERSON_REPLY = 115,
		E_CMD_QUERY_TASK_LIST_REPLY = 116,
    E_CMD_QUERY_DEVICE_STATUS_REPLY = 119,
	};

	enum eAppTaskType
	{
		E_TASK_ESCORT = 1,
		E_TASK_MEDICAL = 2,
		E_TASK_TESTIFY = 3,
	};

	enum eAppNotifyMessageType
	{
		E_ALARM_DEVICE_LOWPOWER = 1,
		E_ALARM_DEVICE_LOOSE = 2,
		E_NOTIFY_DEVICE_POSITION = 3,
		E_NOTIFY_DEVICE_ONLINE = 4,
		E_NOTIFY_DEVICE_OFFLINE = 5,
		E_NOTIFY_DEVICE_BATTERY = 6,
		E_NOTIFY_DEVICE_LOCATE_LOST = 7,
		E_ALARM_DEVICE_FENCE = 8,
	};

	enum eDeviceCommand 
	{
		E_DEV_CMD_ALARM = 1,
		E_DEV_CMD_RESET = 2,
		E_DEV_CMD_QUERY_POSITION = 3,
	};

	enum eQueryMode
	{
		E_QMODE_EQUAL = 0,
		E_QMODE_NOT_EQUAL = 1,
		E_QMODE_FORE_LIKE = 2,
		E_QMODE_TAIL_LIKE = 3,
		E_QMODE_MIDDLE_LIKE = 4,
	};

	enum eParamType
	{
		E_PARAM_LOGTYPE = 1, 
		E_PARAM_TASK_CLOSE_CHECK_STATUS = 2,
		E_PARAM_TASK_FLEE_REPORT_REPLICATED = 3,
	};

	typedef struct tagLogContext
	{
		char * pLogData;
		unsigned int uiDataLen;
		unsigned short usLogCategory;
		unsigned short usLogType;
		tagLogContext()
		{
			uiDataLen = 0;
			pLogData = NULL;
			usLogCategory = 0;
			usLogType = 0;
		}
		~tagLogContext()
		{
			if (uiDataLen && pLogData) {
				free(pLogData);
				pLogData = NULL;
				uiDataLen = 0;
			}
		}
	} LogContext;

	typedef struct tagAppMessageHead
	{
		char marker[2];
		char version[2];
		unsigned int uiDataLen;
	} AppMessageHead;

	typedef struct tagLinkData
	{
		char szLinkId[32];
		int nLinkState; // 0: on, 1: off
		unsigned int uiTotalDataLen;
		unsigned int uiLingeDataLen; 
		unsigned int uiLackDataLen;	
		unsigned char * pLingeData;
		char szUser[20];
		tagLinkData() 
		{
			szLinkId[0] = '\0';
			szUser[0] = '\0';
			nLinkState = 0;
			uiTotalDataLen = 0;
			uiLingeDataLen = 0;
			uiLackDataLen = 0;
			pLingeData = NULL;
		}
		~tagLinkData() {}
	} LinkDataInfo;

	typedef struct tagAppLogin
	{
		unsigned int uiReqSeq;
		char szUser[20];
		char szPasswd[64];
		char szDateTime[16];
		char szHandset[64];
		tagAppLogin()
		{
			uiReqSeq = 0;
			szUser[0] = '\0';
			szPasswd[0] = '\0';
			szDateTime[0] = '\0';
			szHandset[0] = '\0';
		}
	} AppLoginInfo;

	typedef struct tagAppLogout
	{
		unsigned int uiReqSeq;
		char szSession[20];
		char szDateTime[16];
	} AppLogoutInfo;

	typedef struct tagAppBind
	{
		char szSesssion[20];
		char szFactoryId[4];
		char szDeviceId[16];
		char szDateTime[16];
		unsigned int uiReqSeq;
		int nMode; //0-bind 1-unbind;
		tagAppBind()
		{
			uiReqSeq = 0;
			szSesssion[0] = '\0';
			szFactoryId[0] = '\0';
			szDeviceId[0] = '\0';
			szDateTime[0] = '\0';
			nMode = 0;
		}
	} AppBindInfo;

	typedef struct tagAppSubmitTask
	{
		char szSession[20];
		unsigned int uiReqSeq;
		unsigned short usTaskType;
		unsigned short usTaskLimit;
		char szDestination[64];
		char szTarget[64];
		char szDatetime[16];
		tagAppSubmitTask()
		{
			uiReqSeq = 0;
			szSession[0] = '\0';
			usTaskType = 0;
			usTaskLimit = 0;
			szDestination[0] = '\0';
			szTarget[0] = '\0';
			szDatetime[0] = '\0';
		}
	} AppSubmitTaskInfo;

	typedef struct tagAppCloseTask
	{
		char szSession[20];
		char szTaskId[16];
		unsigned int uiReqSeq;
		int nCloseType; //0-normal,1-linge
		char szDatetime[16];
	} AppCloseTaskInfo;

	typedef struct tagAppPosition
	{
		char szSession[20];
		char szTaskId[16];
		double dLat;
		double dLng;
		int nCoordinate;
		unsigned int uiReqSeq;
		char szDatetime[16];
		
	} AppPositionInfo;

	typedef struct tagAppSubmitFlee
	{
		char szSession[20];
		char szTaskId[16];
		char szDatetime[16];
		int nMode; // 0-flee 1-flee revoke
		unsigned int uiReqSeq;
		tagAppSubmitFlee()
		{
			uiReqSeq = 0;
			szSession[0] = '\0';
			szTaskId[0] = '\0';
			szDatetime[0] = '\0';
			nMode = -1;
		}
	} AppSubmitFleeInfo;

	typedef struct tagAppKeepAlive
	{
		char szSession[20];
		unsigned int uiSeq;
		char szDatetime[16];
		tagAppKeepAlive()
		{
			szSession[0] = '\0';
			uiSeq = 0;
			szDatetime[0] = '\0';
		}
	} AppKeepAlive;

	typedef struct tagAppModifyPassword
	{
		char szSession[20];
		char szCurrPassword[64];
		char szNewPassword[64];
		char szDatetime[20];
		unsigned int uiSeq;
		tagAppModifyPassword()
		{
			szSession[0] = '\0';
			szCurrPassword[0] = '\0';
			szNewPassword[0] = '\0';
			szDatetime[0] = '\0';
			uiSeq = 0;
		}
	} AppModifyPassword;

	typedef struct tagAppQueryTask
	{
		char szSession[20];
		char szTaskId[16];
		char szDatetime[16];
		unsigned int uiQuerySeq;
		tagAppQueryTask()
		{
			szSession[0] = '\0';
			szTaskId[0] = '\0';
			szDatetime[0] = '\0';
			uiQuerySeq = 0;
		}
	} AppQueryTask;

	typedef struct tagAppDeviceCommandInfo
	{
		char szSession[20];
		char szFactoryId[4];
		char szDeviceId[16];
		int nParam1;
		int nParam2;
		unsigned int nSeq;
		char szDatetime[20];
		tagAppDeviceCommandInfo()
		{
			szSession[0] = '\0';
			szFactoryId[0] = '\0';
			szDeviceId[0] = '\0';
			nParam1 = 0;
			nParam2 = 0;
			nSeq = 0;
			szDatetime[0] = '\0';
		}
	} AppDeviceCommandInfo;

	typedef struct tagAppQueryPerson
	{
		char szSession[20];
		char szQryPersonId[32];
		int nQryMode;
		unsigned int uiQeurySeq;
		char szQryDatetime[16];
		tagAppQueryPerson()
		{
			szSession[0] = '\0';
			szQryPersonId[0] = '\0';
			szQryDatetime[0] = '\0';
			uiQeurySeq = 0;
		}
	} AppQueryPerson;

	typedef struct tagAppQueryTaskList
	{
		char szOrgId[40];
		char szDatetime[20];
		unsigned int uiQrySeq;
		tagAppQueryTaskList()
		{
			szOrgId[0] = '\0';
			szDatetime[0] = '\0';
			uiQrySeq = 0;
		}
	} AppQueryTaskList;

  typedef struct tagAppQueryDeviceStatus
  {
    char szSession[20];
    char szFactoryId[4];
    char szDeviceId[16];
    char szDatetime[20];
    unsigned int uiQrySeq;
    tagAppQueryDeviceStatus()
    {
      szSession[0] = '\0';
      szFactoryId[0] = '\0';
      szDeviceId[0] = '\0';
      szDatetime[0] = '\0';
      uiQrySeq = 0;
    }
  } AppQueryDeviceStatus;

	typedef struct tagAppLinkInfo
	{
		char szSession[20];
		char szGuarder[20];
		char szEndpoint[40];
		char szFactoryId[4];
		char szDeviceId[16];
		char szOrg[40];
		char szTaskId[16];
		char szHandset[64];
		int nActivated;//0:false, 1:true
		int nNotifyBattery : 8;
		int nNotifyStatus : 8;
		int nNotifyPosition : 8;
		int nNotifyOnline : 8;
		unsigned long long ulActivateTime;
		tagAppLinkInfo()
		{
			szSession[0] = '\0';
			szGuarder[0] = '\0';
			szEndpoint[0] = '\0';
			szFactoryId[0] = '\0';
			szDeviceId[0] = '\0';
			szOrg[0] = '\0';
			szTaskId[0] = '\0';
			szHandset[0] = '\0';
			nActivated = 0;
			ulActivateTime = 0;
			nNotifyBattery = 0;
			nNotifyStatus = 0;
			nNotifyPosition = 0;
			nNotifyOnline = 0;
		}
	} AppLinkInfo;

	typedef struct tagSubscribeInfo
	{
		char szSubFilter[64];
		char szSession[20];
		char szGuarder[20];
		char szEndpoint[40];
		tagSubscribeInfo()
		{
			szSubFilter[0] = '\0';
			szSession[0] = '\0';
			szGuarder[0] = '\0';
			szEndpoint[0] = '\0';
		}
	} AppSubscribeInfo;

	typedef struct tagRemoteLinkInfo
	{
		int nActive; //0-false,1-true
		unsigned long long ulLastActiveTime; //default 0
		tagRemoteLinkInfo()
		{
			nActive = 0;
			ulLastActiveTime = 0;
		}
	} RemoteLinkInfo;
}

//part1: connect and deal with app
//part2: connect and deal with msg-midware
//part3: data handle with content
class AccessService
{
public:
	AccessService(const char * pZkHost, const char * pRoot);
	~AccessService();
	int StartAccessService(const char * pHost, unsigned short usServicePort, const char * pMidwareHost,
		unsigned short usPublishPort, unsigned short usContactPort, const char * pDbProxyHost,
		unsigned short usQueryPort);
	int StopAccessService();
	void SetLogType(unsigned short usLogType);
	int GetStatus();
	void SetParameter(int nParamType, int nParamValue);
protected:
	void initLog();
	bool addLog(access_service::LogContext * pLog);
	void writeLog(const char * pLogContent, unsigned short usLogCategoryType, unsigned short usLogType);
	void dealLog();

	bool addAppMsg(MessageContent * pMsg);
	void dealAppMsg();
	void parseAppMsg(MessageContent * pMsg);
	int getWholeMessage(const unsigned char * pData, unsigned int uiDataLen, unsigned int uiIndex, 
		unsigned int & uiBeginIndex, unsigned int & uiEndIndex, unsigned int & uiUnitLen);
	void decryptMessage(unsigned char * pData, unsigned int uiBeginIndex, unsigned int ulEndIndex);
	void encryptMessage(unsigned char * pData, unsigned int uiBeginIndex, unsigned int ulEndIndex);
	void handleAppLogin(access_service::AppLoginInfo loginInfo, const char *, unsigned long long);
	void handleAppLogout(access_service::AppLogoutInfo logoutInfo, const char *, unsigned long long);
	void handleAppBind(access_service::AppBindInfo bindInfo, const char *, unsigned long long);
	void handleAppSubmitTask(access_service::AppSubmitTaskInfo taskInfo, const char *, unsigned long long);
	void handleAppCloseTask(access_service::AppCloseTaskInfo taskInfo, const char *, unsigned long long);
	void handleAppPosition(access_service::AppPositionInfo positionInfo, const char *, unsigned long long);
	void handleAppFlee(access_service::AppSubmitFleeInfo fleeInfo, const char *, unsigned long long);
	void handleAppKeepAlive(access_service::AppKeepAlive keepAlive, const char *, unsigned long long);
	void handleAppModifyAccountPassword(access_service::AppModifyPassword modifyPasswd, const char *, 
		unsigned long long);
	void handleAppQueryTask(access_service::AppQueryTask queryTask, const char *, unsigned long long);
	void handleAppDeviceCommand(access_service::AppDeviceCommandInfo cmdInfo, const char *);
	void handleAppQueryPerson(access_service::AppQueryPerson queryPerson, const char *);
	void handleAppQueryTaskList(access_service::AppQueryTaskList * pQryTaskList, const char *);
  void handleAppQueryDeviceStatus(access_service::AppQueryDeviceStatus *, const char *);
	unsigned int getNextRequestSequence();
	int generateSession(char * pSession, size_t nSize);
	void generateTaskId(char * pTaskId, size_t nSize);
	void changeDeviceStatus(unsigned short usNewStatus, unsigned short & usDeviceStatus, int nMode = 0);
	bool addTopicMsg(TopicMessage * pMsg);
	void dealTopicMsg();
	void storeTopicMsg(const TopicMessage * pMsg);
	bool addInteractionMsg(InteractionMessage * pMsg);
	void dealInteractionMsg();
	int handleTopicAliveMsg(TopicAliveMessage * pMsg, const char *);
	int handleTopicOnlineMsg(TopicOnlineMessage * pMsg, const char *);
	int handleTopicOfflineMsg(TopicOfflineMessage * pMsg, const char *);
	int handleTopicLocateGpsMsg(TopicLocateMessageGps * pMsg, const char *);
	int handleTopicLocateLbsMsg(TopicLocateMessageLbs * pMsg, const char *);
	int handleTopicAlarmLowpowerMsg(TopicAlarmMessageLowpower * pMsg, const char *);
	int handleTopicAlarmLooseMsg(TopicAlarmMessageLoose * pMsg, const char *);
	int handleTopicAlarmFleeMsg(TopicAlarmMessageFlee * pMsg, const char *);
	int handleTopicAlarmLocateLostMsg(TopicAlarmMessageLocateLost * pMsg, const char *);
	int handleTopicAlarmFenceMsg(TopicAlarmMessageFence * pMsg, const char *);
	int handleTopicDeviceBindMsg(TopicBindMessage * pMsg, const char *);
	int handleTopicTaskSubmitMsg(TopicTaskMessage * pMsg, const char *);
	int handleTopicTaskModifyMsg(TopicTaskModifyMessage * pMsg, const char *);
	int handleTopicTaskCloseMsg(TopicTaskCloseMessage * pMsg, const char *);
	int handleTopicLoginMsg(TopicLoginMessage * pMsg, const char *);
	int handleTopicLogoutMsg(TopicLogoutMessage * pMsg, const char *);
	
	void initZookeeper();
	int competeForMaster();
	void masterExist();
	int setAccessData(const char * pPath, void * pData, size_t nDataSize);
	int runAsSlaver();
	void removeSlaver();
	int getMidwareData(const char * pPath, ZkMidware * pData);

	void zkAddSession(const char * pSession);
	void zkRemoveSession(const char * pSession);
	void zkSetSession(const char * pSession);
	void loadSessionList();
	bool getLoadSessionFlag();
	void setLoadSessionFlag(bool);

	int sendDatatoEndpoint(const char * pData, size_t nDataLen, const char * pEndpoint);
	int sendDataViaInteractor(const char * pData, size_t nDataLen);
		
	void dealNetwork();
	void handleLinkDisconnect(const char * pEndpoint, const char * pUser, bool bFlag = true);
	
	void readDataBuffer();
	void verifyLinkList();
	bool verifyLink(const char * pEndpoint);
	void loadOrgList(bool flag = false);
	//void addElement(std::set<std::string> & list, std::string element);
	void findOrgChild(std::string strOrgId, std::set<std::string> & childList);

	friend void * dealAppMsgThread(void *);
	friend void * dealTopicMsgThread(void *);
	friend void * dealInteractionMsgThread(void *);
	friend void * dealNetworkThread(void *);
	friend void * dealLogThread(void *);
	
	//friend void * deal
	friend void * superviseThread(void *);
	friend int supervise(zloop_t * loop, int timer_id, void * arg);
	friend void __stdcall fMsgCb(int nType, void * pMsg, void * pUserData);
	friend void zk_server_watcher(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx);
	friend void zk_escort_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_master_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_master_exists_watcher(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx);
	friend void zk_access_master_exists_completion(int rc, const Stat * stat, const void * data);
	friend void zk_access_set_completion(int rc, const struct Stat * stat, const void * data);
	friend void zk_access_slaver_create_completion(int rc, const char * name, const void * data);
	friend void zk_session_create_completion(int rc, const char * name, const void * data);
	friend void zk_session_get_children_watcher(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx);
	friend void zk_session_child_delete_completion(int rc, const void * data);
	friend void zk_session_child_create_completion(int rc, const char * name, const void * data);
	friend void zk_session_child_set_completion(int rc, const struct Stat * stat, const void * data);
private:

	unsigned long long m_ullSrvInst;

	int m_nRun;
	unsigned short m_usSrvPort;
	int m_nTaskCloseCheckStatus;
	int m_nTaskFleeReplicatedReport;
	
	pthread_t m_pthdLog;
	pthread_mutex_t m_mutex4LogQueue;
	pthread_cond_t m_cond4LogQueue;
	std::queue<access_service::LogContext *> m_logQueue;
	unsigned short m_usLogType;
	unsigned long long m_ullLogInst;
	char m_szLogRoot[256];
	
	zctx_t * m_ctx;
	void * m_subscriber;
	void * m_interactor;
	char m_szInteractorIdentity[40];
	void * m_seeker;
	char m_szSeekerIdentity[40];
	pthread_t m_pthdNetwork;

	zhandle_t * m_zkHandle;
	char m_szHost[512];
	char m_zkNodePath[256];
	bool m_bConnectZk; 
	ZkAccess m_zkAccess;

	//message from app
	pthread_mutex_t m_mutex4AppMsgQueue;
	pthread_cond_t m_cond4AppMsgQueue;
	std::queue<MessageContent *> m_appMsgQueue;
	pthread_t m_pthdAppMsg;
	pthread_mutex_t m_mutex4LinkDataList;
	std::map<std::string, access_service::LinkDataInfo *> m_linkDataList; //key:linkId 
	
	pthread_mutex_t m_mutex4LinkList; 
	zhash_t * m_linkList; //key: session, value: AppLinkInfo *
	pthread_mutex_t m_mutex4SubscribeList;
	zhash_t * m_subscribeList;	//key:sub_filter|topic, value: AppSubscribeInfo *
	pthread_mutex_t m_mutex4LocalTopicMsgList;
	zlist_t * m_localTopicMsgList;	//TopicMessage
	bool m_bLoadSession;
	pthread_mutex_t m_mutex4LoadSession;

	pthread_t m_pthdSupervisor;//supervisor all the links 
	zloop_t * m_loop;
	int m_nTimer4Supervise;
	int m_nTimerTickCount; //
	pthread_mutex_t m_mutex4RemoteLink;
	access_service::RemoteLinkInfo m_remoteMsgSrvLink; 
	access_service::RemoteLinkInfo m_remoteProxyLink;
	
	pthread_t m_pthdTopicMsg;		//Topic Message from subscribe
	pthread_t m_pthdInteractionMsg;		//message from interactor
	pthread_mutex_t m_mutex4TopicMsgQueue;
	pthread_mutex_t m_mutex4InteractionMsgQueue;
	pthread_cond_t m_cond4TopicMsgQueue;
	pthread_cond_t m_cond4InteractionMsgQueue;
	std::queue<TopicMessage *> m_topicMsgQueue;
	std::queue<InteractionMessage *> m_interactionMsgQueue;

	static unsigned int g_uiRequestSequence;
	static int g_nRefCount;
	static pthread_mutex_t g_mutex4RequestSequence;
	static pthread_mutex_t g_mutex4TaskId;
	//buffer date list: guarderList, deviceList, taskList
	static zhash_t * g_guarderList; //key: guarderId, value: Guarder  all guarder without deactivate
	static pthread_mutex_t g_mutex4GuarderList;
	static zhash_t * g_deviceList; //key: deviceId, all device without deactivate 
	static pthread_mutex_t g_mutex4DevList;
	static zhash_t * g_taskList; //key: taskId , unfinish|executing tasks
	static pthread_mutex_t g_mutex4TaskList; 
	static std::map<std::string, OrganizationEx *> g_orgList2;
	static pthread_mutex_t g_mutex4OrgList;
	static BOOL g_bInitBuffer;

};





#endif
