#ifndef ACCESS_CONCRETE_H
#define ACCESS_CONCRETE_H

#include <WS2tcpip.h>
#include <WinSock2.h>
#include <queue>
#include <map>
#include <set>
#include <string>
#include <random>
#include <thread>
#include <mutex>
#include <condition_variable>

#define _TIMESPEC_DEFINED
#include "pthread.h"
#include "pf_log.h"
#include "zmq.h"
#include "czmq.h"
#include "document.h" //rapidjson
#include "zookeeper.h"
#include "sodium.h"

#include "zk_escort.h"
#include "escort_common.h"
#include "escort_error.h"

const char gSecret = '8';

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "pthreadVC2.lib")
#pragma comment(lib, "pf_log.lib")
#pragma comment(lib, "libzmq.lib")
#pragma comment(lib, "libczmq.lib")

#include "tcp_common.h"

#pragma comment(lib, "zookeeper.lib")
#pragma comment(lib, "libsodium.lib")

#define LogI(inst, content) LOG_Log(inst, content, pf_logger::eLOGCATEGORY_INFO, pf_logger::eLOGTYPE_FILE)
#define LogE(inst, content) LOG_Log(inst, content, pf_logger::eLOGCATEGORY_FAULT, pf_logger::eLOGTYPE_FILE)

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
		E_CMD_TASK_CLOSE_REPLY = 106,
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
		E_ALARM_DEVICE_LOWPOWER = 1,     //deprecated
		E_ALARM_DEVICE_LOOSE = 2,        //deprecated
		E_NOTIFY_DEVICE_POSITION = 3, 
		E_NOTIFY_DEVICE_ONLINE = 4,      //deprecated
		E_NOTIFY_DEVICE_OFFLINE = 5,
		E_NOTIFY_DEVICE_BATTERY = 6,     //deprecated
		E_NOTIFY_DEVICE_LOCATE_LOST = 7,
		E_ALARM_DEVICE_FENCE = 8,
		E_NOTIFY_DEVICE_INFO = 10,
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
		E_PARAM_ENABLE_TASK_LOOSE_CHECK = 4,
	};

	enum eAccessMessageType
	{
		E_ACC_DATA_TRANSFER = 1,
		E_ACC_KEEP_ALIVE = 2,
		E_ACC_DATA_DISPATCH = 3,
		E_ACC_LINK_STATUS = 4,
		E_ACC_APP_EXPRESS = 5,
	};

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
		char szDateTime[20];
		char szHandset[64];
		char szPhone[32];
		tagAppLogin()
		{
			uiReqSeq = 0;
			szUser[0] = '\0';
			szPasswd[0] = '\0';
			szDateTime[0] = '\0';
			szHandset[0] = '\0';
			szPhone[0] = '\0';
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
		char szDatetime[20];
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
			memset(szSession, 0, sizeof(szSession));
			memset(szQryPersonId, 0, sizeof(szQryPersonId));
			memset(szQryDatetime, 0, sizeof(szQryDatetime));
			uiQeurySeq = 0;
			nQryMode = 0;
		}
	} AppQueryPerson;

	typedef struct tagAppQueryTaskList
	{
		char szOrgId[40];
		char szDatetime[20];
		unsigned int uiQrySeq;
		tagAppQueryTaskList()
		{
			memset(szOrgId, 0, sizeof(szOrgId));
			memset(szDatetime, 0, sizeof(szDatetime));
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
    tagAppQueryDeviceStatus() noexcept
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
		char szBroker[40];
		char szFactoryId[4];
		char szDeviceId[16];
		char szOrg[40];
		char szTaskId[16];
		char szHandset[64];
		char szPhone[32];
		int nActivated; //0:false, 1 : true
		int nLinkFormat;//0: tcp, 1: express, default: 0,
		unsigned long long ullActivateTime;
		tagAppLinkInfo() noexcept
		{
			szSession[0] = '\0';
			szGuarder[0] = '\0';
			szEndpoint[0] = '\0';
			szBroker[0] = '\0';
			szFactoryId[0] = '\0';
			szDeviceId[0] = '\0';
			szOrg[0] = '\0';
			szTaskId[0] = '\0';
			szHandset[0] = '\0';
			szPhone[0] = '\0';
			nActivated = 0;
			ullActivateTime = 0;
		}
	} AppLinkInfo;

	typedef struct tagSubscribeInfo
	{
		char szSubFilter[64];
		char szSession[20];
		char szGuarder[20];
		char szEndpoint[32];
		char szAccSource[64];
		int nFormat;
		tagSubscribeInfo() noexcept
		{
			memset(szSubFilter, 0, sizeof(szSubFilter));
			memset(szSession, 0, sizeof(szSession));
			memset(szGuarder, 0, sizeof(szGuarder));
			memset(szEndpoint, 0, sizeof(szEndpoint));
			memset(szAccSource, 0, sizeof(szAccSource));
			nFormat = 0;
		}
	} AppSubscribeInfo;

	typedef struct tagRemoteLinkInfo
	{
		int nActive; //0-false,1-true
		int nSendKeepAlive;
		unsigned long long ulLastActiveTime; //default 0
		tagRemoteLinkInfo() noexcept
		{
			nActive = 0;
			nSendKeepAlive = 0;
			ulLastActiveTime = 0;
		}
	} RemoteLinkInfo;

	typedef struct tagDisconnectEvent
	{
		char szEndpoint[32];
		char szEventTime[20];
	} DisconnectEvent;

	typedef struct tagAccessAppMessage
	{
		char szAccessFrom[64];
		char szMsgFrom[64];
		unsigned char * pMsgData;
		unsigned int uiMsgDataLen;
		unsigned long long ullMsgTime;
	} AccessAppMessage;

	typedef struct tagAccessClientInfo
	{
		char szAccClientId[64];
		unsigned long long ullLastActivatedTime;
		std::set<std::string> proxySet;
		tagAccessClientInfo()
		{
			memset(szAccClientId, 0, sizeof(szAccClientId));
			ullLastActivatedTime = 0;
		}
	} AccessClientInfo;

	typedef struct tagClientExpressMessage
	{
		char szClientId[64];
		unsigned int uiExpressDataLen;
		unsigned char * pExpressData;
		unsigned long long ullMessageTime;
		tagClientExpressMessage() noexcept
		{
			memset(szClientId, 0, sizeof(szClientId));
			uiExpressDataLen = 0;
			ullMessageTime = 0;
			pExpressData = NULL;
		}
	} ClientExpressMessage;

	typedef struct tagQueryPerson
	{
		unsigned int uiQrySeq;
		unsigned short usFormat;
		unsigned short usReverse;
		char szSession[20];
		char szLink[64];
		char szEndpoint[32];
	} QueryPersonEvent;

}

using namespace escort;

//part1: connect and deal with app
//part2: connect and deal with msg-midware
//part3: data handle with content
class AccessService
{
public:
	AccessService(const char * pZkHost, const char * pRoot);
	~AccessService();
	int StartAccessService_v2(const char * pAccHost, unsigned short usAccPort, const char * pMsgHost, 
		unsigned short usMsgPort, unsigned short usInteractPort);
	int StopAccessService_v2();
	void SetLogType(unsigned short usLogType);
	int GetStatus();
	void SetParameter(int nParamType, int nParamValue);
protected:
	void initLog();
	bool addAccAppMsg(access_service::AccessAppMessage *);
	void dealAccAppMsg();
	void parseAccAppMsg(access_service::AccessAppMessage *);
	int getWholeMessage(const unsigned char * pData, unsigned int uiDataLen, unsigned int uiIndex, 
		unsigned int & uiBeginIndex, unsigned int & uiEndIndex, unsigned int & uiUnitLen);
	void decryptMessage(unsigned char * pData, unsigned int uiBeginIndex, unsigned int ulEndIndex);
	void encryptMessage(unsigned char * pData, unsigned int uiBeginIndex, unsigned int ulEndIndex);
	void handleAppLoginV2(access_service::AppLoginInfo * pLoginInfo, const char * pMsgFrom, const char * pClientId,
		unsigned long long ullMsgTime);
	void handleAppLogout(access_service::AppLogoutInfo logoutInfo, const char * pMsgFrom, unsigned long long ullMsgTime, 
		const char * pClientId);
	void handleAppBindV2(access_service::AppBindInfo * pBindInfo, const char * pMsgFrom, const char * pClientId,
		unsigned long long ullMsgTime);
	void handleAppUnbindV2(access_service::AppBindInfo * pUnbindInfo, const char * pMsgFrom, const char * pClientId,
		unsigned long long ullMsgTime);
	void handleAppSubmitTaskV2(access_service::AppSubmitTaskInfo * pTaskInfo, const char * pEndpoint, const char * pClientId,
		unsigned long long ullMsgTime);
	void handleAppCloseTaskV2(access_service::AppCloseTaskInfo * pTaskInfo, const char * pEndpoint, const char * pClientId,
		unsigned long long ullMsgTime);
	void handleAppPosition(access_service::AppPositionInfo positionInfo, const char *, unsigned long long, const char *);
	void handleAppFlee(access_service::AppSubmitFleeInfo fleeInfo, const char *, unsigned long long, const char *);
	void handleAppKeepAlive(access_service::AppKeepAlive * keepAlive, const char *, unsigned long long, const char *);
	void handleAppModifyAccountPassword(access_service::AppModifyPassword modifyPasswd, const char *, 
		unsigned long long, const char *);
	void handleAppQueryTask(access_service::AppQueryTask queryTask, const char *, unsigned long long, const char *);
	void handleAppDeviceCommand(access_service::AppDeviceCommandInfo cmdInfo, const char *, const char *);
	void handleAppQueryPerson(access_service::AppQueryPerson queryPerson, const char *, const char *);
	void handleAppQueryTaskList(access_service::AppQueryTaskList * pQryTaskList, const char *, const char *);
  void handleAppQueryDeviceStatus(access_service::AppQueryDeviceStatus *, const char *, const char *);

	unsigned int getNextRequestSequence();
	int generateSession(char * pSession, size_t nSize);
	void generateTaskId(char * pTaskId, size_t nSize);
	void changeDeviceStatus(unsigned short usNewStatus, unsigned short & usDeviceStatus, int nMode = 0);
	bool addTopicMsg(TopicMessage * pMsg);
	void dealTopicMsg();
	void storeTopicMsg(const TopicMessage * pMsg);
	bool addInteractionMsg(InteractionMessage * pMsg);
	void dealInteractionMsg();
	int handleTopicDeviceAliveMsg(TopicAliveMessage * pMsg, const char * pTopic);
	int handleTopicDeviceOnlineMsg(TopicOnlineMessage * pMsg, const char *);
	int handleTopicDeviceOfflineMsg(TopicOfflineMessage * pMsg, const char *);
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
	int handleTopicUserLoginMsg(TopicLoginMessage * pMsg, const char *);
	int handleTopicUserLogoutMsg(TopicLogoutMessage * pMsg, const char *);
	int handleTopicUserAliveMsg(escort::TopicUserAliveMessage * pMsg, const char *);
	int handleTopicDeviceChargeMsg(TopicDeviceChargeMessage * pMsg, const char * pTopic);

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

	int sendDatatoEndpoint(char * pData, uint32_t nDataLen, const char * pEndpoint);
	int sendDataToEndpoint_v2(char * pData, uint32_t nDataLen, const char * pEndpoint, 
		uint32_t nDataType, const char * pFrom);
	int sendDataViaInteractor_v2(const char * pData, uint32_t nDataLen);
		
	void handleLinkDisconnect(const char * pEndpoint, const char * pUser, bool bFlag = true);
	void loadOrgList(bool flag = false);
	void findOrgChild(std::string strOrgId, std::set<std::string> & childList);
	char * ansiToUtf8(const char *);
	char * utf8ToAnsi(const char *);
	unsigned long long makeDatetime(const char *);
	void formatDatetime(unsigned long long, char *, size_t);

	void getOrgList()
	{
		char szDatetime[20] = { 0 };
		formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
		char szOrgMsg[256] = { 0 };
		sprintf_s(szOrgMsg, sizeof(szOrgMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"\",\"param2\":\"\",\"param3\":0,\"param4\":0}",
			MSG_SUB_GETINFO, getNextRequestSequence(), szDatetime, BUFFER_ORG);
		sendDataViaInteractor_v2(szOrgMsg, (uint32_t)strlen(szOrgMsg));
	}
	void getDeviceList()
	{
		char szDatetime[20] = { 0 };
		formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
		char szDevMsg[256] = { 0 };
		sprintf_s(szDevMsg, sizeof(szDevMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"\",\"param2\":\"\",\"param3\":0,\"param4\":0}",
			MSG_SUB_GETINFO, getNextRequestSequence(), szDatetime, BUFFER_DEVICE);
		sendDataViaInteractor_v2(szDevMsg, (uint32_t)strlen(szDevMsg));
	}
	void getUserList()
	{
		char szDatetime[20] = { 0 };
		formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
		char szUserMsg[256] = { 0 };
		sprintf_s(szUserMsg, sizeof(szUserMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"\",\"param2\":\"\",\"param3\":0,\"param4\":0}",
			MSG_SUB_GETINFO, getNextRequestSequence(), szDatetime, BUFFER_GUARDER);
		sendDataViaInteractor_v2(szUserMsg, (uint32_t)strlen(szUserMsg));
	}
	void getTaskList()
	{
		char szDatetime[20] = { 0 };
		formatDatetime(time(NULL), szDatetime, sizeof(szDatetime));
		char szTaskMsg[256] = { 0 };
		sprintf_s(szTaskMsg, sizeof(szTaskMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\":%d,"
			"\"datetime\":\"%s\",\"getType\":%d,\"param1\":\"\",\"param2\":\"\",\"param3\":0,\"param4\":0}",
			MSG_SUB_GETINFO, getNextRequestSequence(), szDatetime, BUFFER_TASK);
		sendDataViaInteractor_v2(szTaskMsg, (uint32_t)strlen(szTaskMsg));
	}
	void setLoadOrg(bool bValue_)
	{
		pthread_mutex_lock(&g_mutex4LoadOrg);
		g_bLoadOrg = bValue_;
		pthread_mutex_unlock(&g_mutex4LoadOrg);
	}
	bool getLoadOrg()
	{
		bool result = false;
		pthread_mutex_lock(&g_mutex4LoadOrg);
		result = g_bLoadOrg;
		pthread_mutex_unlock(&g_mutex4LoadOrg);
		return result;
	}
	void setLoadDevice(bool bValue_)
	{
		pthread_mutex_lock(&g_mutex4DevList);
		g_bLoadDevice = bValue_;
		pthread_mutex_unlock(&g_mutex4DevList);
	}
	bool getLoadDevice()
	{
		bool result = false;
		pthread_mutex_lock(&g_mutex4LoadDevice);
		result = g_bLoadDevice;
		pthread_mutex_unlock(&g_mutex4LoadDevice);
		return result;
	}
	void setLoadUser(bool bValue_) 
	{
		pthread_mutex_lock(&g_mutex4LoadUser);
		g_bLoadUser = bValue_;
		pthread_mutex_unlock(&g_mutex4LoadUser);
	}
	bool getLoadUser()
	{
		bool result = false;
		pthread_mutex_lock(&g_mutex4LoadUser);
		result = g_bLoadUser;
		pthread_mutex_unlock(&g_mutex4LoadUser);
		return result;
	}
	void setLoadTask(bool bValue_)
	{
		pthread_mutex_lock(&g_mutex4TaskList);
		g_bLoadTask = bValue_;
		pthread_mutex_unlock(&g_mutex4TaskList);
	}
	bool getLoadTask()
	{
		bool result = false;
		pthread_mutex_lock(&g_mutex4TaskList);
		result = g_bLoadTask;
		pthread_mutex_unlock(&g_mutex4TaskList);
		return result;
	}
	bool addDisconnectEvent(const char *, const char *);
	void handleDisconnectEvent();
	void checkAccClientList();
	void updateAccClient(const char *, const char *);
	void checkAppLinkList();
	void sendInteractAlive();

	bool addExpressMessage(access_service::ClientExpressMessage *);
	void dealExpressMessage();
	void parseExpressMessage(const access_service::ClientExpressMessage *);
	void sendExpressMessageToClient(const char * pMessage, const char * pClientId);
	void handleExpressAppLogin(access_service::AppLoginInfo *, unsigned long long ullMsgTime, const char * pClientId);
	void handleExpressAppLogout(access_service::AppLogoutInfo *, unsigned long long ullMsgTime, const char * pClientId);
	void handleExpressAppBind(access_service::AppBindInfo *, unsigned long long ullMsgTime, const char * pClientId);
	void handleExpressAppSubmitTask(access_service::AppSubmitTaskInfo *, unsigned long long, const char *);
	void handleExpressAppCloseTask(access_service::AppCloseTaskInfo *, unsigned long long, const char *);
	void handleExpressAppPosition(access_service::AppPositionInfo *, unsigned long long, const char *);
	void handleExpressAppFlee(access_service::AppSubmitFleeInfo *, unsigned long long, const char *);
	void handleExpressAppKeepAlive(access_service::AppKeepAlive *, unsigned long long, const char *);
	void handleExpressAppModifyAccountPassword(access_service::AppModifyPassword *, unsigned long long, const char *);
	void handleExpressAppQueryTask(access_service::AppQueryTask *, unsigned long long, const char *);
	void handleExpressAppDeviceCommand(access_service::AppDeviceCommandInfo *, const char *);
	void handleExpressAppQueryPerson(access_service::AppQueryPerson *, const char *);
	void handleExpressAppQueryTaskList(access_service::AppQueryTaskList *, const char *);
	void handleExpressAppQueryDeviceStatus(access_service::AppQueryDeviceStatus *, const char *);

	size_t getGuarderListSize();
	size_t getDeviceListSize();
	size_t getOrgListSize();

	//friend void * dealAppMsgThread(void *);
	friend void * dealTopicMsgThread(void *);
	friend void * dealInteractionMsgThread(void *);
	friend void dealExpressMsgThread(void *);
	friend void loadSessionThread(void *);
	friend void dealDisconnectEventThread(void *);
	friend void dealAppAccMsgThread(void *);
	
	friend int readAccInteract(zloop_t *, zsock_t *, void *);
	friend int readMsgSubscriber(zloop_t *, zsock_t *, void *);
	friend int readMsgInteractor(zloop_t *, zsock_t *, void *);
	friend int timerCb(zloop_t*, int, void *);

	friend void * superviseThread(void *);
	friend void zk_server_watcher(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx);
	friend void zk_escort_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_master_create_completion(int rc, const char * name, const void * data);
	friend void zk_access_master_exists_watcher(zhandle_t * zh, int type, int state, 
		const char * path, void * watcherCtx);
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
	int m_nEnableTaskLooseCheck;
	
	unsigned short m_usLogType;
	unsigned long long m_ullLogInst;
	char m_szLogRoot[256];
	
	char m_szInteractorIdentity[40];
	char m_szSeekerIdentity[40];

	zsock_t * m_accessSock;
	zsock_t * m_subscriberSock;
	zsock_t * m_interactorSock;
	std::mutex m_mutex4Interactor;
	zhandle_t * m_zkHandle;
	char m_szHost[512];
	char m_zkNodePath[256];
	bool m_bConnectZk; 
	ZkAccess m_zkAccess;
	ZkMidware m_zkMidware;
	std::mutex m_mutex4AccessSock;

	//message from app
	pthread_mutex_t m_mutex4AppMsgQueue;
	pthread_cond_t m_cond4AppMsgQueue;
	std::queue<MessageContent *> m_appMsgQueue;
	
	std::mutex m_mutex4AccAppMsgQue;
	std::condition_variable m_cond4AccAppMsgQue;
	std::queue<access_service::AccessAppMessage *> m_accAppMsgQue;
	std::thread m_thdAccAppMsg;

	pthread_t m_pthdAppMsg;
	pthread_mutex_t m_mutex4LinkDataList;
	typedef std::map<std::string, access_service::LinkDataInfo *> LinkDataList;
	LinkDataList m_linkDataList; //key:linkId 
	
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

	std::mutex m_mutex4LoadSessionSignal;
	std::condition_variable m_cond4LoadSessionSignal;
	std::thread m_thdLoadSession;
	std::queue<access_service::DisconnectEvent *> m_disconnEventQue;
	std::mutex m_mutex4DisconnEventQue;
	std::condition_variable m_cond4DissconnEventQue;
	std::thread m_thdDissconnEvent;

	std::mutex m_mutex4AccClientList;
	typedef std::map<std::string, access_service::AccessClientInfo *> AccessClientList;
	AccessClientList m_accClientList;

	std::queue<access_service::ClientExpressMessage *> m_cltExpressMsgQue;
	std::mutex m_mutex4CltExpressMsgQue;
	std::condition_variable m_cond4CltExpressMsgQue;
	std::thread m_thdHandleExpressMsg;

	typedef std::map<unsigned int, access_service::QueryPersonEvent *> QueryPersonEventList;
	QueryPersonEventList m_qryPersonEventList;
	std::mutex m_mutex4QryPersonEventList;
	
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
	typedef std::map<std::string, OrganizationEx *> OrgList;
	static OrgList g_orgList2;
	static pthread_mutex_t g_mutex4OrgList;
	static BOOL g_bInitBuffer;
	static pthread_mutex_t g_mutex4LoadOrg;
	static pthread_mutex_t g_mutex4LoadUser;
	static pthread_mutex_t g_mutex4LoadDevice;
	static pthread_mutex_t g_mutex4LoadTask;
	static bool g_bLoadOrg;
	static bool g_bLoadUser;
	static bool g_bLoadDevice;
	static bool g_bLoadTask;
};

#endif
