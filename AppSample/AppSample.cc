#include <WinSock2.h>
#include <WS2tcpip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mutex>
#include <thread>
#include <queue>
#include <map>
#include <condition_variable>
#include <time.h>
#include <string>
#include "rapidjson\document.h"
#include <Windows.h>
#include <fstream>

#pragma comment(lib, "ws2_32.lib")

typedef std::map<std::string, std::string> KVStringPair;

const char * kDefaultDeviceId = "3917862376";

struct MessageHead
{
	char mark[2];
	char version[2];
	unsigned int uiLen;
};

struct RecvData
{
	unsigned char * pData;
	unsigned int uiDataLen;
	RecvData()
	{
		pData = NULL;
		uiDataLen = 0;
	}
};

bool bLogin = false;
bool bBind = false;
bool bTask = false;
bool bModifyEncrypt = false;
bool bKeepAlive = false;

bool bRunning = false;
std::mutex mutex4DataQue;
std::condition_variable cond4DataQue;
std::mutex mutex4AppPos;
std::condition_variable cond4AppPos;
std::queue<RecvData *> dataQue;
char szSession[20] = { 0 };
char szTask[16] = { 0 };
char szDeviceId[20] = { 0 };
bool bConnected = false;
char szUser[64] = { 0 };
char szPasswd[64] = { 0 };

#define MAKEHEAD(x) {x.mark[0] = 'E';x.mark[1]='C';x.version[0]='1';x.version[1]='0';}

std::string Utf8ToAnsi(LPCSTR utf8)
{
	int WLength = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, NULL);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_UTF8, 0, utf8, -1, pszW, WLength);
	pszW[WLength] = '\0';
	int ALength = WideCharToMultiByte(CP_ACP, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca(ALength + 1);
	WideCharToMultiByte(CP_ACP, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr = pszA;
	return retStr;
}

std::string AnsiToUtf8(LPCSTR Ansi)
{
	int WLength = MultiByteToWideChar(CP_ACP, 0, Ansi, -1, NULL, 0);
	LPWSTR pszW = (LPWSTR)_alloca((WLength + 1) * sizeof(WCHAR));
	MultiByteToWideChar(CP_ACP, 0, Ansi, -1, pszW, WLength);
	int ALength = WideCharToMultiByte(CP_UTF8, 0, pszW, -1, NULL, 0, NULL, NULL);
	LPSTR pszA = (LPSTR)_alloca(ALength + 1);
	WideCharToMultiByte(CP_UTF8, 0, pszW, -1, pszA, ALength, NULL, NULL);
	pszA[ALength] = '\0';
	std::string retStr(pszA);
	return retStr;
}

void menu()
{
	printf("----------menu----------\n"
		"'i' or 'I': Login user\n"
		"'o' or 'O': Logout user\n"
		"'b' or 'B': Bind device\n"
		"'u' or 'U': Unbind device\n"
		"'s' or 'S': Submit task\n"
		"'c' or 'C': Close task\n"
		"'n' or 'N': Notify position\n"
		"'f' or 'F': Flee\n"
		"'r' or 'R': Revoke flee\n"
		"'m' or 'M': Modify\n"
		"'t' or 'T': Query task\n"
		"'k' or 'K': Keep Link\n"
		"'h' or 'H': help menu\n"
		"'1','2','3','4': send device command\n"
		"'5': query person list\n"
		"'6': query task list\n"
		"'7': query device status\n"
		"'q' or 'Q': quit\n");
}

void add(RecvData *);

void recv_func(SOCKET sock)
{
	timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	fd_set fdRead;
	//char szRecv[1024] = { 0 };
  unsigned int uiLen = 2 * 1024 * 1024;
  char * pRecv = new char[uiLen];
  memset(pRecv, 0, uiLen);

	bool bRecvHead = false;
	MessageHead msgHead;
	size_t nHeadSize = sizeof(MessageHead);
	unsigned int nOffset = 0;
	unsigned int nRecvLen = 0;
	unsigned int nDataLen = 0;
	while (bRunning) {
		FD_ZERO(&fdRead);
		FD_SET(sock, &fdRead);
		int n = select((int)(sock + 1), &fdRead, NULL, NULL, &timeout);
		if (n == SOCKET_ERROR) {
			printf("[RECEIVE]select error:%d\n", WSAGetLastError());
			break;
		}
		else if (n == 0) {
			continue;
		}
		else if (FD_ISSET(sock, &fdRead)) {
			if (!bRecvHead) {
				if (nOffset == 0) {
					nRecvLen = recv(sock, (char *)&msgHead, (int)nHeadSize, 0);
					if (nRecvLen == (unsigned int)nHeadSize) {
						bRecvHead = true;
						nDataLen = msgHead.uiLen;
					}
					else if (nRecvLen < nHeadSize) {
						nOffset = nRecvLen;
					}
					else if (nRecvLen == SOCKET_ERROR) {
						printf("[RECEIVE]recv error:%d\n", WSAGetLastError());
						bConnected = false;
						break;
					}
				}
				else {
					nRecvLen = recv(sock, (char *)&msgHead + nOffset, (unsigned int)nHeadSize - nOffset, 0);
					if (nRecvLen + nOffset < nHeadSize) {
						nOffset += nRecvLen;
					}
					else if (nRecvLen + nOffset == nHeadSize) {
						nDataLen = msgHead.uiLen;
						bRecvHead = true;
					}
				}
			}
			else {
				if (nOffset == 0) {
					nRecvLen = recv(sock, pRecv, nDataLen, 0);
					if (nRecvLen < nDataLen) {
						nOffset = nRecvLen;
					}
					else if (nRecvLen == nDataLen) {
						printf("[RECEIVE]recv %d data\n", nDataLen);
						RecvData * pRecvData = (RecvData *)malloc(sizeof(RecvData));
						pRecvData->uiDataLen = nDataLen;
						pRecvData->pData = (unsigned char *)malloc(nDataLen + 1);
						memcpy_s(pRecvData->pData, nDataLen, pRecv, nDataLen);
						pRecvData->pData[nDataLen] = '\0';
						add(pRecvData);
						bRecvHead = false;
						memset(&msgHead, 0, nHeadSize);
					}
					else if (nRecvLen == SOCKET_ERROR) {
						printf("[RECEIVE]recv error:%d\n", WSAGetLastError());
						bConnected = false;
						break;
					}
				}
				else {
					nRecvLen = recv(sock, pRecv + nOffset, nDataLen - nOffset, 0);
					if (nRecvLen + nOffset == nDataLen) {
						printf("[RECEIVE]recv %d data\n", nDataLen);
						RecvData * pRecvData = (RecvData *)malloc(sizeof(RecvData));
						pRecvData->uiDataLen = nDataLen;
						pRecvData->pData = (unsigned char *)malloc(nDataLen + 1);
						memcpy_s(pRecvData->pData, nDataLen, pRecv, nDataLen);
						pRecvData->pData[nDataLen] = '\0';
						add(pRecvData);
						bRecvHead = false;
						memset(&msgHead, 0, nHeadSize);
					}
					else if (nRecvLen + nOffset < nDataLen) {
						nOffset += nRecvLen;
					}
					else if (nRecvLen == SOCKET_ERROR) {
						printf("[RECEIVE]recv error:%d\n", WSAGetLastError());
						bConnected = false;
						break;
					}
				}
			}
		}
	}
}

void sendMsg(SOCKET sock, const char * pMsg, size_t nMsgSize)
{
	if (pMsg && nMsgSize && sock > 0 && bConnected) {
		std::string strUtf8Msg = AnsiToUtf8(pMsg);
		MessageHead msgHead;
		MAKEHEAD(msgHead);
		//msgHead.uiLen = nMsgSize;
		//size_t nHeadSize = sizeof(MessageHead);
		//unsigned int nBufLen = nMsgSize + nHeadSize;
		//unsigned char * pBuf = (unsigned char *)malloc(nBufLen + 1);
		//memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
		//memcpy_s(pBuf + nHeadSize, nMsgSize, pMsg, nMsgSize);
		size_t nUtf8MsgSize = strUtf8Msg.size();
		msgHead.uiLen = (unsigned int)nUtf8MsgSize;
		size_t nHeadSize = sizeof(MessageHead);
		unsigned int nBufLen = (unsigned int)(nUtf8MsgSize + nHeadSize);
		unsigned char * pBuf = (unsigned char *)malloc(nBufLen + 1);
		memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
		memcpy_s(pBuf + nHeadSize, nUtf8MsgSize, strUtf8Msg.c_str(), nUtf8MsgSize);
		pBuf[nBufLen] = '\0';
		for (unsigned int i = (unsigned int)nHeadSize; i < nBufLen; i++) {
			pBuf[i] += 1;
			pBuf[i] ^= '8';
		}
		if (send(sock, (const char *)pBuf, nBufLen, 0) == SOCKET_ERROR) {
			bConnected = true;
		}
		free(pBuf);
	}
}

void format_datetime(unsigned long ulSrcTime, char * pStrDatetime, size_t nStrDatetimeLen)
{
	tm tm_time;
	time_t srcTime = ulSrcTime;
	localtime_s(&tm_time, &srcTime);
	char szDatetime[16] = { 0 };
	snprintf(szDatetime, sizeof(szDatetime), "%04d%02d%02d%02d%02d%02d", tm_time.tm_year + 1900,
		tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	size_t nLen = strlen(szDatetime);
	if (pStrDatetime && nStrDatetimeLen >= nLen) {
		strncpy_s(pStrDatetime, nStrDatetimeLen, szDatetime, nLen);
	}
}

void send_func(SOCKET sock)
{
	menu();
	while (bRunning) {
		char c;
		scanf_s("%c", &c, 1);
		char szDatetime[16] = { 0 };
		format_datetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
		if (c == 'i' || c == 'I') { //test || test
			if (!bLogin) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":1,\"account\":\"%s\",\"passwd\":\"%s\",\"datetime\":\"%s\","
					"\"handset\":\"\"}", szUser, szPasswd, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]login: already login\n");
			}
		}
		else if (c == 'o' || c == 'O') {
			if (bLogin) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":2,\"session\":\"%s\",\"datetime\":\"%s\"}", 
					szSession, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]logout: login first\n");
			}
		}
		else if (c == 'b' || c == 'B') {
			//if (bLogin && !bBind) {
			if (bLogin) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":3,\"session\":\"%s\",\"deviceId\":\"%s\","
					"\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]bind: need login or already bind\n");
			}
		}
		else if (c == 'u' || c == 'U') {
			if (bLogin && bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":4,\"session\":\"%s\",\"deviceId\":\"%s\","
					"\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]unbind: need login and bind first\n");
			}
		}
		else if (c == 's' || c == 'S') {
			if (bLogin && bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":5,\"session\":\"%s\",\"type\":1,\"limit\":1,"
					"\"destination\":\"新世纪花园\",\"target\":\"35529120010131&王小二\","
					"\"datetime\":\"%s\"}", 
					szSession, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]submit task: need login and bind first\n");
			}
		}
		else if (c == 'c' || c == 'C') {
			if (bTask) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":6,\"session\":\"%s\",\"taskId\":\"%s\","
					"\"closeType\":0,\"datetime\":\"%s\"}", szSession, szTask, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]close task: need task first\n");
			}
		}
		else if (c == 'f' || c == 'F') {
			if (bTask) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":8,\"session\":\"%s\",\"taskId\":\"%s\","
					"\"datetime\":\"%s\"}", szSession, szTask, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
			else {
				printf("[SEND]flee task: need task first\n");
			}
		}
		else if (c == 'r' || c == 'R') {
			if (bTask) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":9,\"session\":\"%s\",\"taskId\":\"%s\","
					"\"datetime\":\"%s\"}", szSession, szTask, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == 'n' || c == 'N') {
			if (bTask) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":7,\"session\":\"%s\",\"taskId\":\"%s\","
					"\"datetime\":\"%s\",\"lat\":121.3011192,\"lng\":30.1359912}", szSession, szTask,
					szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
				printf("[SEND]notify task position\n");
			}
			else {
				printf("[SEND]notice need task first\n");
			}
		}
		else if (c == 'h' || c == 'H') {
			menu();
		}
		else if (c == 'm' || c == 'M') {
			if (bLogin) {
				char szMsg[256] = { 0 };
				if (!bModifyEncrypt) {
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":12,\"session\":\"%s\",\"currPasswd\":\"%s\","
						"\"newPasswd\":\"%s\",\"datetime\":\"%s\"}", szSession, "3cf2bc71982179c0d0944dee43f"
						"b23d2", "123456", szDatetime);
					sendMsg(sock, szMsg, strlen(szMsg));
					bModifyEncrypt = true;
					printf("[SEND]modify passwd to decrypt\n");
				}
				else {
					snprintf(szMsg, sizeof(szMsg), "{\"cmd\":12,\"session\":\"%s\",\"currPasswd\":\"%s\","
						"\"newPasswd\":\"%s\",\"datetime\":\"%s\"}", szSession, "123456", "3cf2bc71982179c0d"
						"0944dee43fb23d2", szDatetime);
					sendMsg(sock, szMsg, strlen(szMsg));
					bModifyEncrypt = false;
					printf("[SEND]modify passwd to encrypt\n");
				}
			}
			else {
				printf("[SEND]need login first\n");
			}
		}
		else if (c == 'q' || c == 'Q') {
			bRunning = false;
			break;
		}
		else if (c == 't' || c == 'T') {
			if (bTask) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":13,\"session\":\"%s\",\"taskId\":\"%s\",\"date"
					"time\":\"%s\"}", szSession, szTask, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == 'k' || c == 'K') {
			if (!bKeepAlive) {
				bKeepAlive = true;
				printf("turn on keep alive\n");
			}
			else {
				bKeepAlive = false;
				printf("turn off keep alive\n");
			}
		}
		else if (c == '1') {
			if (bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":14,\"session\":\"%s\",\"deviceId\":\"%s\",\"param1\":1"
					",\"param2\":0,\"seq\":0,\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == '2') {
			if (bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":14,\"session\":\"%s\",\"deviceId\":\"%s\",\"param1\":1"
					",\"param2\":1,\"seq\":1,\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == '3') {
			if (bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":14,\"session\":\"%s\",\"deviceId\":\"%s\",\"param1\":2"
					",\"param2\":0,\"seq\":2,\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == '4') {
			if (bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":14,\"session\":\"%s\",\"deviceId\":\"%s\",\"param1\":3"
					",\"param2\":0,\"seq\":3,\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
				sendMsg(sock, szMsg, strlen(szMsg));
			}
		}
		else if (c == '5') {
			char szMsg[256] = { 0 };
			snprintf(szMsg, sizeof(szMsg), "{\"cmd\":15,\"session\":\"%s\",\"queryPid\":\"12345678\",\"queryMode\":4"
				",\"seq\":1,\"datetime\":\"%s\"}", szSession, szDatetime);
			sendMsg(sock, szMsg, strlen(szMsg));
		}
		else if (c == '6') {
			char szMsg[256] = { 0 };
			sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":16,\"orgId\":\"\",\"seq\":10,\"datetime\":\"%s\"}", szDatetime);
			sendMsg(sock, szMsg, strlen(szMsg));
			printf("[SEND]query task list\n");
		}
    else if (c == '7') {
      if (bBind) {
        char szMsg[256] = { 0 };
        sprintf_s(szMsg, sizeof(szMsg), "{\"cmd\":19,\"session\":\"%s\",\"deviceId\":\"%s\",\"seq\":102,"
          "\"datetime\":\"%s\"}", szSession, szDeviceId, szDatetime);
        sendMsg(sock, szMsg, strlen(szMsg));
        printf("[send]query device status\n");
      }
    }
		Sleep(200);
	}
}

void parse(RecvData * pRecvData)
{
	rapidjson::Document doc;
	std::string strData = Utf8ToAnsi((const char *)pRecvData->pData);
	//if (!doc.Parse((const char *)pRecvData->pData).HasParseError()) {
	if (!doc.Parse((const char *)strData.c_str()).HasParseError()) {
		int nCmd = 0;
		int nRet = -1;
		if (doc.HasMember("cmd")) {
			if (doc["cmd"].IsInt()) {
				nCmd = doc["cmd"].GetInt();
			}
		}
		switch (nCmd) {
			case 101: {  //login
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					bLogin = true;
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							size_t nSize = doc["session"].GetStringLength();
							if (nSize) {
								strncpy_s(szSession, sizeof(szSession), doc["session"].GetString(), nSize);
								printf("[PARSE]login session: %s\n", szSession);
							}
						}
					}
					if (doc.HasMember("taskInfo")) {
						if (doc["taskInfo"].IsArray()) {
							if (!doc["taskInfo"].GetArray().Empty()) {
								if (doc["taskInfo"][0].HasMember("taskId")) {
									if (doc["taskInfo"][0]["taskId"].IsString()) {
										size_t nSize = doc["taskInfo"][0]["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(szTask, sizeof(szTask), doc["taskInfo"][0]["taskId"].GetString(), nSize);
											printf("[PARSE]login task:%s\n", szTask);
											std::unique_lock<std::mutex> lock(mutex4AppPos);
											bTask = true;
											cond4AppPos.notify_one();
										}
									}
								}
								if (doc["taskInfo"][0].HasMember("deviceId")) {
									if (doc["taskInfo"][0]["deviceId"].GetString()) {
										size_t nSize = doc["taskInfo"][0]["deviceId"].GetStringLength();
										if (nSize) {
											printf("[PARSE]login task device: %s\n", doc["taskInfo"][0]["deviceId"].GetString());
											bBind = true;
										}
									}
								}
								if (doc["taskInfo"][0].HasMember("type")) {
									if (doc["taskInfo"][0]["type"].IsInt()) {
										printf("[PARSE]login task type:%d\n", doc["taskInfo"][0]["type"].GetInt());
									}
								}
								if (doc["taskInfo"][0].HasMember("limit")) {
									if (doc["taskInfo"][0]["limit"].IsInt()) {
										printf("[PARSE]login task limit:%d\n", doc["taskInfo"][0]["limit"].GetInt());
									}
								}
								if (doc["taskInfo"][0].HasMember("target")) {
									if (doc["taskInfo"][0]["target"].IsString()) {
										printf("[PARSE]login task target:%s\n", doc["taskInfo"][0]["target"].GetString());
									}
								}
								if (doc["taskInfo"][0].HasMember("destination")) {
									if (doc["taskInfo"][0]["destination"].IsString()) {
										printf("[PARSE]login task destination:%s\n", doc["taskInfo"][0]["destination"].GetString());
									}
								}
								if (doc["taskInfo"][0].HasMember("startTime")) {
									if (doc["taskInfo"][0]["startTime"].IsString()) {
										printf("[PARSE]login task startTime:%s\n", doc["taskInfo"][0]["startTime"].GetString());
									}
								}
								if (doc["taskInfo"][0].HasMember("deviceState")) {
									if (doc["taskInfo"][0]["deviceState"].IsInt()) {
										printf("[PARSE]login task state:%d\n", doc["taskInfo"][0]["deviceState"].GetInt());
									}
								}
								if (doc["taskInfo"][0].HasMember("online")) {
									if (doc["taskInfo"][0]["online"].IsInt()) {
										printf("[PARSE]login task online=%d\n", doc["taskInfo"][0]["online"].GetInt());
									}
								}
								if (doc["taskInfo"][0].HasMember("battery")) {
									if (doc["taskInfo"][0]["battery"].IsInt()) {
										printf("[PARSE]login task battery:%d\n", doc["taskInfo"][0]["battery"].GetInt());
									}
								}
								if (doc["taskInfo"][0].HasMember("handset")) {
									if (doc["taskInfo"][0]["handset"].IsString()) {
										printf("[PARSE]login task handset:%s\n", doc["taskInfo"][0]["handset"].GetString());
									}
								}
							}
						}
					}
				}
				else {
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
					printf("[PARSE]login failed, retcode=%d\n", nRet);
				}
				break;
			}
			case 102: {	//logout
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]logout session:%s\n", doc["session"].GetString());
						}
					}
					szSession[0] = '\0';
					bLogin = false;
				}
				else {
					printf("[PARSE]logout failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 103: { //bind
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					bBind = true;
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]bind session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("battery")) {
						if (doc["battery"].IsInt()) {
							printf("[PARSE]bind battery:%d\n", doc["battery"].GetInt());
						}
					}
				}
				else {
					printf("[PARSE]bind failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 104: {	//unbind
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					bBind = false;
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]unbind session:%s\n", doc["session"].GetString());
						}
					}
				}
				else {
					printf("[PARSE]unbind failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 105: { //task
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]task session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("taskId")) {
						if (doc["taskId"].IsString()) {
							size_t nSize = doc["taskId"].GetStringLength();
							if (nSize) {
								strncpy_s(szTask, sizeof(szTask), doc["taskId"].GetString(), nSize);
								printf("[PARSE]task: %s\n", szTask);
							}
						}
					}
					std::unique_lock<std::mutex> lock(mutex4AppPos);
					bTask = true;
					cond4AppPos.notify_one();
				}
				else {
					printf("[PARSE]task submit failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 106: { //close task
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]task session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("taskId")) {
						if (doc["taskId"].IsString()) {
							printf("[PARSE]task: %s\n", doc["taskId"].GetString());
						}
					}
					std::unique_lock<std::mutex> lock(mutex4AppPos);
					bTask = false;
					szTask[0] = '\0';
				}
				else {
					printf("[PARSE]task close failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 107: {
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]session=%s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						printf("[PARSE]retcode=%d\n", doc["retcode"].GetInt());
					}
				}
				if (doc.HasMember("taskId")) {
					if (doc["taskId"].IsString()) {
						printf("[PARSE]taskId=%s\n", doc["taskId"].GetString());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]datetime=%s\n", doc["datetime"].GetString());
					}
				}
				break;
			}
			case 108: { //flee
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]flee task session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("taskId")) {
						if (doc["taskId"].IsString()) {
							size_t nSize = doc["taskId"].GetStringLength();
							if (nSize) {
								strncpy_s(szTask, sizeof(szTask), doc["taskId"].GetString(), nSize);
								printf("[PARSE]flee task: %s\n", szTask);
							}
						}
					}
				}
				else {
					printf("[PARSE]flee failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 109: {	//revoke
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						nRet = doc["retcode"].GetInt();
					}
				}
				if (nRet == 0) {
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							printf("[PARSE]revoke flee task session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("taskId")) {
						if (doc["taskId"].IsString()) {
							printf("[PARSE]revoke flee task:%s\n", doc["taskId"].GetString());
						}
					}
				}
				else {
					printf("[PARSE]revoke flee failed, retcode=%d\n", nRet);
					if (nRet == 10) {
						szSession[0] = '\0';
						bLogin = false;
					}
				}
				break;
			}
			case 10: { //msg notify
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]notice session:%s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("msgType")) {
					if (doc["msgType"].IsInt()) {
						printf("[PARSE]notice msg type:%d\n", doc["msgType"].GetInt());
					}
				}
				if (doc.HasMember("deviceId")) {
					if (doc["deviceId"].GetString()) {
						printf("[PARSE]notice deviceId:%s\n", doc["deviceId"].GetString());
					}
				}
				if (doc.HasMember("mode")) {
					if (doc["mode"].IsInt()) {
						printf("[PARSE]notice mode:%d\n", doc["mode"].GetInt());
					}
				}
				if (doc.HasMember("battery")) {
					if (doc["battery"].IsInt()) {
						printf("[PARSE]notice battery:%d\n", doc["battery"].GetInt());
					}
				}
				if (doc.HasMember("lat")) {
					if (doc["lat"].IsDouble()) {
						printf("[PARSE]notice lat:%.06f\n", doc["lat"].GetDouble());
					}
				}
				if (doc.HasMember("lng")) {
					if (doc["lng"].IsDouble()) {
						printf("[PARSE]notice lng:%.06f\n", doc["lng"].GetDouble());
					}
				}
				if (doc.HasMember("coordinate")) {
					if (doc["coordinate"].IsInt()) {
						printf("[PARSE]notice coordinate: %d\n", doc["coordinate"].GetInt());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]notice datetime:%s\n", doc["datetime"].GetString());
					}
				}
				break;
			}
      case 17: {
        if (doc.HasMember("taskId")) {
          if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
            printf("[PARSE]notify task taskId=%s\n", doc["taskId"].GetString());
          }
        }
        if (doc.HasMember("deviceId")) {
          if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
            printf("[PARSE]notify task deviceId=%s\n", doc["deviceId"].GetString());
          }
        }
        if (doc.HasMember("guarder")) {
          if (doc["guarder"].IsString() && doc["guarder"].GetStringLength()) {
            printf("[PARSE]notify task guarder=%s\n", doc["guarder"].GetString());
          }
        }
        if (doc.HasMember("target")) {
          if (doc["target"].IsString() && doc["target"].GetStringLength()) {
            printf("[PARSE]notify task target=%s\n", doc["target"].GetString());
          }
        }
        if (doc.HasMember("destination")) {
          if (doc["destination"].IsString() && doc["destination"].GetStringLength()) {
            printf("[PARSE]notify task destination=%s\n", doc["destination"].GetString());
          }
        }
        if (doc.HasMember("startTime")) {
          if (doc["startTime"].IsString() && doc["startTime"].GetStringLength()) {
            printf("[PARSE]notify task startTime=%s\n", doc["startTime"].GetString());
          }
        }
        if (doc.HasMember("limit")) {
          if (doc["limit"].IsInt()) {
            printf("[PARSE]notify task limit=%d\n", doc["limit"].GetInt());
          }
        }
        if (doc.HasMember("type")) {
          if (doc["type"].IsInt()) {
            printf("[PARSE]notify task type=%d\n", doc["type"].GetInt());
          }
        }
        break;
      }
      case 18: {
        if (doc.HasMember("taskId")) {
          if (doc["taskId"].IsString() && doc["taskId"].GetStringLength()) {
            printf("[PARSE]notify task close taskId=%s\n", doc["taskId"].GetString());
          }
        }
        if (doc.HasMember("datetime")) {
          if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
            printf("[PARSE]notify task close datetime=%s\n", doc["datetime"].GetString());
          }
        }
        break;
      }
      case 111: {
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]session: %s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("seq")) {
					if (doc["seq"].IsInt()) {
						printf("[PARSE]seq=%d\n", doc["seq"].GetInt());
					}
				}
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						printf("[PARSE]retcode=%d\n", doc["retcode"].GetInt());
					}
				}
				
				break;
			}
			case 112: { //modify passwd reply
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]modify password session=%s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						printf("[PARSE]modify password retcode=%d\n", doc["retcode"].GetInt());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]modify password datetime=%s\n", doc["datetime"].GetString());
					}
				}
				break;
			}
			case 113: { //query task info
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]query task session=%s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						printf("[PARSE]query task retcode=%d\n", doc["retcode"].GetInt());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]query task datetime=%s\n", doc["datetime"].GetString());
					}
				}
				if (doc.HasMember("taskInfo")) {
					if (doc["taskInfo"].IsArray()) {
						if (!doc["taskInfo"].GetArray().Empty()) {
							if (doc["taskInfo"][0].HasMember("taskId")) {
								if (doc["taskInfo"][0]["taskId"].IsString()) {
									printf("[PARSE]query task taskId=%s\n", doc["taskInfo"][0]["taskId"].GetString());
								}
							}
							if (doc["taskInfo"][0].HasMember("deviceId")) {
								if (doc["taskInfo"][0]["deviceId"].IsString()) {
									printf("[PARSE]query task device=%s\n", doc["taskInfo"][0]["deviceId"].GetString());
								}
							}
							if (doc["taskInfo"][0].HasMember("type")) {
								if (doc["taskInfo"][0]["type"].IsInt()) {
									printf("[PARSE]query task type=%d\n", doc["taskInfo"][0]["type"].GetInt());
								}
							}
							if (doc["taskInfo"][0].HasMember("limit")) {
								if (doc["taskInfo"][0]["limit"].IsInt()) {
									printf("[PARSE]query task limit=%d\n", doc["taskInfo"][0]["limit"].GetInt());
								}
							}
							if (doc["taskInfo"][0].HasMember("target")) {
								if (doc["taskInfo"][0]["target"].IsString()) {
									printf("[PARSE]query task target=%s\n", doc["taskInfo"][0]["target"].GetString());
								}
							}
							if (doc["taskInfo"][0].HasMember("destination")) {
								if (doc["taskInfo"][0]["destination"].IsString()) {
									printf("[PARSE]query task destination=%s\n", doc["taskInfo"][0]["destination"].GetString());
								}
							}
							if (doc["taskInfo"][0].HasMember("startTime")) {
								if (doc["taskInfo"][0]["startTime"].IsString()) {
									printf("[PARSE]query task startTime=%s\n", doc["taskInfo"][0]["startTime"].GetString());
								}
							}
							if (doc["taskInfo"][0].HasMember("deviceState")) {
								if (doc["taskInfo"][0]["deviceState"].IsInt()) {
									printf("[PARSE]query task state=%d\n", doc["taskInfo"][0]["deviceState"].GetInt());
								}
							}
							if (doc["taskInfo"][0].HasMember("online")) {
								if (doc["taskInfo"][0]["online"].IsInt()) {
									printf("[PARSE]query task online=%d\n", doc["taskInfo"][0]["online"].GetInt());
								}
							}
							if (doc["taskInfo"][0].HasMember("battery")) {
								if (doc["taskInfo"][0]["battery"].IsInt()) {
									printf("[PARSE]query task battery=%d\n", doc["taskInfo"][0]["battery"].GetInt());
								}
							}
							if (doc["taskInfo"][0].HasMember("handset")) {
								if (doc["taskInfo"][0]["handset"].IsString()) {
									printf("[PARSE]query task handset:%s\n", doc["taskInfo"][0]["handset"].GetString());
								}
							}
						}
					}
				}
				break;
			}
			case 114: {
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]send cmd session=%s\n", doc["session"].GetString());
					}
				}
				if (doc.HasMember("deviceId")) {
					if (doc["deviceId"].IsString()) {
						printf("[PARSE]send cmd device=%s\n", doc["deviceId"].GetString());
					}
				}
				if (doc.HasMember("seq")) {
					if (doc["seq"].IsInt()) {
						printf("[PARSE]send cmd seq=%d\n", doc["seq"].GetInt());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]send cmd datetime=%s\n", doc["datetime"].GetString());
					}
				}
				if (doc.HasMember("retcode")) {
					if (doc["retcode"].IsInt()) {
						printf("[PARSE]send cmd retcode=%d\n", doc["retcode"].GetInt());
					}
				}
				break;
			}
			case 115: {
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[PARSE]query person reply session=%s\n", doc["session"].GetString());
					}
					if (doc.HasMember("seq")) {
						if (doc["seq"].IsInt()) {
							printf("[PARSE]query person reply seq=%d\n", doc["seq"].GetInt());
						}
					}
					if (doc.HasMember("datetime")) {
						if (doc["datetime"].IsString()) {
							printf("[PARSE]query person datetime=%s\n", doc["datetime"].GetString());
						}
					}
					int nCount = 0;
					if (doc.HasMember("count")) {
						if (doc["count"].IsInt()) {
							nCount = doc["count"].GetInt();
							printf("[PARSE]query person reply count=%d\n", nCount);
						}
					}
					if (doc.HasMember("personList")) {
						if (doc["personList"].IsArray()) {
							for (int i = 0; i < nCount; i++) {
								if (doc["personList"][i].HasMember("id")) {
									if (doc["personList"][i]["id"].IsString()) {
										printf("[PARSE]query person reply %d, id=%s\n", i, doc["personList"][i]["id"].GetString());
									}
								}
								if (doc["personList"][i].HasMember("name")) {
									if (doc["personList"][i]["name"].IsString()) {
										printf("[PARSE]query person reply %d, name=%s\n", i, doc["personList"][i]["name"].GetString());
									}
								}
								if (doc["personList"][i].HasMember("state")) {
									if (doc["personList"][i]["state"].IsInt()) {
										printf("[PARSE]query person reply %d, state=%d\n", i, doc["personList"][i]["state"].GetInt());
									}
								}
							}
						}
					}
				}
				break;
			}
			case 116: {
				if (doc.HasMember("orgId")) {
					if (doc["orgId"].IsString()) {
						printf("[PARSE]query task list org=%s\n", doc["orgId"].GetString());
					}
				}
				if (doc.HasMember("seq")) {
					if (doc["seq"].IsInt()) {
						printf("[PARSE]query task list seq=%d\n", doc["seq"].GetInt());
					}
				}
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]query task list datetime=%s\n", doc["datetime"].GetString());
					}
				}
				int nCount = 0;
				if (doc.HasMember("count")) {
					if (doc["count"].IsInt()) {
						nCount = doc["count"].GetInt();
						printf("[PARSE]query task list count = %d\n", nCount);
					}
				}
				if (doc.HasMember("list")) {
					if (doc["list"].IsArray()) {
						if (nCount) {
							for (int i = 0; i < nCount; i++) {
								if (doc["list"][i].HasMember("taskId")) {
									if (doc["list"][i]["taskId"].IsString()) {
										printf("[PARSE]query task list %d: taskId=%s\n", i, doc["list"][i]["taskId"].GetString());
									}
								}
								if (doc["list"][i].HasMember("deviceId")) {
									if (doc["list"][i]["deviceId"].IsString()) {
										printf("[PARSE]query task list %d: deviceId=%s\n", i, doc["list"][i]["deviceId"].GetString());
									}
								}
								if (doc["list"][i].HasMember("guarderId")) {
									if (doc["list"][i]["guarderId"].IsString()) {
										printf("[PARSE]query task list %d: guarderId=%s\n", i, doc["list"][i]["guarderId"].GetString());
									}
								}
								if (doc["list"][i].HasMember("target")) {
									if (doc["list"][i]["target"].IsString()) {
										printf("[PARSE]query task list %d: target=%s\n", i, doc["list"][i]["target"].GetString());
									}
								}
								if (doc["list"][i].HasMember("destination")) {
									if (doc["list"][i]["destination"].IsString()) {
										printf("[PARSE]query task list %d: destination=%s\n", i, doc["list"][i]["destination"].GetString());
									}
								}
								if (doc["list"][i].HasMember("type")) {
									if (doc["list"][i]["type"].IsInt()) {
										printf("[PARSE]query task list %d: type=%d\n", i, doc["list"][i]["type"].GetInt());
									}
								}
								if (doc["list"][i].HasMember("limit")) {
									if (doc["list"][i]["limit"].IsInt()) {
										printf("[PARSE]query task list %d: limit=%d\n", i, doc["list"][i]["limit"].GetInt());
									}
								}
								if (doc["list"][i].HasMember("startTime")) {
									if (doc["list"][i]["startTime"].IsString()) {
										printf("[PARSE]query task list %d: startTime=%s\n", i, doc["list"][i]["startTime"].GetString());
									}
								}
							}
						}
					}
				}
				break;
			}
      case 119: {
        if (doc.HasMember("session")) {
          if (doc["session"].IsString() && doc["session"].GetStringLength()) {
            printf("[PARSE]query device status: session=%s\n", doc["session"].GetString());
          }
        }
        if (doc.HasMember("deviceId")) {
          if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
            printf("[PARSE]query device status: deviceId=%s\n", doc["deviceId"].GetString());
          }
        }
        if (doc.HasMember("retcode")) {
          if (doc["retcode"].IsInt()) {
            printf("[PARSE]query device status: retcode=%d\n", doc["retcode"].GetInt());
          }
        }
        if (doc.HasMember("status")) {
          if (doc["status"].IsInt()) {
            printf("[PARSE]query device status: status=%d\n", doc["status"].GetInt());
          }
        }
				if (doc.HasMember("online")) {
					if (doc["online"].IsInt()) {
						printf("[PARSE]queur device status: online=%d\n", doc["online"].GetInt());
					}
				}
        if (doc.HasMember("battery")) {
          if (doc["battery"].IsInt()) {
            printf("[PARSE]query device status: battery=%d\n", doc["battery"].GetInt());
          }
        }
        if (doc.HasMember("seq")) {
          if (doc["seq"].IsInt()) {
            printf("[PARSE]query device status: seq=%d\n", doc["seq"].GetInt());
          }
        }
        if (doc.HasMember("datetime")) {
          if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
            printf("[PARSE]query device status: datetime=%s\n", doc["datetime"].GetString());
          }
        }
        break;
      }
      default: {
				printf("not support cmd\n");
				break;
			}
		}
	}
	else {
		printf("[PARSE]parse JSON data error\n");
	}
}

void parse_func()
{
	do {
		std::unique_lock <std::mutex> lock(mutex4DataQue);
		cond4DataQue.wait(lock, [&] {
			return (!dataQue.empty() || !bRunning);
		});
		if (!bRunning && dataQue.empty()) {
			break;
		}
		RecvData * pRecvData = dataQue.front();
		dataQue.pop();
		if (pRecvData) {
			for (unsigned int i = 0; i < pRecvData->uiDataLen; i++) {
				pRecvData->pData[i] ^= '8';
				pRecvData->pData[i] -= 1;
			}
			//printf("[PARSE]%s\n", (char *)pRecvData->pData);
			parse(pRecvData);
			free(pRecvData->pData);
			free(pRecvData);
		}
	} while (1);
}

void add(RecvData * pRecvData)
{
	if (pRecvData && pRecvData->pData) {
		std::unique_lock <std::mutex> lock(mutex4DataQue);
		dataQue.push(pRecvData);
		if (dataQue.size() == 1) {
			cond4DataQue.notify_one();
		}
	}
}

void position_func(SOCKET sock)
{
	time_t nLastTime = time(NULL);
	bool bFirst = true;
	do {
		std::unique_lock<std::mutex> lock(mutex4AppPos);
		cond4AppPos.wait(lock, [&] {
			return (bTask || !bRunning);
		});
		if (!bRunning) {
			break;
		}
		time_t nCurrTime = time(NULL);
		bool bPosition = false;
		if (bFirst) {
			bPosition = true;
			bFirst = false;
		}
		else {
			double dInterval = difftime(nCurrTime, nLastTime);
			if (dInterval >= 120.00) {
				bPosition = true;
			}
		}
		if (bPosition && bTask) {
			nLastTime = nCurrTime;
			char szDateTime[20] = { 0 };
			format_datetime((unsigned long)nCurrTime, szDateTime, sizeof(szDateTime));
			char szCmd[256] = { 0 };
			snprintf(szCmd, sizeof(szCmd), "{\"cmd\":7,\"session\":\"%s\",\"taskId\":\"%s\",\"lat\":%f,\"lng\":%f"
				",\"datetime\":\"%s\"}", szSession, szTask, 30.321070, 120.189588, szDateTime);
			sendMsg(sock, szCmd, strlen(szCmd));
			printf("[Position]app notice postion at time:%lu\n", (unsigned long)nCurrTime);
		}
	} while (1);
}

void alive_func(SOCKET sock)
{
	while (bRunning) {
		if (bKeepAlive) {
			if (strlen(szSession) && bLogin) {
				char szDateTime[20] = { 0 };
				unsigned long now = (unsigned long)time(NULL);
				format_datetime(now, szDateTime, sizeof(szDateTime));
				char szCmd[256] = { 0 };
				snprintf(szCmd, sizeof(szCmd), "{\"cmd\":11,\"session\":\"%s\",\"seq\":1,\"datetime\":\"%s\"}",
					szSession, szDateTime);
				sendMsg(sock, szCmd, strlen(szCmd));
				printf("[Alive]app keep alive at time: %lu\n", now);
			}
			Sleep(7000);
		}
		Sleep(3000);
	}
}

int loadConf(const char * szFileName, KVStringPair & kvList)
{
	int result = -1;
	std::fstream cfgFile;
	char buffer[256] = { 0 };
	cfgFile.open(szFileName, std::ios::in);
	if (cfgFile.is_open()) {
		while (!cfgFile.eof()) {
			cfgFile.getline(buffer, 256, '\n');
			std::string str = buffer;
			if (str[0] == '#') { //comment line
				continue;
			}
			size_t n = str.find_first_of('=');
			if (n != std::string::npos) {
				std::string keyStr = str.substr(0, n);
				std::string valueStr = str.substr(n + 1);
				kvList.insert(std::make_pair(keyStr, valueStr));
				result = 0;
			}
		}
	}
	cfgFile.close();
	return result;
}

char * readItem(KVStringPair kvList, const char * pItem)
{
	if (!kvList.empty()) {
		if (pItem != "") {
			KVStringPair::iterator iter = kvList.find(pItem);
			if (iter != kvList.end()) {
				std::string strValue = iter->second;
				const char * pValue = strValue.c_str();
				size_t nSize = strlen(pValue);
				if (nSize) {
					char * value = (char *)malloc(nSize + 1);
					if (value) {
						memcpy_s(value, nSize + 1, pValue, nSize);
						value[nSize] = '\0';
						return value;
					}
				}
			}
		}
	}
	return NULL;
}

int main(int argc, char ** argv)
{
	srand((unsigned int)time(NULL));
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 2), &wsaData);
	char szSrvIp[20] = "127.0.0.1";
	unsigned nPort = 22000;
	char szFileName[256] = { 0 };
	GetModuleFileNameA(NULL, szFileName, sizeof(szFileName));
	char szDriver[32] = { 0 };
	char szDir[256] = { 0 };
	_splitpath_s(szFileName, szDriver, sizeof(szDriver), szDir, sizeof(szDir), NULL, 0, NULL, 0);
	char szCfgFile[256] = { 0 };
	sprintf_s(szCfgFile, sizeof(szCfgFile), "%s%sAppSample.cfg", szDriver, szDir);
	KVStringPair kvList;
	if (loadConf(szCfgFile, kvList) == 0) {
		char * pIp = readItem(kvList, "ip");
		char *pPort = readItem(kvList, "port");
		char *pDevice = readItem(kvList, "device");
		char * pUser = readItem(kvList, "user");
		char * pPwd = readItem(kvList, "passwd");
		if (pIp) {
			if (strlen(pIp) > 0) {
				strcpy_s(szSrvIp, sizeof(szSrvIp), pIp);
			}
			free(pIp);
		}
		if (pPort) {
			if (strlen(pPort) > 0) {
				nPort = (unsigned )atoi(pPort);
			}
			free(pPort);
		}
		if (pDevice) {
			if (strlen(pDevice)) {
				strcpy_s(szDeviceId, sizeof(szDeviceId), pDevice);
			}
			free(pDevice);
		}
		if (pUser) {
			if (strlen(pUser)) {
				strcpy_s(szUser, sizeof(szUser), pUser);
			}
			free(pUser);
		}
		if (pPwd) {
			if (strlen(pPwd)) {
				strcpy_s(szPasswd, sizeof(szPasswd), pPwd);
			}
			free(pPwd);
		}
	}
	SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
	if (argc > 1) {
		if (argc == 2) {
			//appname, ip
			if (strlen(argv[1])) {
				strcpy_s(szSrvIp, sizeof(szSrvIp), argv[1]);
			}
		}
		else if (argc == 3) {
			//appname, ip, port
			if (strlen(argv[1])) {
				strcpy_s(szSrvIp, sizeof(szSrvIp), argv[1]);
			}
			if (strlen(argv[2])) {
				nPort = (unsigned )atoi(argv[2]);
			}
		}
		else if (argc == 4) {
			//appname, ip, port, device
			if (strlen(argv[1])) {
				strcpy_s(szSrvIp, sizeof(szSrvIp), argv[1]);
			}
			if (strlen(argv[2])) {
				nPort = (unsigned)atoi(argv[2]);
			}
			if (strlen(argv[3])) {
				strcpy_s(szDeviceId, sizeof(szDeviceId), argv[3]);
			}
		}
	}
	if (strlen(szDeviceId) == 0) {
		strncpy_s(szDeviceId, sizeof(szDeviceId), kDefaultDeviceId, strlen(kDefaultDeviceId));
	} 
	printf("device=%s\n", szDeviceId);
	do {
		if (sock != INVALID_SOCKET) {
			struct sockaddr_in addr;
			addr.sin_family = AF_INET;
			addr.sin_port = htons(nPort);
			inet_pton(AF_INET, szSrvIp, &addr.sin_addr);
			if (connect(sock, (const sockaddr *)&addr, sizeof(addr)) < 0) {
				printf("disconnect\n");
				closesocket(sock);
				break;
			}
			bRunning = true;
			bLogin = false;
			bBind = false;
			bTask = false;
			bConnected = true;
			std::thread recvThd = std::thread(recv_func, sock);
			std::thread sndThd = std::thread(send_func, sock);
			std::thread parseThd = std::thread(parse_func);
			std::thread posThd = std::thread(position_func, sock);
			std::thread aliveThd = std::thread(alive_func, sock);
			while (bRunning) {
				Sleep(500);
			}
			recvThd.join();
			sndThd.join();
			cond4DataQue.notify_one();
			parseThd.join();
			cond4AppPos.notify_one();
			posThd.join();
			aliveThd.join();
			closesocket(sock);
			bConnected = false;
		}
	} while (0);
	printf("stop\n");
	WSACleanup();
	return 0;
}