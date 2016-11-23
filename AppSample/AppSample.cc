#include <WinSock2.h>
#include <WS2tcpip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mutex>
#include <thread>
#include <queue>
#include <condition_variable>
#include <time.h>
#include "rapidjson\document.h"

#pragma comment(lib, "ws2_32.lib")

const char * kDefaultDeviceId = "3917677394";

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

bool bRunning = false;
std::mutex mutex4DataQue;
std::condition_variable cond4DataQue;
std::mutex mutex4AppPos;
std::condition_variable cond4AppPos;
std::queue<RecvData *> dataQue;
char szSession[20] = { 0 };
char szTask[12] = { 0 };

#define MAKEHEAD(x) {x.mark[0] = 'E';x.mark[1]='C';x.version[0]='1';x.version[1]='0';}

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
		"'h' or 'H': help menu\n"
		"'q' or 'Q': quit\n");
}

void add(RecvData *);

void recv_func(SOCKET sock)
{
	timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	fd_set fdRead;
	char szRecv[512] = { 0 };
	bool bRecvHead = false;
	MessageHead msgHead;
	size_t nHeadSize = sizeof(MessageHead);
	unsigned int nOffset = 0;
	unsigned int nRecvLen = 0;
	unsigned int nDataLen = 0;
	while (bRunning) {
		FD_ZERO(&fdRead);
		FD_SET(sock, &fdRead);
		int n = select(sock + 1, &fdRead, NULL, NULL, &timeout);
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
					nRecvLen = recv(sock, (char *)&msgHead, nHeadSize, 0);
					if (nRecvLen == nHeadSize) {
						bRecvHead = true;
						nDataLen = msgHead.uiLen;
					}
					else if (nRecvLen < nHeadSize) {
						nOffset = nRecvLen;
					}
					else if (nRecvLen == SOCKET_ERROR) {
						printf("[RECEIVE]recv error:%d\n", WSAGetLastError());
						break;
					}
				}
				else {
					nRecvLen = recv(sock, (char *)&msgHead + nOffset, nHeadSize - nOffset, 0);
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
					nRecvLen = recv(sock, szRecv, nDataLen, 0);
					if (nRecvLen < nDataLen) {
						nOffset = nRecvLen;
					}
					else if (nRecvLen == nDataLen) {
						printf("[RECEIVE]recv %d data\n", nDataLen);
						RecvData * pRecvData = (RecvData *)malloc(sizeof(RecvData));
						pRecvData->uiDataLen = nDataLen;
						pRecvData->pData = (unsigned char *)malloc(nDataLen + 1);
						memcpy_s(pRecvData->pData, nDataLen, szRecv, nDataLen);
						pRecvData->pData[nDataLen] = '\0';
						add(pRecvData);
						bRecvHead = false;
						memset(&msgHead, 0, nHeadSize);
					}
					else if (nRecvLen == SOCKET_ERROR) {
						printf("[RECEIVE]recv error:%d\n", WSAGetLastError());
						break;
					}
				}
				else {
					nRecvLen = recv(sock, szRecv + nOffset, nDataLen - nOffset, 0);
					if (nRecvLen + nOffset == nDataLen) {
						printf("[RECEIVE]recv %d data\n", nDataLen);
						RecvData * pRecvData = (RecvData *)malloc(sizeof(RecvData));
						pRecvData->uiDataLen = nDataLen;
						pRecvData->pData = (unsigned char *)malloc(nDataLen + 1);
						memcpy_s(pRecvData->pData, nDataLen, szRecv, nDataLen);
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
						break;
					}
				}
			}
		}
	}
}

void sendMsg(SOCKET sock, const char * pMsg, size_t nMsgSize)
{
	if (pMsg && nMsgSize && sock > 0) {
		MessageHead msgHead;
		MAKEHEAD(msgHead);
		msgHead.uiLen = nMsgSize;
		size_t nHeadSize = sizeof(MessageHead);
		unsigned int nBufLen = nMsgSize + nHeadSize;
		unsigned char * pBuf = (unsigned char *)malloc(nBufLen + 1);
		memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
		memcpy_s(pBuf + nHeadSize, nMsgSize, pMsg, nMsgSize);
		for (unsigned int i = nHeadSize; i < nBufLen; i++) {
			pBuf[i] += 1;
			pBuf[i] ^= '8';
		}
		send(sock, (const char *)pBuf, nBufLen, 0);
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
		char szDatetime[16] = { 0 };
		format_datetime((unsigned long)time(NULL), szDatetime, sizeof(szDatetime));
		char c;
		scanf_s("%c", &c, 1);
		if (c == 'i' || c == 'I') { //test || test
			if (!bLogin) {
				char szMsg[256] = { 0 };
				//snprintf(szMsg, sizeof(szMsg), "{\"cmd\":1,\"account\":\"test\",\"passwd\":\"test123\""
				//	",\"datetime\":\"%s\"}", szDatetime);
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":1,\"account\":\"test2\",\"passwd\":\"3cf2bc71982"
					"179c0d0944dee43fb23d2\",\"datetime\":\"%s\"}", szDatetime);
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
			if (bLogin && !bBind) {
				char szMsg[256] = { 0 };
				snprintf(szMsg, sizeof(szMsg), "{\"cmd\":3,\"session\":\"%s\",\"deviceId\":\"%s\","
					"\"datetime\":\"%s\"}", szSession, kDefaultDeviceId, szDatetime);
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
					"\"datetime\":\"%s\"}", szSession, kDefaultDeviceId, szDatetime);
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
					"\"destination\":\"destination1\",\"target\":\"aaaabbbbcc&Áø·Ç\",\"datetime\":\"%s\"}", 
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
		Sleep(200);
	}
}

void parse(RecvData * pRecvData)
{
	rapidjson::Document doc;
	if (!doc.Parse((const char *)pRecvData->pData).HasParseError()) {
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
								if (doc["taskInfo"][0].HasMember("battery")) {
									if (doc["taskInfo"][0]["battery"].IsInt()) {
										printf("[PARSE]login task battery:%d\n", doc["taskInfo"][0]["battery"].GetInt());
									}
								}
							}
						}
					}
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
							printf("[PARSE]close task session:%s\n", doc["session"].GetString());
						}
					}
					if (doc.HasMember("taskId")) {
						if (doc["taskId"].IsString()) {
							printf("[PARSE]close task:%s\n", doc["taskId"].GetString());
						}
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
				if (doc.HasMember("datetime")) {
					if (doc["datetime"].IsString()) {
						printf("[PARSE]notice datetime:%s\n", doc["datetime"].GetString());
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
			default: break;
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
			printf("[PARSE]%s\n", (char *)pRecvData->pData);
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

int main()
{
	srand((unsigned int)time(NULL));
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2,2), &wsaData);
	printf("start\n");
	SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
	do {
		if (sock != INVALID_SOCKET) {
			struct sockaddr_in addr;
			addr.sin_family = AF_INET;
			addr.sin_port = htons(22000);
			inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
			if (connect(sock, (const sockaddr *)&addr, sizeof(addr)) < 0) {
				printf("disconnect\n");
				closesocket(sock);
				break;
			}
			bRunning = true;
			bLogin = false;
			bBind = false;
			bTask = false;
			std::thread recvThd = std::thread(recv_func, sock);
			std::thread sndThd = std::thread(send_func, sock);
			std::thread parseThd = std::thread(parse_func);
			std::thread posThd = std::thread(position_func, sock);
			while (bRunning) {
				Sleep(500);
			}
			recvThd.join();
			sndThd.join();
			cond4DataQue.notify_one();
			parseThd.join();
			cond4AppPos.notify_one();
			posThd.join();
			closesocket(sock);
		}
	} while (0);
	printf("stop\n");
	WSACleanup();
	return 0;
}