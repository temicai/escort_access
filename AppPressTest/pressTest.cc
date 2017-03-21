#include <WinSock2.h>
#include <WS2tcpip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <thread>
#include <mutex>
#include <string>
#include <condition_variable>
#include <queue>
#include "document.h"

#pragma comment(lib, "ws2_32.lib")

#define CMD_LOGIN 1
#define CMD_LOGOUT 2
#define CMD_KEEPALIVE 3

//#undef FD_SETSIZE 
//#define FD_SETSIZE 128

#define MAKEHEAD(x) {x.mark[0] = 'E';x.mark[1]='C';x.version[0]='1';x.version[1]='0';}

struct MessageHead
{
	char mark[2];
	char version[2];
	unsigned int uiLen;
};

struct LinkContext
{
	SOCKET sock;
	char szSession[20];
	int nRecvHead;
	int nRecvOffset;
	MessageHead msgHead;
	char szBuf[512];
};

struct BufferData
{
	int nIndex;
	unsigned char * pData;
	unsigned int uiDataLen;
	BufferData()
	{
		nIndex = -1;
		pData = NULL;
		uiDataLen = 0;
	}
};

bool g_bRun = false;
bool g_bAliveFlag = false;
std::mutex g_mutex4Alive;
std::condition_variable g_cond4Alive;
std::mutex g_mutex4DataQue;
std::condition_variable g_cond4DataQue;
std::queue<BufferData *> g_dataQue;
LinkContext g_linkCtxList[50];

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

void addBuffer(BufferData * pBuf)
{
	if (pBuf && pBuf->pData) {
		std::unique_lock<std::mutex> lock(g_mutex4DataQue);
		g_dataQue.push(pBuf);
		if (g_dataQue.size() == 1) {
			g_cond4DataQue.notify_one();
		}
	}
}

void parseBuffer(BufferData * pBuf)
{
	rapidjson::Document doc;
	std::string strData = Utf8ToAnsi((const char *)pBuf->pData);
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
					if (doc.HasMember("session")) {
						if (doc["session"].IsString()) {
							size_t nSize = doc["session"].GetStringLength();
							if (nSize) {
								strncpy_s(g_linkCtxList[pBuf->nIndex].szSession, sizeof(g_linkCtxList[pBuf->nIndex].szSession),
									doc["session"].GetString(), nSize);
								printf("[%d]login session: %s\n", pBuf->nIndex, g_linkCtxList[pBuf->nIndex].szSession);
							}
						}
					}
					{
						std::unique_lock<std::mutex> lock(g_mutex4Alive);
						if (!g_bAliveFlag) {
							g_bAliveFlag = true;
							g_cond4Alive.notify_all();
						}
					}
				}
				else {
					printf("[%d]login failed, retcode=%d\n", pBuf->nIndex, nRet);
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
							printf("[%d]logout session:%s\n", pBuf->nIndex, doc["session"].GetString());
						}
					}
					{
						std::unique_lock<std::mutex> lock(g_mutex4Alive);
						g_bAliveFlag = false;
					}
				}
				else {
					printf("[%d]logout failed, retcode=%d\n", pBuf->nIndex, nRet);
				}
				break;
			}
			case 111: {//alive reply
				if (doc.HasMember("session")) {
					if (doc["session"].IsString()) {
						printf("[%d]alive session=%s\n", pBuf->nIndex, doc["session"].GetString());
					}
				}
				break;
			}
		}
	}
}

void sendMsg(LinkContext linkCtx, int nIndex, int nCmd)
{
	char szDatetime[20] = { 0 };
	time_t now = time(NULL);
	tm tm_now;
	localtime_s(&tm_now, &now);
	sprintf_s(szDatetime, 20, "%04d%02d%02d%02d%02d%02d", tm_now.tm_year + 1900,
		tm_now.tm_mon + 1, tm_now.tm_mday, tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec);

	if (linkCtx.sock != INVALID_SOCKET) {
		if (nCmd == CMD_LOGIN) {
			char szMsg[128] = { 0 };
			sprintf_s(szMsg, 128, "{\"cmd\":1,\"account\":\"yali%d\",\"passwd\":\"123456\",\"datetime\":\"%s\","
				"\"handset\":\"\"}", nIndex, szDatetime);
			std::string strUtf8Msg = AnsiToUtf8(szMsg);
			MessageHead msgHead;
			MAKEHEAD(msgHead);
			size_t nUtf8MsgSize = strUtf8Msg.size();
			msgHead.uiLen = nUtf8MsgSize;
			size_t nHeadSize = sizeof(MessageHead);
			unsigned int uiBufLen = nUtf8MsgSize + nHeadSize;
			unsigned char * pBuf = (unsigned char *)malloc(uiBufLen + 1);
			memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
			memcpy_s(pBuf + nHeadSize, nUtf8MsgSize + 1, strUtf8Msg.c_str(), nUtf8MsgSize);
			pBuf[uiBufLen] = '\0';
			for (unsigned int j = nHeadSize; j < uiBufLen; j++) {
				pBuf[j] += 1;
				pBuf[j] ^= '8';
			}
			send(linkCtx.sock, (const char *)pBuf, uiBufLen, 0);
		}
		else if (nCmd == CMD_LOGOUT) {
			if (strlen(linkCtx.szSession)) {
				char szMsg[128] = { 0 };
				sprintf_s(szMsg, 128, "{\"cmd\":2,\"session\":\"%s\",\"datetime\":\"%s\"}",
					linkCtx.szSession, szDatetime);
				std::string strUtf8Msg = AnsiToUtf8(szMsg);
				MessageHead msgHead;
				MAKEHEAD(msgHead);
				size_t nUtf8MsgSize = strUtf8Msg.size();
				msgHead.uiLen = nUtf8MsgSize;
				size_t nHeadSize = sizeof(MessageHead);
				unsigned int uiBufLen = nUtf8MsgSize + nHeadSize;
				unsigned char * pBuf = (unsigned char *)malloc(uiBufLen + 1);
				memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
				memcpy_s(pBuf + nHeadSize, nUtf8MsgSize + 1, strUtf8Msg.c_str(), nUtf8MsgSize);
				pBuf[uiBufLen] = '\0';
				for (unsigned int j = nHeadSize; j < uiBufLen; j++) {
					pBuf[j] += 1;
					pBuf[j] ^= '8';
				}
				send(linkCtx.sock, (const char *)pBuf, uiBufLen, 0);
			}
		}
		else if (nCmd == CMD_KEEPALIVE) {
			if (strlen(linkCtx.szSession)) {
				char szMsg[128] = { 0 };
				sprintf_s(szMsg, 128, "{\"cmd\":11,\"session\":\"%s\",\"seq\":%d,\"datetime\":\"%s\"}",
					linkCtx.szSession, nIndex, szDatetime);
					std::string strUtf8Msg = AnsiToUtf8(szMsg);
				MessageHead msgHead;
				MAKEHEAD(msgHead);
				size_t nUtf8MsgSize = strUtf8Msg.size();
				msgHead.uiLen = nUtf8MsgSize;
				size_t nHeadSize = sizeof(MessageHead);
				unsigned int uiBufLen = nUtf8MsgSize + nHeadSize;
				unsigned char * pBuf = (unsigned char *)malloc(uiBufLen + 1);
				memcpy_s(pBuf, nHeadSize, &msgHead, nHeadSize);
				memcpy_s(pBuf + nHeadSize, nUtf8MsgSize + 1, strUtf8Msg.c_str(), nUtf8MsgSize);
				pBuf[uiBufLen] = '\0';
				for (unsigned int j = nHeadSize; j < uiBufLen; j++) {
					pBuf[j] += 1;
					pBuf[j] ^= '8';
				}
				send(linkCtx.sock, (const char *)pBuf, uiBufLen, 0);
			}
		}
		
	}
}

void parseFunc()
{
	do {
		std::unique_lock<std::mutex> lock(g_mutex4DataQue);
		g_cond4DataQue.wait(lock, [&] {
			return (!g_dataQue.empty() || !g_bRun);
		});
		if (!g_bRun && g_dataQue.empty()) {
			break;
		}
		BufferData * pBuf = g_dataQue.front();
		g_dataQue.pop();
		if (pBuf) {
			for (unsigned int i = 0; i < pBuf->uiDataLen; i++) {
				pBuf->pData[i] ^= '8';
				pBuf->pData[i] -= 1;
			}
			parseBuffer(pBuf);
			free(pBuf->pData);
			free(pBuf);
		}
	} while (1);
}

void sendFunc(const LinkContext * pLinkList, int nSize)
{
	while (g_bRun) {
		for (int i = 0; i < nSize; i++) {
			sendMsg(pLinkList[i], i, CMD_KEEPALIVE);
		}
		Sleep(3000);
	}
}

void recvFunc(LinkContext * pLinkList, int nSize)
{
	int nMaxSockFd = pLinkList[nSize - 1].sock;
	size_t nHeadSize = sizeof(MessageHead);
	timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	fd_set fdRead;
	while (g_bRun) {
		FD_ZERO(&fdRead);
		for (int i = 0; i < nSize; i++) {
			if (pLinkList[i].sock != INVALID_SOCKET) {
				FD_SET(pLinkList[i].sock, &fdRead);
			}
		}
		int n = select(0, &fdRead, NULL, NULL, &timeout);
		if (n == -1) {
			printf("break:%d\n", WSAGetLastError());
			break;
		}
		else if (n == 0) {
			continue;
		}
		for (int i = 0; i < nSize; i++) {
			int nRecvLen = 0;
			if (FD_ISSET(pLinkList[i].sock, &fdRead)) {
				if (pLinkList[i].nRecvHead == 0) {
					if (pLinkList[i].nRecvOffset == 0) {
						nRecvLen = recv(pLinkList[i].sock, (char *)&pLinkList[i].msgHead, nHeadSize, 0);
						if (nRecvLen == nHeadSize) {
							pLinkList[i].nRecvHead = 1;
							pLinkList[i].nRecvOffset = 0;
						}
						else if (nRecvLen < nHeadSize) {
							pLinkList[i].nRecvOffset = nRecvLen;
						}
						else if (nRecvLen == SOCKET_ERROR) {
							printf("break:%d\n", WSAGetLastError());
							break;
						}
					}
					else {
						nRecvLen = recv(pLinkList[i].sock, (char *)&pLinkList[i].msgHead + pLinkList[i].nRecvOffset,
							nHeadSize - pLinkList[i].nRecvOffset, 0);
						if (nRecvLen + pLinkList[i].nRecvOffset < nHeadSize) {
							pLinkList[i].nRecvOffset += nRecvLen;
						}
						else if (nRecvLen + pLinkList[i].nRecvOffset == nHeadSize) {
							pLinkList->nRecvHead = 1;
							pLinkList->nRecvOffset = 0;
						}
						else if (nRecvLen == SOCKET_ERROR) {
							printf("break:%d\n", WSAGetLastError());
							break;
						}
					}
				}
				else { 
					if (pLinkList[i].nRecvOffset == 0) {
						nRecvLen = recv(pLinkList[i].sock, pLinkList[i].szBuf, pLinkList[i].msgHead.uiLen, 0);
						if (nRecvLen < pLinkList[i].msgHead.uiLen) {
							pLinkList[i].nRecvOffset = nRecvLen;
						}
						else if (nRecvLen == pLinkList[i].msgHead.uiLen) {
							printf("[%d]receive %u data\n", i, pLinkList[i].msgHead.uiLen);
							BufferData * pBufData = (BufferData *)malloc(sizeof(BufferData));
							pBufData->nIndex = i;
							pBufData->uiDataLen = pLinkList[i].msgHead.uiLen;
							pBufData->pData = (unsigned char *)malloc(pBufData->uiDataLen + 1);
							memcpy_s(pBufData->pData, pBufData->uiDataLen + 1, pLinkList[i].szBuf, pBufData->uiDataLen);
							pBufData->pData[pBufData->uiDataLen] = '\0';
							addBuffer(pBufData);
							memset(&pLinkList[i].msgHead, 0, nHeadSize);
							memset(&pLinkList[i].szBuf, 0, sizeof(pLinkList[i].szBuf));
							pLinkList[i].nRecvHead = 0;
							pLinkList[i].nRecvOffset = 0;
						}
						else if (nRecvLen == SOCKET_ERROR) {
							printf("break:%d\n", WSAGetLastError());
							break;
						}
					}
					else {
						nRecvLen = recv(pLinkList[i].sock, pLinkList[i].szBuf + pLinkList[i].nRecvOffset,
							pLinkList[i].msgHead.uiLen - pLinkList[i].nRecvOffset, 0);
						if (nRecvLen < pLinkList[i].msgHead.uiLen - pLinkList[i].nRecvOffset) {
							pLinkList[i].nRecvOffset += nRecvLen;
						}
						else if (nRecvLen == pLinkList[i].msgHead.uiLen - pLinkList[i].nRecvOffset) {
							printf("[%d]receive %u data\n", i, pLinkList[i].msgHead.uiLen);
							BufferData * pBufData = (BufferData *)malloc(sizeof(BufferData));
							pBufData->nIndex = i;
							pBufData->uiDataLen = pLinkList[i].msgHead.uiLen;
							pBufData->pData = (unsigned char *)malloc(pBufData->uiDataLen + 1);
							memcpy_s(pBufData->pData, pBufData->uiDataLen + 1, pLinkList[i].szBuf, pBufData->uiDataLen);
							pBufData->pData[pBufData->uiDataLen] = '\0';
							addBuffer(pBufData);
							memset(&pLinkList[i].msgHead, 0, nHeadSize);
							memset(&pLinkList[i].szBuf, 0, sizeof(pLinkList[i].szBuf));
							pLinkList[i].nRecvHead = 0;
							pLinkList[i].nRecvOffset = 0;
						}
						else if (nRecvLen == SOCKET_ERROR) {
							printf("break:%d\n", WSAGetLastError());
							break;
						}
					}
				}
			}
		}
	}
}

int main(int argc, char ** argv)
{
	char szHost[20] = { 0 };
	if (argc >= 2) {
		sprintf_s(szHost, 20, argv[1]);
	}
	else {
		sprintf_s(szHost, 20, "127.0.0.1");
	}
	srand((unsigned int)time(NULL));
	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 2), &wsaData);
	g_bAliveFlag = false;
	//SOCKET sockList[100];
	for (int i = 0; i < 50; i++) {
		g_linkCtxList[i].sock = socket(AF_INET, SOCK_STREAM, 0);
		g_linkCtxList[i].szSession[0] = '\0';
		g_linkCtxList[i].nRecvHead = 0;
		g_linkCtxList[i].nRecvOffset = 0;
		memset(&g_linkCtxList[i].msgHead, 0, sizeof(MessageHead));
		memset(g_linkCtxList[i].szBuf, 0, sizeof(g_linkCtxList[i].szBuf));
		struct sockaddr_in addr;
		addr.sin_family = AF_INET;
		addr.sin_port = htons(22000);
		inet_pton(AF_INET, szHost, &addr.sin_addr);
		//inet_pton(AF_INET, "112.74.196.90", &addr.sin_addr);
		if (connect(g_linkCtxList[i].sock, (const sockaddr *)&addr, sizeof(addr)) < 0) {
			printf("disconnect\n");
			closesocket(g_linkCtxList[i].sock);
			g_linkCtxList[i].sock = INVALID_SOCKET;
			break;
		}
		sendMsg(g_linkCtxList[i], i, CMD_LOGIN);
	}
	g_bRun = true;
	std::thread t1(sendFunc, g_linkCtxList, 50);
	std::thread t2(recvFunc, g_linkCtxList, 50);
	std::thread t3(parseFunc);
	while (g_bRun) {
		Sleep(1000);
	}
	t1.join();
	t2.join();
	t3.join();
	for (int i = 0; i < 50; i++) {
		sendMsg(g_linkCtxList[i], i, CMD_LOGOUT);
	}
	WSACleanup();
	return 0;
}