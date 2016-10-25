#include "escort_access.h"
#include "access_concrete.h"
#include <Windows.h>
#include <map>
#include <fstream>
#include <string>

typedef std::map<std::string, std::string> KVStringPair;

static char g_szDllDir[256] = { 0 };
std::map<unsigned int, AccessService *> g_instList;
pthread_mutex_t g_mutex4InstList;

int loadConf(const char * szFileName, KVStringPair & kvList);
char * readItem(KVStringPair kvList, const char * pItem);

BOOL APIENTRY DllMain(void * hInst, unsigned long ulReason, void * pReserved)
{
	switch (ulReason) {
		case DLL_PROCESS_ATTACH: {
			pthread_mutex_init(&g_mutex4InstList, NULL);
			g_instList.clear();
			g_szDllDir[0] = '\0';
			char szPath[256] = { 0 };
			if (GetModuleFileNameA((HMODULE)hInst, szPath, sizeof szPath) != 0) {
				char drive[32] = { 0 };
				char dir[256] = { 0 };
				char fname[256] = { 0 };
				char ext[32] = { 0 };
				_splitpath_s(szPath, drive, 32, dir, 256, fname, 256, ext, 32);
				snprintf(g_szDllDir, sizeof(g_szDllDir), "%s%s", drive, dir);
			}
			break;
		}
		case DLL_PROCESS_DETACH: {
			pthread_mutex_lock(&g_mutex4InstList);
			if (!g_instList.empty()) {
				std::map<unsigned int, AccessService *>::iterator iter;
				iter = g_instList.begin();
				do {
					AccessService * pService = iter->second;
					if (pService) {
						if (pService->GetStatus()) {
							pService->StopAccessService();
						}
						delete pService;
						pService = NULL;
					}
					iter = g_instList.erase(iter);
				} while (iter != g_instList.end());
			}
			pthread_mutex_unlock(&g_mutex4InstList);
			pthread_mutex_destroy(&g_mutex4InstList);
			break;
		}
	}
	return TRUE;
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

EAAPI unsigned int __stdcall EA_Start(const char * pCfgFileName)
{
	if (strlen(g_szDllDir)) {
		char szFileName[256] = { 0 };
		if (pCfgFileName && strlen(pCfgFileName)) {
			strncpy_s(szFileName, sizeof(szFileName), pCfgFileName, strlen(pCfgFileName));
		}
		else {
			snprintf(szFileName, sizeof(szFileName), "%sconf\\server.data", g_szDllDir);
		}
		KVStringPair kvList;
		if (loadConf(szFileName, kvList) == 0) {
			char * pZkHost = readItem(kvList, "zk_host");
			char * pAccessHost = readItem(kvList, "access_ip");
			char * pAccessPort = readItem(kvList, "access_port");
			char * pMidwareHost = readItem(kvList, "midware_ip");
			char * pMidwarePubPort = readItem(kvList, "publish_port");
			char * pMidwareTalkPort = readItem(kvList, "talk_port");
			char * pDbProxyHost = readItem(kvList, "db_proxy_ip");
			char * pDbProxyQryPort = readItem(kvList, "query_port");
			char szZkHost[256] = { 0 };
			char szAccessHost[32] = { 0 };
			unsigned short usAccessPort = 0;
			char szMidwareHost[32] = { 0 };
			unsigned short usMidwarePubPort = 0;
			unsigned short usMidwareTalkPort = 0;
			char szDbProxyHost[32] = { 0 };
			unsigned short usDbProxyQryPort = 0;
			if (pZkHost) {
				strncpy_s(szZkHost, sizeof(szZkHost), pZkHost, strlen(pZkHost));
				free(pZkHost);
			}
			if (pAccessHost) {
				strncpy_s(szAccessHost, sizeof(szAccessHost), pAccessHost, strlen(pAccessHost));
				free(pAccessHost);
			}
			if (pAccessPort) {
				usAccessPort = (unsigned short)atoi(pAccessPort);
				free(pAccessPort);
			}
			if (pMidwareHost) {
				strncpy_s(szMidwareHost, sizeof(szMidwareHost), pMidwareHost, strlen(pMidwareHost));
				free(pMidwareHost);
			}
			if (pMidwarePubPort) {
				usMidwarePubPort = (unsigned short)atoi(pMidwarePubPort);
				free(pMidwarePubPort);
			}
			if (pMidwareTalkPort) {
				usMidwareTalkPort = (unsigned short)atoi(pMidwareTalkPort);
				free(pMidwareTalkPort);
			}
			if (pDbProxyHost) {
				strncpy_s(szDbProxyHost, sizeof(szDbProxyHost), pDbProxyHost, strlen(pDbProxyHost));
				free(pDbProxyHost);
			}
			if (pDbProxyQryPort) {
				usDbProxyQryPort = (unsigned short)atoi(pDbProxyQryPort);
				free(pDbProxyQryPort);
			}
			AccessService * pService = new AccessService(szZkHost, g_szDllDir);
			if (pService) {
				if (pService->StartAccessService(szAccessHost, usAccessPort, szMidwareHost, usMidwarePubPort, 
					usMidwareTalkPort, szDbProxyHost, usDbProxyQryPort) == 0) {
					unsigned int uiVal = (unsigned int)pService;
					pthread_mutex_lock(&g_mutex4InstList);
					g_instList.insert(std::make_pair(uiVal, pService));
					pthread_mutex_unlock(&g_mutex4InstList);
					return uiVal;
				}
				else {
					delete pService;
					pService = NULL;
				}
			}
		}
	}
	return 0;
}

EAAPI int __stdcall EA_Stop(unsigned int uiInst)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4InstList);
	if (!g_instList.empty()) {
		std::map<unsigned int, AccessService *>::iterator iter = g_instList.find(uiInst);
		if (iter != g_instList.end()) {
			AccessService * pService = iter->second;
			if (pService) {
				result = pService->StopAccessService();
				delete pService;
				pService = NULL;
			}
			g_instList.erase(iter);
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}

EAAPI int __stdcall EA_SetLogType(unsigned int uiInst, int nLogType)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4InstList);
	if (!g_instList.empty()) {
		std::map<unsigned int, AccessService *>::iterator iter = g_instList.find(uiInst);
		if (iter != g_instList.end()) {
			AccessService * pService = iter->second;
			if (pService) {
				pService->SetLogType(nLogType);
				result = 0;
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}

EAAPI int __stdcall EA_GetStatus(unsigned int uiInst)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4InstList);
	if (!g_instList.empty()) {
		std::map<unsigned int, AccessService *>::iterator iter = g_instList.find(uiInst);
		if (iter != g_instList.end()) {
			AccessService * pService = iter->second;
			result = pService->GetStatus();
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}

