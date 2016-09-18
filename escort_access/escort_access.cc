#include "escort_access.h"
#include "access_concrete.h"
#include <Windows.h>
#include <map>

static char g_szDllDir[256] = { 0 };
std::map<unsigned int, AccessService *> g_instList;
pthread_mutex_t g_mutex4InstList;

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
						delete pService;
						pService = NULL;
					}
					iter = g_instList.erase(iter);
				} while (iter != g_instList.end());
				g_instList.clear();
			}
			pthread_mutex_unlock(&g_mutex4InstList);
			pthread_mutex_destroy(&g_mutex4InstList);
			break;
		}
	}
	return TRUE;
}

EAAPI unsigned int __stdcall EA_Start(const char * pZkHost, unsigned short usAccessPort)
{
	AccessService * pService = new AccessService(pZkHost, g_szDllDir);
	if (pService) {
		if (pService->StartAccessService(usAccessPort) == 0) {
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
			result = pService->StopAccessService();
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
			pService->SetLogType(nLogType);
			result = 0;
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}