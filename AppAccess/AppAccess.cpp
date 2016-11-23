#include <stdio.h>
#include "escort_access.h"
#include <Windows.h>
#include <string>


//typedef std::map<std::string, std::string> KVStringPair;
//void loadConf(const char * szFileName, KVStringPair & kvList); 
//char * readItem(KVStringPair kvList, const char * pItemKey);

int main(int argc, char ** argv)
{
	char szCfgFile[256] = { 0 };
	unsigned int uiInst = 0;
	if (argc == 3 && (strcmp(argv[1], "-l") == 0)) {
		snprintf(szCfgFile, sizeof(szCfgFile), "%s", argv[2]);
	}
	if (strlen(szCfgFile)) {
		uiInst = EA_Start();
	}
	else {
		uiInst = EA_Start(szCfgFile);
	}
	DWORD dwProcessId = GetCurrentProcessId();
	printf("PID=%lu, escort access service Instance=%u\n", dwProcessId, uiInst);
	if (uiInst) {
		getchar();
		EA_Stop(uiInst);
		printf("end\n");
	}
	else {
		printf("start access server error\n");
	}
	return 0;
}

//void loadConf(const char * szConfFile_, KVStringPair & kvList_)
//{
//	std::fstream cfgFile;
//	char buffer[256] = { 0 };
//	cfgFile.open(szConfFile_, std::ios::in);
//	if (cfgFile.is_open()) {
//		while (!cfgFile.eof()) {
//			cfgFile.getline(buffer, 256, '\n');
//			std::string str = buffer;
//			if (str[0] == '#') { //×¢ÊÍÐÐ
//				continue;
//			}
//			size_t n = str.find_first_of('=');
//			if (n != std::string::npos) {
//				std::string keyStr = str.substr(0, n);
//				std::string valueStr = str.substr(n + 1);
//				kvList_.insert(std::make_pair(keyStr, valueStr));
//			}	
//		}
//	}
//	cfgFile.close();
//}
//
//char * readItem(KVStringPair kvList_, const char * pItemKey_)
//{
//	if (!kvList_.empty()) {
//		if (pItemKey_ != "") {
//			KVStringPair::iterator iter = kvList_.find(pItemKey_);
//			if (iter != kvList_.end()) {
//				std::string strValue = iter->second;
//				const char * pValue = strValue.c_str();
//				size_t nSize = strlen(pValue);
//				if (nSize) {
//					char * value = (char *)malloc(nSize + 1);
//					strncpy_s(value, nSize + 1, pValue, nSize);
//					return value;
//				}
//			}
//		}
//	}
//	return NULL;
//}