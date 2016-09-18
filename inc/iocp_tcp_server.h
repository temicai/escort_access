#ifndef IOCP_TCP_SERVER_CDEB7879_85C4_4256_8438_AC752950BC52_H
#define IOCP_TCP_SERVER_CDEB7879_85C4_4256_8438_AC752950BC52_H

#include "iocp_common_define.h"

extern "C"
{
#ifndef SERV_API 
#define SERV_API extern "C" __declspec(dllexport)
#endif
}

#ifndef NULL
#define NULL 0
#endif

extern "C"
{
	SERV_API unsigned int __stdcall TS_StartServer(unsigned int uiPort, int nLogType, fMessageCallback fMsgCb, 
		void * pUserData);
	SERV_API int __stdcall TS_StopServer(unsigned int uiServerInst);
	SERV_API int __stdcall TS_SetLogType(unsigned int uiServerInst, int nLogType);
	SERV_API int __stdcall TS_SendData(unsigned int uiServerInst, const char * szEndPoit, const char * pData,
		unsigned long ulDataLen);
	SERV_API int __stdcall TS_GetPort(unsigned int uiServerInst, unsigned int & uiPort);
	SERV_API int __stdcall TS_SetMessageCallback(unsigned int uiServerInst, fMessageCallback fMsgCb, void * pUserData);
}

#endif
