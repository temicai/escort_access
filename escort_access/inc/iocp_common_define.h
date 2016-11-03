#ifndef IOCP_COMMON_DEFINE_705262E6_9110_4BCC_A9A7_DA50E33432DC_H
#define IOCP_COMMON_DEFINE_705262E6_9110_4BCC_A9A7_DA50E33432DC_H

#include <sys/types.h>
#include <stdlib.h>

#define MSG_LINK_CONNECT  0
#define MSG_DATA 1
#define MSG_LINK_DISCONNECT 2 

typedef struct tagMessageContent
{
	char szEndPoint[32];				//消息来源
	unsigned char * pMsgData;		//消息体
	unsigned long ulMsgDataLen;	//消息长度
	unsigned long ulMsgTime;		//消息时间,精确到秒
	tagMessageContent()
	{
		szEndPoint[0] = '\0';
		pMsgData = 0;
		ulMsgDataLen = 0;
		ulMsgTime = 0;
	}
	~tagMessageContent()
	{
		if (pMsgData && ulMsgDataLen > 0) {
			free(pMsgData);
			pMsgData = NULL;
			ulMsgDataLen = 0;
		}
	}
} MessageContent;

typedef void(__stdcall *fMessageCallback)(int, void *, void *);

#endif 
