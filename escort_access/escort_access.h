#ifndef ESCORT_ACCESS_42124C0D_51E9_4C98_AD0A_55100A459272_H
#define ESCORT_ACCESS_42124C0D_51E9_4C98_AD0A_55100A459272_H

#ifdef __cplusplus
extern "C"
{
#endif 
	unsigned long long __stdcall EA_Start(const char * pCfgFileName = 0);
	int __stdcall EA_Stop(unsigned long long);
	int __stdcall EA_SetLogType(unsigned long long, unsigned short);
	int __stdcall EA_GetStatus(unsigned long long);

#ifdef __cplusplus
}
#endif

#endif