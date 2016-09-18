#ifndef ESCORT_ACCESS_42124C0D_51E9_4C98_AD0A_55100A459272_H
#define ESCORT_ACCESS_42124C0D_51E9_4C98_AD0A_55100A459272_H

#ifdef __cplusplus
extern "C"
{
#endif 

#ifdef DLL_IMPORT
#define EAAPI __declspec(dllimport)
#else 
#define EAAPI __declspec(dllexport)
#endif

	EAAPI unsigned int __stdcall EA_Start(const char * pZkHost, unsigned short usAccessPort);
	EAAPI int __stdcall EA_Stop(unsigned int);
	EAAPI int __stdcall EA_SetLogType(unsigned int, int);

#ifdef __cplusplus
}
#endif

#endif