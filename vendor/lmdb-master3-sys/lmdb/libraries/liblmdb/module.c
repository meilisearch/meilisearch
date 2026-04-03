/* module.c - helper for dynamically loading crypto module */
/*
 * Copyright 2020-2021 Howard Chu, Symas Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the Symas
 * Dual-Use License.
 *
 * A copy of this license is available in the file LICENSE in the
 * source distribution.
 */
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

#include <stddef.h>
#include <string.h>

#include "lmdb.h"

void *
mdb_modload(const char *file, const char *name, MDB_crypto_funcs **mcf_ptr, char **errmsg)
{
	MDB_crypto_hooks *hookfunc;
	void *ret = NULL;
	if (!name)
		name = "MDB_crypto";

#ifdef _WIN32
	{
		HINSTANCE mlm = LoadLibrary(file);
		if (mlm) {
			hookfunc = GetProcAddress(mlm, name);
			if (hookfunc)
				*mcf_ptr = hookfunc();
			else {
				*errmsg = "Crypto hook function not found";
				FreeLibrary(mlm);
				mlm = NULL;
			}
		} else {
			*errmsg = GetLastError();
		}
		ret = (void *)mlm;
	}
#else
	{
		void *mlm = dlopen(file, RTLD_NOW);
		if (mlm) {
			hookfunc = dlsym(mlm, name);
			if (hookfunc)
				*mcf_ptr = hookfunc();
			else {
				*errmsg = "Crypto hook function not found";
				dlclose(mlm);
				mlm = NULL;
			}
		} else {
			*errmsg = dlerror();
		}
		ret = mlm;
	}
#endif
	return ret;
}

void
mdb_modunload(void *mlm)
{
#ifdef _WIN32
	FreeLibrary((HINSTANCE)mlm);
#else
	dlclose(mlm);
#endif
}

void
mdb_modsetup(MDB_env *env, MDB_crypto_funcs *cf, const char *password)
{
	MDB_val enckey = {0};
	if (cf->mcf_sumfunc) {
		mdb_env_set_checksum(env, cf->mcf_sumfunc, cf->mcf_sumsize);
	}
	if (cf->mcf_encfunc && password) {
		char keybuf[2048];
		enckey.mv_data = keybuf;
		enckey.mv_size = cf->mcf_keysize;
		if (cf->mcf_str2key)
			cf->mcf_str2key(password, &enckey);
		else
			strncpy(enckey.mv_data, password, enckey.mv_size);
		mdb_env_set_encrypt(env, cf->mcf_encfunc, &enckey, cf->mcf_esumsize);
		memset(enckey.mv_data, 0, enckey.mv_size);
	}
}
