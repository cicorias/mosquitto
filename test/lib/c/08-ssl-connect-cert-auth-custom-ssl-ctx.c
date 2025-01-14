#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <mosquitto.h>
#include <openssl/ssl.h>
#include "path_helper.h"

static int run = -1;

void handle_sigint(int signal)
{
	(void)signal;

	run = 0;
}

void on_connect(struct mosquitto *mosq, void *obj, int rc)
{
	(void)obj;

	if(rc){
		exit(1);
	}else{
		mosquitto_disconnect(mosq);
	}
}

void on_disconnect(struct mosquitto *mosq, void *obj, int rc)
{
	(void)mosq;
	(void)obj;
	run = rc;
}

int main(int argc, char *argv[])
{
	struct mosquitto *mosq;
	SSL_CTX *ssl_ctx;
	assert(argc == 2);
	int port = atoi(argv[1]);

	mosquitto_lib_init();

	OPENSSL_init_crypto(OPENSSL_INIT_ADD_ALL_CIPHERS \
			| OPENSSL_INIT_ADD_ALL_DIGESTS \
			| OPENSSL_INIT_LOAD_CONFIG, NULL);
	ssl_ctx = SSL_CTX_new(TLS_client_method());

	char cafile[4096];
	cat_sourcedir_with_relpath(cafile, "/../../ssl/test-root-ca.crt");
	char capath[4096];
	cat_sourcedir_with_relpath(capath, "/../../ssl/certs");
	char certfile[4096];
	cat_sourcedir_with_relpath(certfile, "/../../ssl/client.crt");
	char keyfile[4096];
	cat_sourcedir_with_relpath(keyfile, "/../../ssl/client.key");

	SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);
	SSL_CTX_use_certificate_chain_file(ssl_ctx, certfile);
	SSL_CTX_use_PrivateKey_file(ssl_ctx, keyfile, SSL_FILETYPE_PEM);
	SSL_CTX_load_verify_locations(ssl_ctx, cafile, capath);

	mosq = mosquitto_new("08-ssl-connect-crt-auth", true, NULL);
	if(mosq == NULL){
		return 1;
	}
	mosquitto_tls_set(mosq, "../ssl/test-root-ca.crt", "../ssl/certs", "../ssl/client.crt", "../ssl/client.key", NULL);
	mosquitto_connect_callback_set(mosq, on_connect);
	mosquitto_disconnect_callback_set(mosq, on_disconnect);

	mosquitto_int_option(mosq, MOSQ_OPT_SSL_CTX_WITH_DEFAULTS, 0);
	mosquitto_void_option(mosq, MOSQ_OPT_SSL_CTX, ssl_ctx);

	mosquitto_connect(mosq, "localhost", port, 60);

	signal(SIGINT, handle_sigint);
	while(run == -1){
		mosquitto_loop(mosq, -1, 1);
	}
	SSL_CTX_free(ssl_ctx);
	mosquitto_destroy(mosq);

	mosquitto_lib_cleanup();
	return run;
}
