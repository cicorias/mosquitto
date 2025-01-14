R=../..
include ${R}/config.mk

.PHONY : all binary check clean reallyclean test test-compile install uninstall

PLUGIN_NAME=mosquitto_dynamic_security
LOCAL_CPPFLAGS=-I${R}/lib/ -I${R}/src/ -I${R}/plugins/common -DWITH_CJSON -DWITH_TLS
ifeq ($(WITH_BUNDLED_DEPS),yes)
	LOCAL_CPPFLAGS:=$(LOCAL_CPPFLAGS) -I${R}/deps
endif

OBJS=	\
		acl.o \
		auth.o \
		base64_mosq.o \
		clients.o \
		clientlist.o \
		config.o \
		config_init.o \
		control.o \
		control_common.o \
		default_acl.o \
		groups.o \
		grouplist.o \
		hash.o \
		json_help.o \
		kicklist.o \
		misc_mosq.o \
		password_mosq.o \
		plugin.o \
		roles.o \
		rolelist.o \
		tick.o

ifeq ($(WITH_CJSON),yes)
ifeq ($(WITH_TLS),yes)
ALL_DEPS:= binary
else
ALL_DEPS:=
endif
else
ALL_DEPS:=
endif

all : ${ALL_DEPS}
binary : ${PLUGIN_NAME}.so

${PLUGIN_NAME}.so : ${OBJS}
	${CROSS_COMPILE}${CC} $(PLUGIN_LDFLAGS) -fPIC -shared $^ -o $@ -lcjson

acl.o : acl.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

auth.o : auth.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

base64_mosq.o : ${R}/common/base64_mosq.c ${R}/common/base64_mosq.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

clients.o : clients.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

clientlist.o : clientlist.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

config.o : config.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

config_init.o : config_init.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

control.o : control.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

default_acl.o : default_acl.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

groups.o : groups.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

grouplist.o : grouplist.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

hash.o : hash.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

json_help.o : ${R}/common/json_help.c ${R}/common/json_help.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

kicklist.o : kicklist.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

misc_mosq.o : ${R}/common/misc_mosq.c ${R}/common/misc_mosq.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

password_mosq.o : ${R}/common/password_mosq.c ${R}/common/password_mosq.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

plugin.o : plugin.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

control_common.o : ${R}/common/control_common.c ${R}/common/control_common.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

roles.o : roles.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

rolelist.o : rolelist.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

tick.o : tick.c dynamic_security.h
	${CROSS_COMPILE}${CC} $(LOCAL_CPPFLAGS) $(PLUGIN_CPPFLAGS) $(PLUGIN_CFLAGS) -c $< -o $@

reallyclean : clean
clean:
	-rm -f *.o ${PLUGIN_NAME}.so *.gcda *.gcno

test-compile:

check: test
test: test-compile

install: all
ifeq ($(WITH_CJSON),yes)
ifeq ($(WITH_TLS),yes)
	$(INSTALL) -d "${DESTDIR}$(libdir)"
	$(INSTALL) ${STRIP_OPTS} ${PLUGIN_NAME}.so "${DESTDIR}${libdir}/${PLUGIN_NAME}.so"
endif
endif

uninstall :
	-rm -f "${DESTDIR}${libdir}/${PLUGIN_NAME}.so"
