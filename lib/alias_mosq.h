/*
Copyright (c) 2019-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
*/

#ifndef ALIAS_MOSQ_H
#define ALIAS_MOSQ_H

#include "mosquitto_internal.h"

int alias__add_r2l(struct mosquitto *mosq, const char *topic, uint16_t alias);
int alias__add_l2r(struct mosquitto *mosq, const char *topic, uint16_t *alias);
int alias__find_by_alias(struct mosquitto *mosq, int direction, uint16_t alias, char **topic);
int alias__find_by_topic(struct mosquitto *mosq, int direction, const char *topic, uint16_t *alias);
void alias__free_all(struct mosquitto *mosq);

#endif
