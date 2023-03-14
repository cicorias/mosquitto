/*
Copyright (c) 2020-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Abilio Marques - initial implementation and documentation.
*/

/*
 * This is an *example* plugin which demonstrates how to jail a client.
 * It mounts such client topics in a subtree starting with the client id.
 * It modifies it's subscriptions and the topics of the messages destined to
 * that client. It also modifies the topics of the messages published
 * by the client.
 *
 *  client | event | destination | original topic       | modified topic
 *  -------|-------|-------------|----------------------|--------------------
 *  jailed |  sub  |     ---     | topic                | ${jailed_id}/topic
 *  normal |  pub  |   jailed    | ${jailed_id}/topic   | topic
 *  jailed |  pub  |   normal    | topic                | ${jailed_id}/topic
 *
 * For simplicity of this example, all clients with id starting with "jailed"
 * will be jailed. All other clients will work as normal.
 *
 * Two jailed clients cannot interact with each other. Normal clients can interact
 * with any jailed client by publishing or subscribing to the mounted topic.
 *
 * You should be very sure of what you are doing before making use of this feature.
 *
 * Compile with:
 *   gcc -I<path to mosquitto-repo/include> -fPIC -shared mosquitto_topic_jail.c -o mosquitto_topic_jail.so
 *
 * Use in config with:
 *
 *   plugin /path/to/mosquitto_topic_jail.so
 *
 * Note that this only works on Mosquitto 2.0 or later.
 */
#include <stdio.h>
#include <string.h>

#include "mosquitto_broker.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"
#include "mqtt_protocol.h"

#define PLUGIN_NAME "topic-jail-all"
#define PLUGIN_VERSION "1.0"

#define UNUSED(A) (void)(A)

#define SLASH_Z sizeof((char)'/')

MOSQUITTO_PLUGIN_DECLARE_VERSION(5);

static mosquitto_plugin_id_t* mosq_pid = NULL;

// #pragma GCC push_options
// #pragma GCC optimize("O0")
static bool is_jailed(const char* str)
{
	return !(strncmp("admin", str, 5) == 0);
}

static int callback_acl_check(int event, void* event_data, void* userdata)
{
	struct mosquitto_evt_acl_check* ed = event_data;

	UNUSED(event);
	UNUSED(userdata);

	const char* clientid = mosquitto_client_id(ed->client);

	if (!is_jailed(clientid))
	{
	/* will only modify the topic of jailed clients */
		return MOSQ_ERR_SUCCESS;
	}

	char* putTopic = "$dps/registrations/PUT/iotdps-register/";
	char* getTopic = "$dps/registrations/GET/iotdps-get-operationstatus/";
	char* subTopic = "$dps/registrations/res/#";
	size_t putLen = strlen(putTopic) - 1;
	size_t getLen = strlen(getTopic) - 1;
	size_t subLen = strlen(subTopic) - 1;

	char* new_topic;
	size_t new_topic_len;

	// NOTE: during subscribe its the raw topic
	switch (ed->access) {
		case MOSQ_ACL_SUBSCRIBE:
			if (strncmp(ed->topic, subTopic, subLen) == 0)
				return MOSQ_ERR_SUCCESS;
			
			break;

		case MOSQ_ACL_READ:
			new_topic_len = strlen(clientid) + SLASH_Z + subLen + 1;
			new_topic = mosquitto_calloc(1, new_topic_len);
			if (!new_topic)
				return MOSQ_ERR_NOMEM;

			snprintf(new_topic, new_topic_len, "%s/%s", clientid, subTopic);

			if (strncmp(ed->topic, new_topic, new_topic_len - 1) == 0)
				return MOSQ_ERR_SUCCESS;
			
			break;

		case MOSQ_ACL_WRITE:
			if (strncmp(ed->topic, putTopic, putLen) == 0)
				return MOSQ_ERR_SUCCESS;

			if (strncmp(ed->topic, getTopic, getLen) == 0)
				return MOSQ_ERR_SUCCESS;

			break;

		default:
			break;
	}

	return MOSQ_ERR_ACL_DENIED;

	// if(ed->access == MOSQ_ACL_SUBSCRIBE){
	// 	if(strncmp(ed->topic, subTopic, subLen) == 0){
	// 		return MOSQ_ERR_SUCCESS;
	// 	}
	// }
	// // NOTE: during a inbond message with the prefix - it has the prefix
	// // NOTE: this is the subscribe topic
	// if(ed->access == MOSQ_ACL_READ){
	// 	new_topic_len = strlen(clientid) + SLASH_Z + subLen + 1;
	// 	new_topic = mosquitto_calloc(1, new_topic_len);
	// 	if(!new_topic){
	// 		return MOSQ_ERR_NOMEM;
	// 	}
	// 	snprintf(new_topic, new_topic_len, "%s/%s", clientid, subTopic);

	// 	if(strncmp(ed->topic, new_topic, new_topic_len - 1) == 0){
	// 		return MOSQ_ERR_SUCCESS;
	// 	}
	// }

	// if(ed->access == MOSQ_ACL_WRITE){
	// 	if(strncmp(ed->topic, putTopic, putLen) == 0){
	// 		return MOSQ_ERR_SUCCESS;
	// 	}

	// 	if(strncmp(ed->topic, getTopic, getLen) == 0){
	// 		return MOSQ_ERR_SUCCESS;
	// 	}
	// }

	// return MOSQ_ERR_ACL_DENIED;
}

static int callback_message_in(int event, void* event_data, void* userdata)
{
	struct mosquitto_evt_message* ed = event_data;
	char* new_topic;
	size_t new_topic_len;

	UNUSED(event);
	UNUSED(userdata);

	const char* clientid = mosquitto_client_id(ed->client);
	new_topic_len = strlen(clientid) + SLASH_Z + strlen(ed->topic) + 1;

	/* Allocate some memory - use
	 * mosquitto_calloc/mosquitto_malloc/mosquitto_strdup when allocating, to
	 * allow the broker to track memory usage */
	new_topic = mosquitto_calloc(1, new_topic_len);
	if (new_topic == NULL)
	{
		return MOSQ_ERR_NOMEM;
	}

/* prepend the clientid to the topic */
	snprintf(new_topic, new_topic_len, "%s/%s", clientid, ed->topic);

	/* Assign the new topic to the event data structure. You
	 * must *not* free the original topic, it will be handled by the
	 * broker. */
	ed->topic = new_topic;

	return MOSQ_ERR_SUCCESS;
}

static int callback_message_out(int event, void* event_data, void* userdata)
{
	struct mosquitto_evt_message* ed = event_data;
	size_t clientid_len;

	UNUSED(event);
	UNUSED(userdata);

	const char* clientid = mosquitto_client_id(ed->client);

	if (!is_jailed(clientid))
	{
	/* will only modify the topic of jailed clients */
		return MOSQ_ERR_SUCCESS;
	}

/* remove the clientid from the front of the topic */
	clientid_len = strlen(clientid);

	if (strlen(ed->topic) <= clientid_len + 1)
	{
	/* the topic is not long enough to contain the
	 * clientid + '/' */
		return MOSQ_ERR_SUCCESS;
	}

	if (!strncmp(clientid, ed->topic, clientid_len) && ed->topic[clientid_len] == '/')
	{
	/* Allocate some memory - use
	 * mosquitto_calloc/mosquitto_malloc/mosquitto_strdup when allocating, to
	 * allow the broker to track memory usage */

	 /* skip the clientid + '/' */
		char* new_topic = mosquitto_strdup(ed->topic + clientid_len + 1);

		if (new_topic == NULL)
		{
			return MOSQ_ERR_NOMEM;
		}

	/* Assign the new topic to the event data structure. You
	 * must *not* free the original topic, it will be handled by the
	 * broker. */
		ed->topic = new_topic;
	}

	return MOSQ_ERR_SUCCESS;
}

static int callback_subscribe(int event, void* event_data, void* userdata)
{
	struct mosquitto_evt_subscribe* ed = event_data;
	char* new_sub;
	size_t new_sub_len;

	UNUSED(event);
	UNUSED(userdata);

	const char* clientid = mosquitto_client_id(ed->client);

	if (!is_jailed(clientid))
	{
	/* will only modify the topic of jailed clients */
		return MOSQ_ERR_SUCCESS;
	}

/* put the clientid on front of the topic */
/* calculate the length of the new payload */
	new_sub_len = strlen(clientid) + SLASH_Z + strlen(ed->data.topic_filter) + 1;

	/* Allocate some memory - use
	 * mosquitto_calloc/mosquitto_malloc/mosquitto_strdup when allocating, to
	 * allow the broker to track memory usage */
	new_sub = mosquitto_calloc(1, new_sub_len);
	if (new_sub == NULL)
	{
		return MOSQ_ERR_NOMEM;
	}

/* prepend the clientid to the subscription */
	snprintf(new_sub, new_sub_len, "%s/%s", clientid, ed->data.topic_filter);

	/* Assign the new topic to the event data structure. You
	 * must *not* free the original topic, it will be handled by the
	 * broker. */
	ed->data.topic_filter = new_sub;

	return MOSQ_ERR_SUCCESS;
}

static int callback_unsubscribe(int event, void* event_data, void* userdata)
{
	struct mosquitto_evt_unsubscribe* ed = event_data;
	char* new_sub;
	size_t new_sub_len;

	UNUSED(event);
	UNUSED(userdata);

	const char* clientid = mosquitto_client_id(ed->client);

	if (!is_jailed(clientid))
	{
	/* will only modify the topic of jailed clients */
		return MOSQ_ERR_SUCCESS;
	}

/* put the clientid on front of the topic */
/* calculate the length of the new payload */
	new_sub_len = strlen(clientid) + SLASH_Z + strlen(ed->data.topic_filter) + 1;

	/* Allocate some memory - use
	 * mosquitto_calloc/mosquitto_malloc/mosquitto_strdup when allocating, to
	 * allow the broker to track memory usage */
	new_sub = mosquitto_calloc(1, new_sub_len);
	if (new_sub == NULL)
	{
		return MOSQ_ERR_NOMEM;
	}

/* prepend the clientid to the subscription */
	snprintf(new_sub, new_sub_len, "%s/%s", clientid, ed->data.topic_filter);

	/* Assign the new topic to the event data structure. You
	 * must *not* free the original topic, it will be handled by the
	 * broker. */
	ed->data.topic_filter = new_sub;

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t* identifier, void** user_data, struct mosquitto_opt* opts, int opt_count)
{
	UNUSED(user_data);
	UNUSED(opts);
	UNUSED(opt_count);

	mosq_pid = identifier;
	mosquitto_plugin_set_info(identifier, PLUGIN_NAME, PLUGIN_VERSION);

	int rc;

	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_ACL_CHECK, callback_acl_check, NULL, NULL);
	if (rc)
		return rc;
	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE_IN, callback_message_in, NULL, NULL);
	if (rc)
		return rc;
	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE_OUT, callback_message_out, NULL, NULL);
	if (rc)
		return rc;
	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_SUBSCRIBE, callback_subscribe, NULL, NULL);
	if (rc)
		return rc;
	rc = mosquitto_callback_register(mosq_pid, MOSQ_EVT_UNSUBSCRIBE, callback_unsubscribe, NULL, NULL);
	return rc;
}
// #pragma GCC pop_options