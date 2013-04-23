/*
 * Copyright (c) 2010-2013 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include <julea-config.h>

#include "julea.h"

#include <jitem-internal.h>
#include <juri.h>

gboolean
j_cmd_status (gchar const** arguments)
{
	gboolean ret = TRUE;
	JBatch* batch;
	JURI* uri = NULL;
	GError* error = NULL;

	if (j_cmd_arguments_length(arguments) != 1)
	{
		ret = FALSE;
		j_cmd_usage();
		goto end;
	}

	if ((uri = j_uri_new(arguments[0])) == NULL)
	{
		ret = FALSE;
		g_print("Error: Invalid argument “%s”.\n", arguments[0]);
		goto end;
	}

	if (!j_uri_get(uri, &error))
	{
		ret = FALSE;
		g_print("Error: %s\n", error->message);
		g_error_free(error);
		goto end;
	}

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

	if (j_uri_get_item(uri) != NULL)
	{
		JCredentials* credentials;
		GDateTime* date_time;
		gchar* modification_time_string;
		gchar* size_string;
		guint64 modification_time;
		guint64 size;

		j_item_get_status(j_uri_get_item(uri), J_ITEM_STATUS_ALL, batch);
		j_batch_execute(batch);

		credentials = j_item_get_credentials(j_uri_get_item(uri));
		modification_time = j_item_get_modification_time(j_uri_get_item(uri));
		size = j_item_get_size(j_uri_get_item(uri));

		date_time = g_date_time_new_from_unix_local(modification_time / G_USEC_PER_SEC);
		modification_time_string = g_date_time_format(date_time, "%Y-%m-%d %H:%M:%S");
		size_string = g_format_size(size);

		g_print("User:              %" G_GUINT32_FORMAT "\n", j_credentials_get_user(credentials));
		g_print("Group:             %" G_GUINT32_FORMAT "\n", j_credentials_get_group(credentials));
		g_print("Modification time: %s.%06" G_GUINT64_FORMAT "\n", modification_time_string, modification_time % G_USEC_PER_SEC);
		g_print("Size:              %s\n", size_string);

		j_credentials_unref(credentials);

		g_date_time_unref(date_time);

		g_free(modification_time_string);
		g_free(size_string);
	}
	else if (j_uri_get_collection(uri) != NULL)
	{

	}
	else
	{
		ret = FALSE;
		j_cmd_usage();
	}

	j_batch_unref(batch);

end:
	if (uri != NULL)
	{
		j_uri_free(uri);
	}

	return ret;
}