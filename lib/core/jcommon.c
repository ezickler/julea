/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2019 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <jcommon.h>


#include <core/jhelper.h>
#include <jbackend.h>
#include <jbackground-operation-internal.h>
#include <jconfiguration.h>
#include <jconnection-pool-internal.h>
#include <jdistribution-internal.h>
#include <jlist.h>
#include <jlist-iterator.h>
#include <jbatch.h>
#include <jbatch-internal.h>
#include <joperation-cache-internal.h>
#include <joperation-internal.h>
#include <jtrace-internal.h>

/**
 * \defgroup JCommon Common
 * @{
 **/

/**
 * Common structure.
 */
struct JCommon
{
	/**
	 * The configuration.
	 */
	JConfiguration* configuration;


	JBackend** object_backend;
	JBackend** kv_backend;
	JBackend* db_backend;


	GModule** object_module;
	GModule** kv_module;
	GModule* db_module;


	guint32 object_tier_count;
	guint32 kv_tier_count;
};

static JCommon* j_common = NULL;

enum Component{Server, Client};

/**
 * Returns whether JULEA has been initialized.
 *
 * \private
 *
 * \return TRUE if JULEA has been initialized, FALSE otherwise.
 */
static
gboolean
j_is_initialized (void)
{
	JCommon* p;

	p = g_atomic_pointer_get(&j_common);

	return (p != NULL);
}

/**
 * Returns the program name.
 *
 * \private
 *
 * \param default_name Default name
 *
 * \return The progran name if it can be determined, default_name otherwise.
 */
static
gchar*
j_get_program_name (gchar const* default_name)
{
	gchar* program_name;

	if ((program_name = g_file_read_link("/proc/self/exe", NULL)) != NULL)
	{
		gchar* basename;

		basename = g_path_get_basename(program_name);
		g_free(program_name);
		program_name = basename;
	}

	if (program_name == NULL)
	{
		program_name = g_strdup(default_name);
	}

	return program_name;
}

/**
 * Initializes JULEA.
 *
 * \param argc A pointer to \c argc.
 * \param argv A pointer to \c argv.
 */
static
void
j_init_intern (enum Component component, gint opt_port)
{
	JCommon* common;
	g_autofree gchar* basename = NULL;
	gchar const* object_backend;
	gchar const* object_component;
	g_autofree gchar* object_path = NULL;
	gchar const* kv_backend;
	gchar const* kv_component;
	g_autofree gchar* kv_path = NULL;
	gchar const* db_backend;
	gchar const* db_component;
	g_autofree gchar* db_path = NULL;
	g_autofree gchar* port_str = NULL;

	gboolean success_load_backend;

	// Server should be able to reinitilaize as Client
	// initlizatiosn is called on libray load.
	if (j_is_initialized() && component == Client)
	{
		return;
	}

	common = g_slice_new(JCommon);
	common->configuration = NULL;

	basename = j_get_program_name("julea");
	j_trace_init(basename);

	j_trace_enter(G_STRFUNC, NULL);

	common->configuration = j_configuration_new();

	common->object_tier_count = j_configuration_get_object_tier_count(common->configuration);
	common->kv_tier_count = j_configuration_get_kv_tier_count(common->configuration);

	//FIXME memory is reserver for all backenda in client and server but never used in bothe
	common->object_backend = g_new( JBackend*, common->object_tier_count );
	common->object_module = g_new( GModule*, common->object_tier_count );
	common->kv_backend = g_new( JBackend*, common->kv_tier_count );
	common->kv_module = g_new( GModule*, common->kv_tier_count );


	if (common->configuration == NULL)
	{
		goto error;
	}

	port_str = g_strdup_printf("%d", opt_port);


	db_backend = j_configuration_get_db_backend(common->configuration);
	db_component = j_configuration_get_db_component(common->configuration);
	//db_path = j_configuration_get_db_path(common->configuration);
	db_path = j_helper_str_replace(j_configuration_get_db_path(common->configuration), "{PORT}", port_str);


	if(component == Server)
	{
		if (j_backend_load_server(db_backend, db_component, J_BACKEND_TYPE_DB, &(common->db_module), &(common->db_backend)))
		{
			if (common->db_backend == NULL || !j_backend_db_init(common->db_backend, db_path))
			{
				g_critical("Could not initialize db backend %s.\n", db_backend);
				goto error;
			}
		}
	}
	if(component == Client)
	{
		if (j_backend_load_client(db_backend, db_component, J_BACKEND_TYPE_DB, &(common->db_module), &(common->db_backend)))
		{
			if (common->db_backend == NULL || !j_backend_db_init(common->db_backend, db_path))
			{
				g_critical("Could not initialize db backend %s.\n", db_backend);
				goto error;
			}
		}
	}

	for (guint32 tier = 0; tier < common->object_tier_count; tier++)
	{
		object_backend = j_configuration_get_object_backend(common->configuration, tier);
		object_component = j_configuration_get_object_component(common->configuration, tier);
		object_path  = j_helper_str_replace(j_configuration_get_object_path(common->configuration, 0), "{PORT}", port_str);


		if(component == Server)
		{
			success_load_backend = j_backend_load_server(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &(common->object_module[tier]), &(common->object_backend[tier]));
		}
		if(component == Client)
		{
			success_load_backend = j_backend_load_client(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &(common->object_module[tier]), &(common->object_backend[tier]));
		}

		if (success_load_backend)
		{
			if (common->object_backend[tier] == NULL || !j_backend_object_init(common->object_backend[tier], object_path))
			{
				g_critical("Could not initialize object backend %s.\n", object_backend);
				goto error;
			}
		}
	}

	for (guint32 tier = 0; tier < common->kv_tier_count; tier++)
	{
		kv_backend = j_configuration_get_kv_backend(common->configuration, tier);
		kv_component = j_configuration_get_kv_component(common->configuration, tier);
		kv_path = j_helper_str_replace(j_configuration_get_kv_path(common->configuration, 0), "{PORT}", port_str);


		if(component == Server)
		{
			success_load_backend = j_backend_load_server(kv_backend, kv_component, J_BACKEND_TYPE_KV, &(common->kv_module[tier]), &(common->kv_backend[tier]));
		}
		if(component == Client)
		{
			success_load_backend = j_backend_load_client(kv_backend, kv_component, J_BACKEND_TYPE_KV, &(common->kv_module[tier]), &(common->kv_backend[tier]));
		}

		if (success_load_backend )
		{
			if (common->kv_backend[tier] == NULL || !j_backend_kv_init(common->kv_backend[tier], kv_path))
			{
				g_critical("Could not initialize kv backend %s.\n", kv_backend);
				goto error;
			}
		}
	}

	if (j_backend_load_client(db_backend, db_component, J_BACKEND_TYPE_DB, &(common->db_module), &(common->db_backend)))
	{
		if (common->db_backend == NULL || !j_backend_db_init(common->db_backend, db_path))
		{
			g_critical("Could not initialize db backend %s.\n", db_backend);
			goto error;
		}
	}

	if(component == Client)
	{
		j_connection_pool_init(common->configuration);
		j_distribution_init();
		j_background_operation_init(0);
		j_operation_cache_init();
	}

	g_atomic_pointer_set(&j_common, common);

	j_trace_leave(G_STRFUNC);

	return;

error:
	if (common->configuration != NULL)
	{
		j_configuration_unref(common->configuration);
	}

	j_trace_leave(G_STRFUNC);

	j_trace_fini();

	g_slice_free(JCommon, common);

	g_error("%s: Failed to initialize JULEA.", G_STRLOC);
}


/**
 * Initializes JULEA as client..
 *
 * \param argc A pointer to \c argc.
 * \param argv A pointer to \c argv.
 */
void
j_init (void)
{
	//FIXME port. paramter is not used in Client init
	j_init_intern(Client, 0);
}


/**
 * Initializes JULEA as server..
 *
 * \param argc A pointer to \c argc.
 * \param argv A pointer to \c argv.
 */
void
j_init_server (gint opt_port)
{
	j_init_intern(Server, opt_port);
}

/**
 * Shuts down JULEA.
 */
static
void
j_fini_internal (enum Component component)
{
	JCommon* common;

	if (!j_is_initialized())
	{
		return;
	}

	j_trace_enter(G_STRFUNC, NULL);

	if(component == Client)
	{
		j_operation_cache_fini();
		j_background_operation_fini();
		j_connection_pool_fini();
	}

	common = g_atomic_pointer_get(&j_common);
	g_atomic_pointer_set(&j_common, NULL);

	if (common->db_backend != NULL)
	{
		j_backend_db_fini(common->db_backend);
	}

	if (common->db_module)
	{
		g_module_close(common->db_module);
	}

	for (guint32 tier = 0; tier < common->object_tier_count; tier++)
	{
		if (common->object_module[tier])
		{
			g_module_close(common->object_module[tier]);
		}

		if (common->object_backend[tier] != NULL)
		{
			j_backend_object_fini(common->object_backend[tier]);
		}
	}

	for (guint32 tier = 0; tier < common->kv_tier_count; tier++)
	{
		if (common->kv_backend[tier] != NULL)
		{
			j_backend_kv_fini(common->kv_backend[tier]);
		}

		if (common->kv_module[tier])
		{
			g_module_close(common->kv_module[tier]);
		}
	}

	g_free(common->object_backend);
	g_free(common->object_module);
	g_free(common->kv_backend);
	g_free(common->kv_module);


	if(component == Client)
	{
		j_configuration_unref(common->configuration);
	}

	j_trace_leave(G_STRFUNC);

	j_trace_fini();

	g_slice_free(JCommon, common);
}


/**
 * Shuts down JULEA.
 */

void
j_fini (void)
{
	j_fini_internal(Client);
}

/**
 * Shuts down JULEA.
 */
void
j_fini_server (void)
{
	j_fini_internal(Server);
}

/* Internal */

/**
 * Returns the configuration.
 *
 * \private
 *
 * \return The configuration.
 */
JConfiguration*
j_configuration (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->configuration;
}

/**
 * Returns the object backend.
 *
 * \private
 *
 * \return The object backend.
 */
JBackend*
j_object_backend_tier (guint32 tier)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->object_backend[tier];
}

/**
 * Returns the kv backend.
 *
 * \private
 *
 * \return The kv backend.
 */
JBackend*
j_object_backend (void)
{
	return j_object_backend_tier(0);
}

/**
 * Returns the data backend.
 *
 * \private
 *
 * \return The data backend.
 */
JBackend*
j_kv_backend_tier (guint32 tier)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->kv_backend[tier];
}

/**
 * Returns the data backend.
 *
 * \private
 *
 * \return The data backend.
 */
JBackend*
j_kv_backend (void)
{
	return j_kv_backend_tier(0);
}

/**
 * Returns the db backend.
 *
 * \private
 *
 * \return The db backend.
 */
JBackend*
j_db_backend (void)
{
	JCommon* common;

	g_return_val_if_fail(j_is_initialized(), NULL);

	common = g_atomic_pointer_get(&j_common);

	return common->db_backend;
}

/**
 * @}
 **/
