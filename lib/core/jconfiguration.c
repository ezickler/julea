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

#include <string.h>

#include <jconfiguration.h>

/**
 * \defgroup JConfiguration Configuration
 *
 * @{
 **/

/**
 * A configuration.
 */
struct JConfiguration
{
	struct
	{
		/**
		 * The object servers.
		 */
		gchar** object;

		/**
		 * The kv servers.
		 */
		gchar** kv;

		/**
		 * The db servers.
		 */
		gchar** db;

		/**
		 * The number of object servers.
		 */
		guint32 object_len;

		/**
		 * The number of kv servers.
		 */
		guint32 kv_len;

		/**
		 * The number of db servers.
		 */
		guint32 db_len;
	}
	servers;


	struct
	{
		/**
		 * The object configuration.
		 */
		struct Object_backends
		{
			/**
			 * The backend.
			 */
			gchar* backend;

			/**
			 * The component.
			 */
			gchar* component;

			/**
			 * The path.
			 */
			gchar* path;


		}
		* object;

		/**
		 * The kv configuration.
		 */
		struct Kv_backends
		{
			/**
			 * The backend.
			 */
			gchar* backend;

			/**
			 * The component.
			 */
			gchar* component;

			/**
			 * The path.
			 */
			gchar* path;
		}
		* kv;

		/**
		 * The number of object backends.
		 */
		guint32 object_len;

		/**
		 * The number of kv backends.
		 */
		guint32 kv_len;
	}
	backends;

	/**
	 * The db configuration.
	 */
	struct
	{
		/**
		 * The backend.
		 */
		gchar* backend;

		/**
		 * The component.
		 */
		gchar* component;

		/**
		 * The path.
		 */
		gchar* path;
	} db;

	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;

	/**
	 * The reference count.
	 */
	gint ref_count;
};

/**
 * Creates a new configuration.
 *
 * \code
 * \endcode
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new (void)
{
	JConfiguration* configuration = NULL;
	GKeyFile* key_file;
	gchar* config_name = NULL;
	gchar const* env_path;
	gchar* path = NULL;
	gchar const* const* dirs;

	key_file = g_key_file_new();

	if ((env_path = g_getenv("JULEA_CONFIG")) != NULL)
	{
		if (g_path_is_absolute(env_path))
		{
			if (g_key_file_load_from_file(key_file, env_path, G_KEY_FILE_NONE, NULL))
			{
				configuration = j_configuration_new_for_data(key_file);
			}
			else
			{
				g_critical("Can not open configuration file %s.", env_path);
			}

			/* If we do not find the configuration file, stop searching. */
			goto out;
		}
		else
		{
			config_name = g_path_get_basename(env_path);
		}
	}

	if (config_name == NULL)
	{
		config_name = g_strdup("julea");
	}

	path = g_build_filename(g_get_user_config_dir(), "julea", config_name, NULL);

	if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
	{
		configuration = j_configuration_new_for_data(key_file);

		goto out;
	}

	g_free(path);

	dirs = g_get_system_config_dirs();

	for (guint i = 0; dirs[i] != NULL; i++)
	{
		path = g_build_filename(dirs[i], "julea", config_name, NULL);

		if (g_key_file_load_from_file(key_file, path, G_KEY_FILE_NONE, NULL))
		{
			configuration = j_configuration_new_for_data(key_file);

			goto out;
		}

		g_free(path);
	}

	path = NULL;

out:
	g_key_file_free(key_file);

	g_free(path);
	g_free(config_name);

	return configuration;
}

/**
 * Creates a new configuration for the given configuration data.
 *
 * \code
 * \endcode
 *
 * \param key_file The configuration data.
 *
 * \return A new configuration. Should be freed with j_configuration_unref().
 **/
JConfiguration*
j_configuration_new_for_data (GKeyFile* key_file)
{
	JConfiguration* configuration;
	gchar** servers_object;
	gchar** servers_kv;
	gchar** servers_db;
	gchar** object_backend;
	guint32 object_backend_length;
	gchar** object_component;
	guint32 object_component_length;
	gchar** object_path;
	guint32 object_path_length;
	gchar** kv_backend;
	guint32 kv_backend_length;
	gchar** kv_component;
	guint32 kv_component_length;
	gchar** kv_path;
	guint32 kv_path_length;
	gchar* db_backend;
	gchar* db_component;
	gchar* db_path;
	guint64 max_operation_size;
	guint32 max_connections;
	guint64 stripe_size;

	g_return_val_if_fail(key_file != NULL, FALSE);

	max_operation_size = g_key_file_get_uint64(key_file, "core", "max-operation-size", NULL);
	max_connections = g_key_file_get_integer(key_file, "clients", "max-connections", NULL);
	stripe_size = g_key_file_get_uint64(key_file, "clients", "stripe-size", NULL);

	servers_object = g_key_file_get_string_list(key_file, "servers", "object", NULL, NULL);
	servers_kv = g_key_file_get_string_list(key_file, "servers", "kv", NULL, NULL);
	servers_db = g_key_file_get_string_list(key_file, "servers", "db", NULL, NULL);

	object_backend = g_key_file_get_string_list(key_file, "object", "backend", NULL, NULL);
	object_backend_length = g_strv_length(object_backend);
	object_component = g_key_file_get_string_list(key_file, "object", "component", NULL, NULL);
	object_component_length = g_strv_length(object_backend);
	object_path = g_key_file_get_string_list(key_file, "object", "path", NULL, NULL);
	object_path_length = g_strv_length(object_path);

	kv_backend = g_key_file_get_string_list(key_file, "kv", "backend", NULL, NULL);
	kv_backend_length = g_strv_length(kv_backend);
	kv_component = g_key_file_get_string_list(key_file, "kv", "component", NULL, NULL);
	kv_component_length = g_strv_length(kv_component);
	kv_path = g_key_file_get_string_list(key_file, "kv", "path", NULL, NULL);
	kv_path_length = g_strv_length(kv_path);

	db_backend = g_key_file_get_string(key_file, "db", "backend", NULL);
	db_component = g_key_file_get_string(key_file, "db", "component", NULL);
	db_path = g_key_file_get_string(key_file, "db", "path", NULL);

	if (servers_object == NULL || servers_object[0] == NULL
	    || servers_kv == NULL || servers_kv[0] == NULL
	    || servers_db == NULL || servers_db[0] == NULL
	    || object_backend == NULL
	    || object_component == NULL
	    || object_path == NULL
	    || kv_backend == NULL
	    || kv_component == NULL
	    || kv_path == NULL
		|| ((object_backend_length != object_component_length) && (object_component_length != object_path_length))
		|| ((kv_backend_length != kv_component_length) && (kv_component_length != kv_path_length)))
	    || db_backend == NULL
	    || db_component == NULL
	    || db_path == NULL)
	{
		g_free(db_backend);
		g_free(db_component);
		g_free(db_path);
		g_free(kv_backend);
		g_free(kv_component);
		g_free(kv_path);
		g_free(object_backend);
		g_free(object_component);
		g_free(object_path);
		g_strfreev(servers_object);
		g_strfreev(servers_kv);
		g_strfreev(servers_db);

		return NULL;
	}

	configuration = g_slice_new(JConfiguration);

	configuration->backends.kv = g_new( struct Kv_backends, kv_backend_length );
	configuration->backends.object = g_new( struct Object_backends, object_backend_length );

	configuration->servers.object = servers_object;
	configuration->servers.kv = servers_kv;
	configuration->servers.db = servers_db;
	configuration->servers.object_len = g_strv_length(servers_object);
	configuration->servers.kv_len = g_strv_length(servers_kv);
	configuration->servers.db_len = g_strv_length(servers_db);
	configuration->backends.object_len = object_backend_length;
	for (size_t i = 0; i < object_backend_length; i++) {
		configuration->backends.object[i].backend = object_backend[i];
		configuration->backends.object[i].component = object_component[i];
		configuration->backends.object[i].path = object_path[i];
	}
	configuration->backends.kv_len = kv_backend_length;
	for (size_t i = 0; i < kv_backend_length; i++) {
		configuration->backends.kv[i].component = kv_component[i];
		configuration->backends.kv[i].path = kv_path[i];
		configuration->backends.kv[i].backend = kv_backend[i];
	}
	configuration->db.backend = db_backend;
	configuration->db.component = db_component;
	configuration->db.path = db_path;
	configuration->max_operation_size = max_operation_size;
	configuration->max_connections = max_connections;
	configuration->stripe_size = stripe_size;
	configuration->ref_count = 1;

	if (configuration->max_operation_size == 0)
	{
		configuration->max_operation_size = 8 * 1024 * 1024;
	}

	if (configuration->max_connections == 0)
	{
		configuration->max_connections = g_get_num_processors();
	}

	if (configuration->stripe_size == 0)
	{
		configuration->stripe_size = 4 * 1024 * 1024;
	}

	return configuration;
}

/**
 * Increases a configuration's reference count.
 *
 * \code
 * JConfiguration* c;
 *
 * j_configuration_ref(c);
 * \endcode
 *
 * \param configuration A configuration.
 *
 * \return #configuration.
 **/
JConfiguration*
j_configuration_ref (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	g_atomic_int_inc(&(configuration->ref_count));

	return configuration;
}

/**
 * Decreases a configuration's reference count.
 * When the reference count reaches zero, frees the memory allocated for the configuration.
 *
 * \code
 * \endcode
 *
 * \param configuration A configuration.
 **/
void
j_configuration_unref (JConfiguration* configuration)
{
	if (g_atomic_int_dec_and_test(&(configuration->ref_count)))
	{
		for (size_t i = 0; i < configuration->backends.kv_len; i++) {
			g_free(configuration->backends.kv[i].backend);
			g_free(configuration->backends.kv[i].component);
			g_free(configuration->backends.kv[i].path);
		}
		g_free(configuration->backends.kv);

		for (size_t i = 0; i < configuration->backends.object_len; i++) {
			g_free(configuration->backends.object[i].backend);
			g_free(configuration->backends.object[i].component);
			g_free(configuration->backends.object[i].path);
		}
		g_free(configuration->backends.object);

		g_free(configuration->db.backend);
		g_free(configuration->db.component);
		g_free(configuration->db.path);
		
		g_strfreev(configuration->servers.object);
		g_strfreev(configuration->servers.kv);
		g_strfreev(configuration->servers.db);

		g_slice_free(JConfiguration, configuration);
	}
}

gchar const*
j_configuration_get_object_server (JConfiguration* configuration, guint32 index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.object_len, NULL);

	return configuration->servers.object[index];
}

gchar const*
j_configuration_get_kv_server (JConfiguration* configuration, guint32 index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.kv_len, NULL);

	return configuration->servers.kv[index];
}

gchar const*
j_configuration_get_db_server (JConfiguration* configuration, guint32 index)
{
	g_return_val_if_fail(configuration != NULL, NULL);
	g_return_val_if_fail(index < configuration->servers.db_len, NULL);

	return configuration->servers.db[index];
}

guint32
j_configuration_get_object_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.object_len;
}

guint32
j_configuration_get_kv_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.kv_len;
}

guint32
j_configuration_get_db_server_count (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->servers.db_len;
}

gchar const*
j_configuration_get_object_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.object[0].backend;
}

gchar const*
j_configuration_get_object_component (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.object[0].component;
}

gchar const*
j_configuration_get_object_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.object[0].path;
}

gchar const*
j_configuration_get_kv_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.kv[0].backend;
}

gchar const*
j_configuration_get_kv_component (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.kv[0].component;
}

gchar const*
j_configuration_get_kv_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->backends.kv[0].path;
}

gchar const*
j_configuration_get_db_backend (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->db.backend;
}

gchar const*
j_configuration_get_db_component (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->db.component;
}

gchar const*
j_configuration_get_db_path (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, NULL);

	return configuration->db.path;
}

guint64
j_configuration_get_max_operation_size (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_operation_size;
}

guint32
j_configuration_get_max_connections (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->max_connections;
}

guint64
j_configuration_get_stripe_size (JConfiguration* configuration)
{
	g_return_val_if_fail(configuration != NULL, 0);

	return configuration->stripe_size;
}

/**
 * @}
 **/
