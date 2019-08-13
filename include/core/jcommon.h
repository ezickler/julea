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

#ifndef JULEA_COMMON_H
#define JULEA_COMMON_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

struct JCommon;

typedef struct JCommon JCommon;

G_END_DECLS

#include <core/jbackend.h>
#include <core/jbatch.h>
#include <core/jconfiguration.h>

G_BEGIN_DECLS

// FIXME copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
void __attribute__((constructor)) j_init (void);
void __attribute__((destructor)) j_fini (void);

void j_init_server (gint);
void j_fini_server (void);

JConfiguration* j_configuration (void);

JBackend* j_object_backend (void);
JBackend* j_kv_backend (void);
JBackend* j_db_backend (void);

// JBackend* j_object_backend_hsm (JHsmHints hsmHints);
// JBackend* j_kv_backend_hsm (JHsmHints hsmHints);
JBackend* j_object_backend_tier (guint32 tier);
JBackend* j_kv_backend_tier (guint32 tier);

G_END_DECLS

#endif
