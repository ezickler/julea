#!/usr/bin/env python

import os

import Utils

def set_options (opt):
	opt.tool_options('boost')
	opt.tool_options('compiler_cxx')

def configure (conf):
	conf.check_tool('boost')
	conf.check_tool('compiler_cxx')

	conf.check_boost(
		lib='filesystem program_options system thread'
	)

	conf.check_cxx(
		lib='mongoclient'
	)

	conf.write_config_header('config.h')

def build (bld):
	bld.new_task_gen(
		features = 'cxx cstaticlib',
		source = ['common.cc', 'julea.cc'],
		target = 'julea',
		uselib = ['BOOST_FILESYSTEM', 'BOOST_PROGRAM_OPTIONS', 'BOOST_THREAD', 'MONGOCLIENT'],
		includes = ['.']
	)

	bld.new_task_gen(
		features = 'cxx cprogram',
		source = ['test.cc'],
		target = 'test',
		uselib = ['BOOST_FILESYSTEM', 'BOOST_PROGRAM_OPTIONS', 'BOOST_THREAD', 'MONGOCLIENT'],
		uselib_local = ['julea'],
		includes = ['.']
	)