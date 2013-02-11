from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-arrayexpress',
	version=version,
	description="Harvester and supporting code for importing dataset descriptions from ArrayExpress via their API and MAGE-TAB files.",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='James McCusker',
	author_email='james.mccusker@yale.edu',
	url='https://github.com/jimmccusker/ckanext-arrayexpress',
	license='Apache 2.0 License',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.arrayexpress'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
	entry_points=\
	"""
        [ckan.plugins]
        arrayexpress=ckanext.arrayexpress:ArrayExpressHarvester
	# Add plugins here, eg
	# myplugin=ckanext.arrayexpress:PluginClass
	""",
)
