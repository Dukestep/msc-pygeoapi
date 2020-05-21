# =================================================================
#
# Author: Etienne Pelletier <etienne.pelletier@canada.ca>
#
# Copyright (c) 2020 Etienne Pelletier
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import csv
import gzip
import logging
import os
from pathlib import Path
import uuid

import click
from elasticsearch import helpers, logger as elastic_logger
from msc_pygeoapi.env import (MSC_PYGEOAPI_ES_TIMEOUT, MSC_PYGEOAPI_ES_URL,
                              MSC_PYGEOAPI_ES_AUTH)
from msc_pygeoapi.loader.base import BaseLoader
from msc_pygeoapi.util import get_es

LOGGER = logging.getLogger(__name__)
elastic_logger.setLevel(logging.WARNING)

# index settings
INDEX_NAME = 'emet_{}_{}'

FILE_PROPERTIES = {
    'refpoints': {
        'lat': {
            'type': 'float'
        },
        'lon': {
            'type': 'float'
        },
        'alt': {
            'type': 'integer',
        },
        'alt_obs': {
            'type': 'integer',
        },
        'land_sea_proportion': {
            'type': 'float',
        },
    },
    'obsmatch': {
        'id_prog': {
            'type': 'integer'
        },
        'date_orig': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'date_valid': {
            'type': 'date',
            'format': 'date_time_no_millis',
            'ignore_malformed': False,
        },
        'id_var': {
            'type': 'integer',
        },
        'lat': {
            'type': 'float',
        },
        'lon': {
            'type': 'float',
        },
        'code_stn': {
            'type': 'text'
        },
        'value_model': {
            'type': 'float',
        },
        'id_obs': {
            'type': 'long',
        },
        'value_obs': {
            'type': 'integer',
        },
        'diff': {
            'type': 'float'
        }
    }
}

SETTINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'properties': {
            'geometry': {
                'type': 'geo_shape'
            },
            'properties': {
                'properties': None
            }
        }
    }
}


class EmetLoader(BaseLoader):
    """Emet loader"""

    def __init__(self, plugin_def):
        """initializer"""

        BaseLoader.__init__(self)

        self.ES = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)
        self.filepath = None
        self.type = None

    def file_open(self):
        if self.type == 'obsmatch':
            return gzip.open(self.filepath, mode='rt')
        if self.type == 'refpoints':
            return open(self.filepath)

    def generate_features(self):
        """
        :returns: Generator of ElasticSearch actions to upsert the EMET
                  refpoints for given file.
        """
        with self.file_open() as f:
            reader = csv.reader(f)
            try:
                # discard headers
                next(reader)
            except StopIteration:
                raise EOFError('File at {} is empty'.format(self.filepath))

            for row in reader:
                properties = FILE_PROPERTIES[self.type].keys()
                feature = {
                    'type': 'Feature',
                    'properties': {},
                    'geometry': {
                        'type': 'Point',
                        'coordinates': None
                    }
                }

                for index, value in enumerate(properties):
                    feature['properties'][value] = row[index]

                if self.type == 'refpoints':
                    feature['geometry']['coordinates'] = [float(row[1]), float(row[0])]
                elif self.type == 'obsmatch':
                    feature['geometry']['coordinates'] = [float(row[5]), float(row[4])]

                action = {
                    '_id': uuid.uuid4(),
                    '_index': "emet_{}".format(self.filepath.stem),
                    '_op_type': 'update',
                    'doc': feature,
                    'doc_as_upsert': True
                }

                yield action

    def load_data(self, filepath):
        """
        loads data from event to target
        :returns: `bool` of status result
        """

        self.filepath = Path(filepath)

        inserts = 0
        updates = 0
        noops = 0
        fails = 0

        LOGGER.debug('Received file {}'.format(self.filepath))
        chunk_size = 80000

        if self.filepath.suffix == '.gz':
            self.type = 'obsmatch'
        else:
            self.type = 'refpoints'

        index_name = "emet_{}".format(self.filepath.stem)

        if not self.ES.indices.exists(index_name):
            SETTINGS['mappings']['properties']['properties']['properties'] = FILE_PROPERTIES[self.type]
            self.ES.indices.create(index=index_name,
                                   body=SETTINGS,
                                   request_timeout=MSC_PYGEOAPI_ES_TIMEOUT)

        package = self.generate_features()

        for ok, response in helpers.streaming_bulk(self.ES, package,
                                                   chunk_size=chunk_size,
                                                   request_timeout=30):
            status = response['update']['result']

            if status == 'created':
                inserts += 1
            elif status == 'updated':
                updates += 1
            elif status == 'noop':
                noops += 1
            else:
                LOGGER.warning('Unhandled status code {}'.format(status))

        total = inserts + updates + noops + fails
        LOGGER.info('Inserted package of {} features ({} '
                    'inserts, {} updates, {} no-ops, {} rejects)'
                    .format(total, inserts, updates, noops, fails))
        return True


@click.group()
def emet():
    """Manages EMET indices"""
    pass


@click.command()
@click.pass_context
@click.option('--file', '-f', 'file_',
              type=click.Path(exists=True, resolve_path=True),
              help='Path to file')
@click.option('--directory', '-d', 'directory',
              type=click.Path(exists=True, resolve_path=True,
                              dir_okay=True, file_okay=False),
              help='Path to directory')
def add(ctx, file_, directory):
    """add data to system"""

    if all([file_ is None, directory is None]):
        raise click.ClickException('Missing --file/-f or --dir/-d option')

    files_to_process = []

    if file_ is not None:
        files_to_process = [file_]
    elif directory is not None:
        for root, dirs, files in os.walk(directory):
            for f in [file for file in files if file.endswith('.shp')]:
                files_to_process.append(os.path.join(root, f))
        files_to_process.sort(key=os.path.getmtime)

    for file_to_process in files_to_process:
        plugin_def = {
            'filename_pattern': 'g1710n',
            'handler': 'msc_pygeoapi.loader.emet.EmetLoader'  # noqa
        }
        loader = EmetLoader(plugin_def)
        result = loader.load_data(file_to_process)
        if result:
            click.echo(
                'Successfully loaded {} data'.format(loader.filepath.stem))


# @click.command()
# @click.pass_context
# @click.option('--index_name', '-i',
#               type=click.Choice(INDICES),
#               help='msc-geousage elasticsearch index name to delete')
# def delete_index(ctx, index_name):
#     """
#     Delete a particular ES index with a given name as argument or all if no
#     argument is passed
#     """
#     es = get_es(MSC_PYGEOAPI_ES_URL, MSC_PYGEOAPI_ES_AUTH)
#     if index_name:
#         if click.confirm(
#                 'Are you sure you want to delete ES index named: {}?'.format(
#                     click.style(index_name, fg='red')), abort=True):
#             LOGGER.info('Deleting ES index {}'.format(index_name))
#             es.indices.delete(index=index_name)
#             return True
#     else:
#         if click.confirm(
#             'Are you sure you want to delete {} forecast polygon'
#             ' indices ({})?'.format(click.style('ALL', fg='red'),
#                                     click.style(", ".join(INDICES), fg='red')),
#                 abort=True):
#             es.indices.delete(index=",".join(INDICES))
#             return True


emet.add_command(add)
