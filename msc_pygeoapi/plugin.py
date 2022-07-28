# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@canada.ca>
#         Felix Laframboise <felix.laframboise@canada.ca>
#
# Copyright (c) 2020 Tom Kralidis
# Copyright (c) 2021 Felix Laframboise
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

import importlib
import logging

LOGGER = logging.getLogger(__name__)

PLUGINS = {
    'loader': {
        'hydrometric_realtime': {
            'filename_pattern': 'hydrometric',
            'path': 'msc_pygeoapi.loader.hydrometric_realtime.HydrometricRealtimeLoader'  # noqa
        },
        'bulletins_realtime': {
            'filename_pattern': 'bulletins/alphanumeric',
            'path': 'msc_pygeoapi.loader.bulletins_realtime.BulletinsRealtimeLoader'  # noqa
        },
        'citypageweather_realtime': {
            'filename_pattern': 'citypage_weather/xml',
            'path': 'msc_pygeoapi.loader.citypageweather_realtime.CitypageweatherRealtimeLoader'  # noqa
        },
        'hurricanes_realtime': {
            'filename_pattern': 'trajectoires/hurricane',
            'path': 'msc_pygeoapi.loader.hurricanes_realtime.HurricanesRealtimeLoader'  # noqa
        },
        'forecast_polygons': {
            'filename_pattern': 'meteocode/geodata/',
            'path': 'msc_pygeoapi.loader.forecast_polygons.ForecastPolygonsLoader'  # noqa
        },
        'marine_weather_realtime': {
            'filename_pattern': 'marine_weather/xml/',
            'path': 'msc_pygeoapi.loader.marine_weather_realtime.MarineWeatherRealtimeLoader'  # noqa
        },
        'cap_alerts_realtime': {
            'filename_pattern': 'alerts/cap',
            'path': 'msc_pygeoapi.loader.cap_alerts_realtime.CapAlertsRealtimeLoader'  # noqa
        },
        'swob_realtime': {
            'filename_pattern': 'observations/swob-ml',
            'path': 'msc_pygeoapi.loader.swob_realtime.SWOBRealtimeLoader'
        },
        'aqhi_realtime': {
            'filename_pattern': 'air_quality/aqhi',
            'path': 'msc_pygeoapi.loader.aqhi_realtime.AQHIRealtimeLoader'
        },
    },
    'notifier': {
        'Celery': {
            'path': 'msc_pygeoapi.notifier.celery_.CeleryTaskNotifier'
        }
    },
}


def load_plugin(plugin_type, plugin_def, **kwargs):
    """
    loads plugin by type

    :param plugin_type: type of plugin (loader, etc.)
    :param plugin_def: plugin definition

    :returns: plugin object
    """

    type_ = plugin_def['type']

    if plugin_type not in PLUGINS.keys():
        msg = 'Plugin type {} not found'.format(plugin_type)
        LOGGER.exception(msg)
        raise InvalidPluginError(msg)

    plugin_list = PLUGINS[plugin_type]

    LOGGER.debug('Plugins: {}'.format(plugin_list))

    if '.' not in type_ and type_ not in plugin_list.keys():
        msg = 'Plugin {} not found'.format(type_)
        LOGGER.exception(msg)
        raise InvalidPluginError(msg)

    if '.' in type_:  # dotted path
        packagename, classname = type_['path'].rsplit('.', 1)
    else:  # core formatter
        packagename, classname = plugin_list[type_]['path'].rsplit('.', 1)

    LOGGER.debug('package name: {}'.format(packagename))
    LOGGER.debug('class name: {}'.format(classname))

    module = importlib.import_module(packagename)
    class_ = getattr(module, classname)
    plugin = class_(plugin_def)
    return plugin


class InvalidPluginError(Exception):
    """Invalid plugin"""
    pass
