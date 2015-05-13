#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2015 jaidev <jaidev@newton>
#
# Distributed under terms of the MIT license.

"""SpikedPandas"""


def get_namespaces(client):
    """Get a list of namespaces defined on the server.

    :param client: Connected aerospike client.
    :type client: aerospike.Client
    :return: namespaces
    :rtype: list
    :Example:
    >>> get_namespaces(client)
    ['foo', 'bar', 'baz']
    """
    data = client.info("namespaces").values()[0][1]
    return data.rstrip().split(';')


def get_sets(client, namespace):
    """Get list of sets in a namespace.

    :param client: Connected aerospike client.
    :param namespace: A valid namespace on the server.
    :type client: aerospike.Client
    :type namespace: str
    :return: sets under the namespace
    :rtype: list
    :Examples:
    >>> get_sets(client, "southpark")
    ['stan', 'kyle', 'cartman', 'kenny']
    """
    command = "sets/{}".format(namespace)
    output = client.info(command).values()[0][1]
    set_info_list = output.rstrip().split(';')
    set_names = []
    for set_info in set_info_list:
        info = set_info.split(':')
        for info_item in info:
            if info_item.startswith('set_name='):
                set_names.append(info_item.replace('set_name=', ''))
                break
    return set_names


def get_bins(client, namespace):
    """Get a list of all bins for a given namespaces.

    :param client: Connected aerospike client.
    :param namespace: A valid namespace on the server.
    :type client: aerospike.Client
    :type namespace: str
    :return: bins of the namespace
    :rtype: list
    :Example:
    >>> get_bins(client, "iris")
    ['Sepal Length', 'Sepal Width', 'Petal Length', 'Petal Width', 'Species']
    """
    command = "bins/{}".format(namespace)
    output = client.info(command).values()[0][1]
    return output.rstrip().split(',')[2:]


class AerospikeSetDataFrame(object):
    """A wrapper which makes an aerospike client look like a pandas.DataFrame.
    """

    def __init__(self, client, namespace, set_name):
        self.client = client
        self.namespace = namespace
        self.set_name = set_name

    def head(self, n_rows=5):
        pass

    @property
    def columns(self):
        self._bins = get_bins(self.client, self.namespace)
        return self._bins
