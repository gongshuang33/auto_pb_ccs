import os
import string
import logging

LOG = logging.getLogger(__name__)


def check_file(file):
    if not os.path.exists(file):
        LOG.error('File Not Found, Exit! %s' % file)
        raise Exception

    return 0


def check_path(path):
    if not os.path.exists(path):
        LOG.error('Path Not Found, Exit! %s' % path)
        raise Exception

    return 0


def RenderShell(temp, conf):

    return string.Template(temp).safe_substitute(conf)


