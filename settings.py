#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

# SECURITY WARNING: don't run with debug turned on in production!

DEBUG = False

# MySQL Database parameters

DATABASES = {
    'default': {
        'NAME': 'facedet',
        'USER': 'root',
        'PASSWORD': '123456',
        'HOST': '192.168.1.145',
        'PORT': 3306,
    },
    'remote': {
        'NAME': 'facePlatformDC',
        'USER': 'root',
        'PASSWORD': '123456',
        'HOST': '192.168.1.145',
        'PORT': 3306,
    }
}

# Application key for replica
appKey = '385f9b60_0617_4acc_b297_e5a043196798'

# Start time
startTime = '1970-01-01 00:00:00'

# End time
endTime = '2017-06-30 23:59:59'

