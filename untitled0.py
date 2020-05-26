# -*- coding: utf-8 -*-
"""
Created on Wed May 20 21:44:43 2020

@author: xcxg109
"""

import pandas as pd
import io
import requests
import time
import re

string_lst = ['fun', 'dum', 'sun', 'gum']

x = "I love to have fun in the sun."
regex = re.compile("(?=(" + "|".join(map(re.escape, string_lst)) + "))")
print(re.findall(regex, x))
