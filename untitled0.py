# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 17:57:09 2021

@author: xcxg109
"""

import pandas as pd

df = pd.DataFrame({'foo': ['one', 'one', 'one', 'two', 'two',
                           'two'],
                   'bar': ['A', 'B', 'C', 'A', 'B', 'C'],
                   'baz': [1, 2, 3, 4, 5, 6],
                   'zoo': ['x', 'y', 'z', 'q', 'w', 't']})

df.pivot(index='foo', columns='bar', values='baz')