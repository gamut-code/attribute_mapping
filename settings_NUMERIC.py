# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 20:44:56 2019

@author: xcxg109
"""
from tkinter.filedialog import askopenfilename
import re
import os



def get_file_data():
    #ask for user input for which file to read
    file_name = askopenfilename(initialdir = att_directory)
    file_data = [re.split('\s+', i.strip('\n')) for i in open(file_name)]
    return file_data

def choose_file():
    #ask for user input for which file to read
    file_name = askopenfilename(initialdir = att_directory)
    return file_name
    
def get_files_in_directory():
    #read all files in directory then process them in sequence
    file_data = dict()
    path = "F:\\CGabriel\\Grainger_Shorties\\OUTPUT\\L1s\\"
    file_list = os.listdir(path)
    for file in file_list:
        file_data[file] = [re.split('\s+', i.strip('\n')) for i in open(path+file)]
        print('file nodes (file {}) = {}'.format(file, file_data[file]))

    return file_data


directory_name = 'C:/Users/xcxg109/NonDriveFiles'

att_directory = 'C:/Users/xcxg109/NonDriveFiles/reference'
