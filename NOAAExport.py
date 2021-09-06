#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  6 12:06:38 2021

@author: wrichter
"""

import requests
import pandas as pd
import string
import csv

ftpSite = 'https://ftp.emc.ncep.noaa.gov/mmb/sref/data/TJ/'

datafolder = './data/'
exportfolder = './export/'
filename = 'fcst1_error.year01'


def processLine(input, year, prefix):
    # print(newline)
    newline = ''
    keys = ['t=', 'x=', 'y=', 'z=', 'rmse=']
    if 'Year' in input:  # process day header
        year = input.split('=')[1].strip() + ','
        print('Year:', year)
    elif 'Day' in input:
        prefix = year + (input.split('=')[1].strip() + ',')
    elif 'Control' in input:
        prefix += (input.split('run')[1].strip() + ',')
    elif 'error' in input:
        prefix += (input.split(':')[1].strip() + ',')
    elif 'full' in input:
        prefix += 'Truth run,'
    else:  # Process each day in the year
        parseLine = input
        for key in keys:
            if key in parseLine:
                parseLine = parseLine.split(key)
                newline += parseLine[0].strip() + ','
                parseLine = parseLine[1]
        newline += (parseLine.strip() + '\n')
    return year, prefix, prefix + newline


def exportCsv(data, export, filename):
    year = ''
    header = 'Year,Day,ControlRun,error,id,t,x,y,z,rmse\n'
    if 'full' in filename:
        header = 'Year,Day,full,id,t,x,y,z\n'
    fcstInput = open(data + filename + '.txt', 'r')
    fcstExport = open(export + filename + '.csv', 'w')
    fcstExport.write(header)
    line = fcstInput.readline().replace('\n', '')
    csvline = ''
    csvPrefix = ''
    while(line):
        year, csvPrefix, csvline = processLine(line, year, csvPrefix)
        if 't=' in line:
            fcstExport.write(csvline)
        line = fcstInput.readline().replace('\n', '')
    fcstInput.close()
    fcstExport.close()

def dowloadFcst(fileList, filePath, site):
    for filename in fileList:
        fileUrl = site + filename
        saveFile = filePath + filename + '.txt'
        results = requests.get(fileUrl)
        print('Writing:', saveFile)
        with open(saveFile,'wb') as save:
            save.write(results.content)

if __name__ == '__main__':
    results = requests.get(ftpSite)
    dfFiles = pd.read_html(results.text)
    N = len(dfFiles)
    groups_names = list(string.ascii_uppercase[0:N])
    df = pd.DataFrame()
    for i in range(0,N):
        group_col = [groups_names[i]] * len(dfFiles[i])
        dfFiles[i]['Group'] = group_col
        df = df.append(dfFiles[i])
    df = df.drop('Size', 1)
    df = df.drop('Group', 1)
    df = df.dropna()
    df.to_csv(datafolder + 'fcst.csv')

    fileList = []
    fcstReader = open(datafolder + 'fcst.csv','r')
    fcstList = csv.reader(fcstReader)
    for r in fcstList:
        if not r[1] == 'Name':
            fileList.append(r[1])
    fcstReader.close()
    dowloadFcst(fileList, datafolder, ftpSite)
    for filename in fileList:
        exportCsv(datafolder, exportfolder, filename)
