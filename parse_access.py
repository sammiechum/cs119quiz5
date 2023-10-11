#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 16:47:21 2023

@author: sammiechum
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("parse_access.py").getOrCreate()
df = spark.read.options(delimited = ' ').csv("/user/chums/quiz5/access.log")
df.show(5)