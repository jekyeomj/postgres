#!/usr/bin/python3
import argparse
#import git
import os
import os.path
import subprocess
import sys
import time
import re
import json

def vprint(*args, **kwargs):
  print(*args, **kwargs)

def setup_argparse():
  parser = argparse.ArgumentParser(description='sysbench parser')
  parser.add_argument('-v', '--verbose', action='store_true', help='Print extra information to stdout', default=False)
# RAID/RAIZN parameters
  parser.add_argument('-f', '--file', help='target db file', required=True)
#   parser.add_argument('-d', '--directory', default="./", help='target directory path (e.g. ./result)', required=True)
  return parser

def process(args):
  global OUTPUT

  print(args.file)

  db_file = open(args.file, 'rb')
  
  db_file.seek(-4, 2)
  # db_file.seek(0)
  json_size = int.from_bytes(db_file.read(4), "little")
  
  print(json_size)
 
  db_file.seek((json_size + 4) * -1, 2)
  raw_data = db_file.read(json_size)
  
  json_res = json.loads(raw_data)
  
  print(json.dumps(json_res, indent=2))
  
  print(json_res["Columns"])

def main():
  global VERBOSE
  parser = setup_argparse()
  args = parser.parse_args()
  VERBOSE = args.verbose

  process(args)
  sys.exit(0)

if __name__ == '__main__':
  main()
