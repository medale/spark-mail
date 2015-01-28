#!/bin/bash

#
# This script is to rename the unpacked enron email dataset files. Currently, the email files 
# have filenames that end with '.' which is not compatible with Java + Windows.
#
# This is intended to be run in CYGWIN (cygwin.org) on Windows. 
#
# Run the script with your maildir as the first argument.
#
# Example:
#
#   cygwin> ./enron_windows_rename_SLOW.sh /cygdrive/c/Users/you/data/enron_dataset/maildir
# 

rename() {
  local file=$1

  if [ -f $file ]; then
    if [[ "$file" =~ ^.*\.$ ]]; then
	  mv $file "${file}txt"
	fi
  
  elif [ -d $file ]; then
    for child in `ls $file`; do
	  rename "$file/$child"
	done
  
  fi
}

rename $1

echo "done"
