#!/bin/bash
START=`date +%s`
$1 > /dev/null 2>&1
END=`date +%s`
expr $END - $START
