#!/bin/bash
for((i=0;i<$1;i++))
do
	for((j=0;j<$1;j++))
	do
		if [ $i = $j ]; then
			echo -n "0 "
		else
			echo -n "$RANDOM "
		fi
	done
	echo
done
