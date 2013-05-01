#!/bin/bash
echo "disable 'graph_bfs'"
echo "drop 'graph_bfs'"
echo "create 'graph_bfs','neighbourhood','access'"
for((i=0;i<$1;i++))
do
	echo -n '' > tmp
	NEIGHBOR=`expr $RANDOM % $1 + 1`
	for((j=0;j<$NEIGHBOR;j++))
	do
		echo "`expr $RANDOM % $1` " >> tmp
	done
	echo -n "put 'graph_bfs', '$i', 'neighbourhood:', '"
	echo -n `sort tmp | uniq | tr -d '\n'`
	echo "'"
done
rm tmp
