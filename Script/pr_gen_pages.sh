#!/bin/bash
mkdir $2 > /dev/null 2>&1
for((i=0;i<$1;i++))
do
	echo -n '<html><body>' > $2/$i.html
	LINK=$1
	for((j=0;j<$LINK;j++))
	do
		LINE=$j
		if [ $LINE != $i ]
		then
			echo -n "<a href=\"$LINE.html\">$LINE</a><br/>" >> $2/$i.html
		fi
	done
	echo -n '</body></html>' >> $2/$i.html
	echo "Generated page $i" >> /dev/stderr
done
