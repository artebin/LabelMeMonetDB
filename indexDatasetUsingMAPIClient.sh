#!/bin/bash

#this script insert LabelMe in MonetDB using the MAPI mclient.

function recDescent() {
	path=`basename $1`;
	for i in $1/*
	do
		if [ -d "$i" ]; then
			recDescent "${i}"
		else
			filename=`basename "$i"`
			#echo $filename
			colname="$path"
			#echo $colname
			/home/nicolas/apps/MonetDB/MonetDB-bin/bin/mclient --language=xquery --input="$filename" --collection="$colname" < "$i"
		fi
	done
}

recDescent $1