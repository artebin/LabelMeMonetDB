#!/bin/bash

#author Nicolas James <nicolas.james@gmail.com> [[http://njames.trevize.net]]
#normalizeAnnotationsFilenames.sh - 2009.01.15

#this script replace the URL-encoding of files names for the space character %2520
#by the character _
#MonetDB doesn't support URL encoding %2520.

function recDescent() {
	path=`basename $1`;
	for i in $1/*
	do
		if [ -d "$i" ]; then
			recDescent "${i}"
		else
			newname=`echo "${i}" | sed "s/%2520/_/g"`
			mv ${i} ${newname}
			echo "${i} ${newname}"
		fi
	done
}

recDescent $1