#!/bin/sh
set -eu
for i in $(/home/work/fifo/bin/fifoc $1:9000 fifo_list | sed 1d | awk '{print $1}'); do
    /home/work/fifo/bin/fifoc $1:9000 fifo_stat $i all;
done | sed 's/^  /Dest /' | sed 's/([0-9.]*)//' | awk '($1=="Fifo"){if(first){printf("\n");first=0;}printf("Fifo %s %s", $3, $4);first=1}($1=="Port"){printf(" %d", $3)}($1=="Dest"&&NF>=9){if(first){printf("\n");first=0;}print "Dest "$2,$3,$9}'
