#!/bin/bash
mpicc -o f1 Analysis.c -lm

touch output.txt
touch plot.txt
flag=0
for p in 1,2
do
	for ppn in 1,2,4
	do
		python script.py 1 $p $ppn
		pr=$((p*ppn))

		if [ $flag==0 ]
		then
			mpirun -np pr -f hostfile ./f1 tdata.csv >> output.txt
			tail -n 1 output.txt >> plot.txt
		else
			rm output.txt
			touch output.txt
			mpirun -np pr -f hostfile ./f1 tdata.csv >> output.txt
			tail -n output.txt >> plot.txt

		fi
	done
done

