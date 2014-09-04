#create.py
from decimal import *

def create(filen, n, r):
	fp = open(filen, "w")
	for i in xrange(n):
		string = "INSERT " + r + " VALUES "
		string = string + str(i%3) + " "
		string = string + str(float(i)) + "\n"
		fp.write(string)

create("2.txt", 10, "r2")