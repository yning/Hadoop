#! /usr/bin/env python
import sys
'''
In this case, compute the average appearance of a word per year which is (all_count)/(#_of_different_years)
For example:
Input:
ad 1949 1
ad 1950 1
ad 1950 1
ad 1950 1
ad 1950 1
ad 1950 1
Output:
ad 3.0

Code starts:
(last_key, all_year, sum, count) = ("",[],0.0,0.0)

for line in sys.stdin:
	lint = line.strip()
	(key, year, once) = line.split('\t')
	if last_key != key and last_key !="":
		count = float(len(all_year))
		print '%s\t%s' % (last_key, str(sum/count))
		sum = 0.0
		count = 0.0
	
	if year not in all_year:
		all_year.append(year)
		
	last_key = key
	sum += 1.0
#print '%s\t%s' % (last_key,  str(sum/count))
'''

'''
In this case, compute the frequency of a word per year. For example:
Input:
ad 1949 1
ad 1950 1
ad 1950 1
ad 1950 1
ad 1951 1
Output:
ad 1949 1
ad 1950 3
ad 1951 1

Code starts:
'''
(last_key, count_per_year) = ("",0)
for line in sys.stdin:
	lint = line.strip()
	(key, once) = line.split('\t')
	if (last_key != key and last_key !=""):
		print '%s\t%s' % (last_key, str(count_per_year))
		count_per_year = 0
	
	last_key = key
	count_per_year += 1
#print '%s\t%s\t%s' % (last_key, last_year, str(count_per_year))

