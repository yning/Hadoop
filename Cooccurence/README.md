## Shell Files:

- runall.sh: run all steps by one file on all datasets.

- runpairs.sh: test on the smallest dataset for pairs approach to compute frequency of co-occurrence

- runstripes.sh: test on the smallest dataset for stripes approach to compute frequency of co-occurrence

- runcond.sh: test on the smallest dataset to compute conditional probability p(a|b) of each pair (a,b)

- runlift.sh: test on the smallest dataset to compute lift(a,b) of each movie pair (a,b)

## Input Files:

- Required files:
ratings-100l, ratings-1m, ratings-10m these are ratings files of different dataset(Needed in each program)
movies100k, movies1m, movies10m are movie info of different dataset(Needed in conditional probability)
uinfo100k, uinfo1m, uinfo10m(Needed in lift computation)

## Source Code:

- PairsOccurrence.java: Using "pairs" approach to compute frequency of co-occurrence for every pair of movies that recieve a high 
ranking from the same user.
    - Input: userid::movieid::ratings::timestamp
    - Output: <movie1_id1, movie2_id	frequency>
    - Final output: <movie1_name, movie2_name	frequency>


- StripesOccurrence.java: Using "stripes" approach to compute frequency of co-occurrence for every pair of movies that recieve a high 
ranking from the same user.
    - Input: userid::movieid::ratings::timestamp
    - Output: <movie1_id1, movie2_id	frequency>
    - Final output: <movie1_name, movie2_name	frequency>

- PairsConditional.java: Compute conditional probability p(a|b) where a,b are both movies. 
In the mean time of collecting the co-occurence of each pair, also output the count for each movie. In next mapreduce job, after mappers collecting the cout of each pair and each movie, reducers output the conditional probability p(a|b)= count(b,a)/count(b).

I add one more job to sort on conditional probabily and read movie names from distributed file and put the names in the output.
- Input: userid::movieid::ratings::timestamp
- Output: <movie1_name, movie2_name	conditional_probability>


PairsLift.java:
Compute lift(a,b) = p(a|b)/p(a) where a,b are both movies.
After I get the conditional probability, output the probability of "a" in the same mapreduce job when computing probability of p(b|a).

In next mapreduce job of computing lift, mappers collect the probability of "a" and conditional probability p(a|b). reducers are responsible for computing the lift given the p(a|b) and p(a). At last, output the pairs with lift greater than 1.2.

Input: userid::movieid::ratings::timestamp
Output: <movie1_name, movie2_name	lift>
