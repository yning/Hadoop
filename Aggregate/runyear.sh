hadoop jar /usr/share/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar\
        -files stopwords.txt\
        -input 'data/'\
        -output 'test-year'\
        -file SRC/YearMapper.py\
        -file SRC/YearReducer.py\
        -mapper 'YearMapper.py'\
        -reducer 'YearReducer.py'
