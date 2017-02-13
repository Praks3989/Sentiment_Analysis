
Note: 1) All input and output paths refer to paths on HDFS
      2) Jars are provided in the respective directories.
      3) Training data provided in zip
	  4) Link to report: http://webpages.uncc.edu/gdhamdhe/
	
1) NaiveBayes Classifier
	a)Running NaiveBayes on Training input
		To compile Classes and buld jar Use following instructions
		Go to Directory NaiveBayes/train and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf NaiveBayes.jar -C build/ .
		$ hadoop jar NaiveBayes.jar  input output  user_specified_tweet
	b)Running NaiveBayes to classify input
		To compile Classes and buld jar Use following instructions
		Go to Directory NaiveBayes/classify and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf NaiveBayes.jar -C build/ .
		$ hadoop jar NaiveBayes.jar  input output
			Note: Give output of training data as input to classify data.

2) TFIDF based Nearest N - bag of words
	a)Running Training input
		To compile Classes and buld jar Use following instructions
		Go to Directory BagOfWords_Training and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf BOW_Train.jar -C build/ .
		$ hadoop jar BOW_Train.jar  input output  user_specified_tweet
	b)Running to classify input
		To compile Classes and buld jar Use following instructions
		Go to Directory BagOfWords_Classification and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf BOW_Classify.jar -C build/ .
		$ hadoop jar BOW_Classify.jar  input output
			Note: Give output of training data as input to classify data.

	

3) Afinn classification
	a) Training for Afinn
		Copy the folder provided in afinn_input on hdfs.
		
		To compile Classes and buld jar Use following instructions
		Go to Directory Afinn/train and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf AfinnClassify.jar -C build/ .
		$ hadoop jar AfinnTraining.jar afinnIp afinnTrained
	b) Running Afinn to classify input
		To compile Classes and buld jar Use following instructions
		Go to Directory Afinn/classify and run following instructions
		$ mkdir -p build
		$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
		$ jar -cvf AfinnClassify.jar -C build/ .
		$ hadoop jar AfinnClassify.jar afinnTrained AfinnResult happy excited
				here, afinnTrained is the directory which contains trained classifier of AFINN
					AfinnResult is the directory which will give result of classification
					happy excited - are query words
					
		