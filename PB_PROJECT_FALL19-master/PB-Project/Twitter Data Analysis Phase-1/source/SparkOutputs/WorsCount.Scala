//Creating RDD
val htFile = sc.textFile("C:/Users/Vamsi Draksharam/Desktop /hashtags.txt")
 
val htWords = htFile.flatMap(line => line.split(" "))

// Summation of occurrences by each word
val htCounts = htWords.map(word => (word, 1)).reduceByKey(_ + _)

// Sorting the words by count
val htSortedCount = ht.sortBy(-_._2)

//saves output to the file
htSortedCount.saveAsTextFile("C:/Users/Vamsi Draksharam/Desktop/hashtagsoutput")


val urlsFile = sc.textFile("C:/Users/Vamsi Draksharam/Desktop/urls.txt")
val urlsWords = urlsFile.flatMap(line => line.split(" "))
val urlsCounts = urlsWords.map(word => (word, 1)).reduceByKey(_ + _)
val urlsSortedCount = urlsCounts.sortBy(-_._2)
urlsSortedCount.saveAsTextFile("C:/Users/Vamsi Draksharam/Desktop /urlsoutput")

