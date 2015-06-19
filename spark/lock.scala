/* In order to refresh my memory on spark, I quickly translated 
 * Peter Norvig's Bike Lock script from Python to Scala
 * http://nbviewer.ipython.org/url/norvig.com/ipython/Fred%20Buns.ipynb
 */

// The spindles
val spindles = List("SPHMTWDLFB","LEYHNRUOAI","ENMLRTAOSK","DSNMPYLKTE")

// Need to remain as strings to make the reduce simpler. Need to filter nulls
val lock = spindles.map(str => sc.parallelize[String](str.split("").filter(c => c != "")))

// Cartesian product to get all the strings - keep flattened via map concat
val combinations = lock.reduce((a,b)=>a.cartesian(b).filter(x=>x._1 != "").map(x=>"" + x._1 + x._2))

// Load in dictionary of words
val dict = sc.textFile("s3://my-learning/words4.txt")

// Compute the intersection
val validWords = combinations.intersection(dict)

// Result
println(s"${validWords.count()} words out of ${combinations.count()} combinations")
