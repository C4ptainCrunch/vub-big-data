import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path
import scala.util.Try

case class RawTweet(id: BigInt,
                    text: String,
                    hashtags: Array[String],
                    retweet_fav_count: BigInt,
                    user_fav_count: BigInt,
                    statuses_count: BigInt)

case class Tweet(id: BigInt,
                 text: String,
                 hashTags: Array[String],
                 likes: Int)

case class Settings(cache_path: String,
                    data_path: String,
                    set_master: Boolean,
                    use_cache: Boolean)



object Twitter {
  type HashTag = String
  type LikesCount = Int
  type ClusterCenter = Int
  type ClusterIndex = Int
  type TweetCount = Int


  def get_config(): Settings = {
    val use_cache = true
    if(scala.util.Properties.envOrElse("DEV", "0") == "1") {
      println("Running in dev/standalone mode")
      val root = "/home/nikita/Documents/Vub/2017-2018/BC/bd-project/"
      Settings(
        cache_path = root + "tag_likes.data/",
        data_path =  root + "reducedTweetsRaw/",
        set_master = true,
        use_cache = use_cache
      )
    } else {
      println("Running in cluster mode")
      Settings(
        cache_path = "/exports/home/nimarcha/tag_likes.data/",
        data_path = "/data/twitter/tweetsraw",
//        data_path = "/data/twitter/reducedTweetsRaw/",
        set_master = false,
        use_cache = use_cache
      )
    }
  }

  val EPS = 100
  val K = 5
  val MAX_ITER = 100
  val APP_NAME = "Twitter Nikita"
  val TOPN = 20

  def raw_to_tweet(raw: RawTweet): Tweet = raw match {
    case RawTweet(id, text, hashtags, retweet_fav_count, user_fav_count, statuses_count) => {
      val likes: Int =
        if (retweet_fav_count != null) retweet_fav_count.intValue()
        else Math.round(user_fav_count.intValue() / statuses_count.intValue())

      Tweet(id, text, hashtags, likes)
    }
  }

  def get_tweets_from_disk(spark: SparkSession , data_path: String) = {
    import spark.implicits._ // we need this to cast Dataframe to Dataset[RawTweet] with .as[X]

    val raw: DataFrame = spark.read.json(data_path)

    val raw_tweets: Dataset[RawTweet] = raw
      // Select only useful fields
      .selectExpr(
        "id",
        "text",
        "entities.hashtags.text as hashtags",
        "retweeted_status.favorite_count as retweet_fav_count",
        "user.favourites_count as user_fav_count",
        "user.statuses_count"
      )
      // Only keep tweets and not other actions (user likes, ...)
      .where("id is not null")
      .as[RawTweet]

    // convert to Tweet (mostly an heuristic for the # of likes)
    raw_tweets.map(raw_to_tweet)
  }

  def basicGetTrending(unique_tag_likes: RDD[(HashTag, LikesCount)]): Array[HashTag] = {
    unique_tag_likes
        .sortBy({ case (_, likes) => likes }, ascending = false)
      .take(TOPN)
      .map({ case (tag, _) => tag })
  }

  def partitionedGetTrending(unique_tag_likes: RDD[(HashTag, LikesCount)]): Array[HashTag] = {
    unique_tag_likes
      .mapPartitions(partition => {
        // Inspired from https://stackoverflow.com/a/5675204
        // We should not use a list as we have to sort it every time
        // something like https://docs.python.org/3.6/library/bisect.html
        // would be better
        partition.foldLeft(List[(HashTag, LikesCount)]()) { (l, n) => {
          (n :: l).sortWith(_._2 > _._2).take(TOPN)
        }
        }.iterator
      })
      .sortBy({ case (_, likes) => likes }, ascending = false)
      .take(TOPN)
      .map({ case (tag, _) => tag })
  }

  def stdlibGetTrending(unique_tag_likes: RDD[(HashTag, LikesCount)]): Array[HashTag] = {
    unique_tag_likes
      .top(TOPN)(Ordering.by[(HashTag, LikesCount), LikesCount](_._2))
      .map({ case (tag, _) => tag })
  }

  def fastGetTrending(unique_tag_likes: RDD[(HashTag, LikesCount)]): Array[HashTag] = {
    // Sort faster by removing the most insignificant hashtags first
    // We take 100 random hashtags and take the minimum like count. This like count is
    // obviously > than the like count of the 20th trending hashtag
    val lower_bound: LikesCount = unique_tag_likes.takeSample(withReplacement = false, num = 100)
      .map(pair => pair._2)
      .sorted
      .reverse(TOPN)

    unique_tag_likes
      .filter({ case (_, likes) => likes > lower_bound})
      .sortBy({ case (_, likes) => likes})
      .take(TOPN)
      .map({ case (tag, _) => tag })
  }

  def getHashtagsLikes(sc: SparkContext, spark: SparkSession, config: Settings) = {
    import spark.implicits._ // Needed to serialize case classes

    if (Files.exists(Paths.get(config.cache_path)) && config.use_cache){
      println("Loading cached data")
      sc.objectFile[(HashTag, LikesCount)](config.cache_path)
    }
    else {
      println("Loading raw tweet data")

      val tweets: Dataset[Tweet] = get_tweets_from_disk(spark, config.data_path)
      val tag_likes = tweets.flatMap(
        (tweet: Tweet) => {
          tweet.hashTags.map((tag: HashTag) => (tag, tweet.likes))
        }
      ).rdd

      if(config.use_cache) {
        val path: Path = Path (config.cache_path)
        Try(path.deleteRecursively())

        tag_likes.saveAsObjectFile(config.cache_path) // Cache on disk for future runs
      }

      tag_likes
    }
  }


  def getInitialClusters(trending_set: Set[HashTag], trending_tweets: RDD[(HashTag, LikesCount)]): Map[HashTag, Array[ClusterCenter]] = {
    trending_set
      .map((hashtag) => {(
        hashtag,
        trending_tweets
          // Keep only tweets with the current hashtag
          .filter({ case (tag, _) => tag == hashtag})
          // Take 5
          .takeSample(withReplacement = false, num = K)
          // Keep only the LikesCount
          .map({ case (_, likes) => likes})
          .sorted
      )})
      .toMap
  }

  def main(args: Array[String]): Unit = {

    val config = get_config()

    val conf = new SparkConf()
    conf.setAppName(APP_NAME)
    if (config.set_master) {
      conf.setMaster("local[*]")
    }
    val sc = new SparkContext(conf)
    val cache_path = config.cache_path

    val spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") // Needed to serialize case classes


    val tag_likes: RDD[(HashTag, LikesCount)] = getHashtagsLikes(sc, spark, config)
      .persist()
    val unique_tag_likes: RDD[(HashTag, LikesCount)] = tag_likes.reduceByKey(_ + _)
    val trending: Array[HashTag] = stdlibGetTrending(unique_tag_likes)

    println("Trending tags: " + trending.mkString(","))


    // Extract the tweets that have at least a trending hashtag
    val trending_set: Set[HashTag] = trending.toSet
    val trending_tweets: RDD[(HashTag, LikesCount)] = tag_likes
      .filter({ case (tag, _) => trending_set contains tag})
      .persist()

    print("Amount of tweets with a trending hashtag: ")
    println(trending_tweets.count())

    // Initialize the clusters with sampled values
    var clusters: collection.Map[HashTag, Array[ClusterCenter]] = getInitialClusters(trending_set, trending_tweets)

    var cluster_sizes: collection.Map[HashTag, Array[Int]] = collection.Map()

    // Make a big cluster_distance so we enter the  at least once
    var cluster_distance = 2 * EPS

    for (i <- 1 to MAX_ITER; if cluster_distance > EPS) {

      // Assign each tweet the closest cluster center
      var clustered_tweets: RDD[((HashTag, ClusterIndex), LikesCount)] = trending_tweets
        .map({ case (tag, likes) => {
          // Get the clusters list for this hashtag
          val tag_clusters: Array[ClusterCenter] = clusters.get(tag) match {
            case Some(x) => x
            case None => Array()
          }

          val distances = tag_clusters.map((cluster_likes) => (cluster_likes - likes).abs)
          val idx: ClusterIndex = distances.indices.minBy(distances)
          ((tag, idx), likes)
        }})

      // Get back the new clusters
      val ungrouped_clusters: Array[(HashTag, (Int, Int))] = clustered_tweets
        .mapValues(value => (value, 1))
        .reduceByKey {
          case ((sumL, countL), (sumR, countR)) =>
            (sumL + sumR, countL + countR)
        }.collect // we have only 100 values (5 clusters * 20 hashtags)
        .map({ // remove cluster index
          case ((tag, _), (sum, count)) => (tag, (sum, count))
        })

      val new_clusters_with_count: Map[HashTag, Array[(ClusterCenter, TweetCount)]] = ungrouped_clusters
        .map({ // compute mean likes
          case (tag, (sum, count)) => (tag, (sum / count, count))
        })
        .groupBy({
          case (tag, _) => tag
        })
        // transform Array[(Tag, (Center, Count))] to Array[(Center, Count)]
        .mapValues((array) => array.map({case (_, (center, count)) => (center, count)}))
        .mapValues((array) => array.sorted)
        // mapValues is not serializable, so we map it with identity it to
        // make it serializable (see https://stackoverflow.com/a/32910318)
        .map(identity)

      val new_clusters: Map[HashTag, Array[ClusterCenter]] = new_clusters_with_count
        // keep only the centers
        .mapValues((array) => array.map({case (center, _) => center}))
        .map(identity)

      cluster_sizes = new_clusters_with_count
        // keep only the sizes
        .mapValues((array) => array.map({case (_, count) => count}))
        .map(identity)


      cluster_distance = clusters.
        map({ case (tag, old_centers) => {
          val new_centers = new_clusters.getOrElse(tag, Array())
          val distance = old_centers.zip(new_centers)
            .map({ case (a, b) => (a - b).abs})
            .sum

          distance
        }})
        .sum

      if(i % 10 == 1 || cluster_distance <= EPS){
        println("Iteration " + i.toString + " distance " + cluster_distance.toString)
      }

      clusters = new_clusters
    }

    println("Clusters: ")
    clusters.foreach({case (hashtag, centers) => {
      println(hashtag.toString + " (centers) " + centers.mkString(","))
      println(hashtag.toString + " (sizes) " + cluster_sizes(hashtag).mkString(","))
    }})

  }
}
