import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

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
        set_master = false,
        use_cache = use_cache
      )
    }
  }

  val EPS = 1
  val MAX_ITER = 100
  val APP_NAME = "Twitter Nikita"


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
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val tag_likes =
      if (Files.exists(Paths.get(cache_path)) && config.use_cache){
        println("Loading cached data")
        sc.objectFile[(String, Int)](cache_path)
      }
      else {
        println("Computing data")
        val raw = spark.read.json(config.data_path)

        val raw_tweets = raw.selectExpr(
          "id",
          "text",
          "entities.hashtags.text as hashtags",
          "retweeted_status.favorite_count as retweet_fav_count",
          "user.favourites_count as user_fav_count",
          "user.statuses_count"
        )
          .where("id is not null")
          .as[RawTweet]


        def raw_to_tweet(raw: RawTweet): Tweet = raw match {
          case RawTweet(id, text, hashtags, retweet_fav_count, user_fav_count, statuses_count) => {
            val likes: Int =
              if (retweet_fav_count != null) retweet_fav_count.intValue()
              else Math.round(user_fav_count.intValue() / statuses_count.intValue())

            Tweet(id, text, hashtags, likes)
          }
        }

        val tweets = raw_tweets.map(raw_to_tweet)
        val tag_likes = tweets.flatMap(
          (tweet: Tweet) => {
            tweet.hashTags.map((tag: String) => (tag, tweet.likes))
          }
        ).rdd.persist()

        tag_likes.saveAsObjectFile(cache_path)
        tag_likes
      }

    val unique_tag_likes = tag_likes.reduceByKey(_ + _)
    val trending = unique_tag_likes.
      sortBy({ case (_, likes) => likes}, ascending = false)
      .take(20)
      .map({ case (tag, _) => tag })

//    Sort faster by removing the smallest elements first
//    val lower_bound = unique_tag_likes.takeSample(withReplacement = false, num = 100).map(pair => pair._2).sorted.reverse(20)
//    val trending = unique_tag_likes
//      .filter(pair => -pair._2 > lower_bound)
//      .sortBy(pair => -pair._2)
//      .take(20)
//      .map(pair => pair._1)

//    Output the trending tags
    println("Trending tags: ")
    trending.foreach({
      println
    })


//    Extract the tweets that have a trending hashtag
    val trending_set = trending.toSet
    val trending_tweets = tag_likes.filter({ case (tag, _) => trending_set contains tag})
      .persist()

    print("Trending tweets count: ")
    println(trending_tweets.count())

//    Initialize the clusters with sampled values
    var clusters = trending_set.map((hashtag) => {
      (
        hashtag,
        trending_tweets.filter({ case (tag, _) => tag == hashtag})
          .takeSample(withReplacement = false, num = 5)
          .map({ case (_, likes) => likes}).sorted
      )
    }).toMap

    var cluster_distance = 2 * EPS

    for (i <- 1 to MAX_ITER; if cluster_distance > EPS) {
  //    Assign a cluster to each tweet
      var clustered_tweets = trending_tweets.map({ case (tag, likes) => {
        val tag_clusters = clusters.get(tag) match {
          case Some(x) => x
          case None => Array()
        }

        val distances = tag_clusters.map((cluster_likes) => (cluster_likes - likes).abs)
        val idx = distances.indices.minBy(distances)
        ((tag, idx), likes)
      }})

  //    Get back the new clusters
      val clusters_with_ixd = clustered_tweets.mapValues(value => (value, 1)).reduceByKey {
          case ((sumL, countL), (sumR, countR)) =>
            (sumL + sumR, countL + countR)
        }.mapValues {
          case (sum, count) => sum / count
        }.collect()

      val new_clusters = clusters_with_ixd.map({ case ((tag, _), center) => {(tag, center)}})
        .groupBy({ case (tag, _) => tag }) // group by hashtag
        .mapValues((array) => array.map({ case (_, likes) => likes }).sorted) // keep only a list of centers
        .map(identity)

      cluster_distance = clusters.map({ case (tag, old_centers) => {
        val new_centers = new_clusters.getOrElse(tag, Array())
        val distance = old_centers.zip(new_centers)
          .map({ case (a, b) => (a - b).abs})
          .sum

        distance
      }}).sum

      print("Iteration ")
      print(i)
      print(" distance ")
      println(cluster_distance)

      clusters = new_clusters

    }

    println("Clusters")
    clusters.foreach({
      println
    })


  }
}
