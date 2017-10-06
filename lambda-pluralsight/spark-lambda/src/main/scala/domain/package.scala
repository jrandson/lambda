package object domain {
  case class Activity(timestamp_hour:Long,
                      refereeer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
}
