import java.lang.String.valueOf
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Random

object BookingEventGenerator {
  private lazy val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private lazy val rd = new Random()

  def generateBookingEvent(id: Int): String = {
    val srch_ci = new Date(rd.nextLong()).toLocalDate
    val srch_co = srch_ci.plusDays(rd.nextInt(70))
    valueOf(id).concat(",")
      .concat(dateFormatter.format(new Date(rd.nextLong()))).concat(",") // date_time
      .concat(valueOf(rd.nextInt(50))).concat(",") // site_name
      .concat(valueOf(rd.nextInt(7))).concat(",") // posa_continent
      .concat(valueOf(rd.nextInt(195))).concat(",") // user_location_country
      .concat(valueOf(rd.nextInt(500))).concat(",") // user_location_region
      .concat(valueOf(rd.nextInt(60000))).concat(",") // user_location_city
      .concat(valueOf(rd.nextFloat())).concat(",") // orig_destination_distance
      .concat(valueOf(rd.nextInt(10000))).concat(",") // user_id
      .concat(valueOf(rd.nextInt(2))).concat(",") // is_mobile
      .concat(valueOf(rd.nextInt(2))).concat(",") // is_package
      .concat(valueOf(rd.nextInt(10))).concat(",") // channel
      .concat(dateFormatter.format(Date.valueOf(srch_ci))).concat(",") // srch_ci
      .concat(dateFormatter.format(Date.valueOf(srch_co))).concat(",") // srch_co
      .concat(valueOf(rd.nextInt(10))).concat(",") // srch_adults_cnt
      .concat(valueOf(rd.nextInt(20))).concat(",") // srch_children_cnt
      .concat(valueOf(rd.nextInt(50))).concat(",") // srch_rm_cnt
      .concat(valueOf(rd.nextInt(500))).concat(",") // srch_destination_id
      .concat(valueOf(rd.nextInt(10))).concat(",") // srch_destination_type_id
      .concat(valueOf(rd.nextInt(7))).concat(",") // hotel_continent
      .concat(valueOf(rd.nextInt(195))).concat(",") // hotel_country
      .concat(valueOf(rd.nextInt(2000))) // hotel_market
  }

}
