package com.xm4399.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by hemintang on 17-3-17.
  */
object DateKeyUtil {

  private val FORMAT = "yyyyMMdd"

  def getDatekey(daysAgo: Int): String = {
    val sdf = new SimpleDateFormat(FORMAT)
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1 * daysAgo)
    val dateKey = sdf.format(calendar.getTime)
    dateKey
  }
}
