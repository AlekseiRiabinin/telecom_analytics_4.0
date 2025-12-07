package com.telecomanalytics.models

case class SmartMeterRecord(
    meter_id: String,
    timestamp: String,
    energy_consumption: Double,
    voltage: Double,
    current_reading: Double,
    power_factor: Double,
    frequency: Double
)
