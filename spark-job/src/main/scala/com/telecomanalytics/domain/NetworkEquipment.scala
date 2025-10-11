package com.telecomanalytics.domain

import java.sql.Timestamp

case class NetworkTelemetry(
    equipmentId: String,
    timestamp: Timestamp,
    equipmentType: String,
    operator: String,
    region: String,
    city: String,
    cpuUsage: Double,
    memoryUsage: Double,
    diskUsage: Double,
    networkThroughput: Double,
    activeConnections: Int,
    droppedCalls: Int,
    latency: Double,
    temperature: Double,
    powerConsumption: Double,
    status: String
)

case class TelecomSummary(
    operator: String,
    region: String,
    equipmentCount: Long,
    avgCpuUsage: Double,
    avgThroughput: Double,
    totalConnections: Long
)
