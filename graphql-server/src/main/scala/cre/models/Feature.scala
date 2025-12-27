package cre.models

/** 
  * Domain model for a spatial feature.
  *
  * @param id   Unique identifier of the feature
  * @param risk Risk score computed by graph engine / analytics
  */
case class Feature(
  id: String,
  risk: Double
)

