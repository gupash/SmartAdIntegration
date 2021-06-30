package com.imvu.smartad

object SmartAdModels {
  case class UpdateBearerToken(clientId: String, clientSecret: String)
  case class Token(token: String)
  // Please don't change the naming convention for the fields in this class,
  // it's required in this format by the Unmarshaller
  case class OAuthResp(access_token: String, token_type: String, expires_in: Int)
  case class Profile(profileId: String, segmentIdsToSet: List[Int])
  case class Profiles(profiles: Seq[Profile])
  case object GetToken
}
