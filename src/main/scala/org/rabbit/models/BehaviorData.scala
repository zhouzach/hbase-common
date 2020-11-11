package org.rabbit.models

case class BehaviorData(uid: String,
                        time: String,
                        phoneType: String,
                        clickCount: Int)

case class BehaviorInfo(uid: Long,
                        time: String,
                        phoneType: String,
                        clickCount: Int)

case class UserData(uid: String,
                    sex: String,
                    age: Int,
                    created_time: String)



