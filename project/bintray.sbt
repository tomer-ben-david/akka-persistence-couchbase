resolvers += Resolver.url("bintray-productfoundry-releases", url("http://dl.bintray.com/tomer/"))(Resolver.ivyStylePatterns)

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0" exclude("org.slf4j", "slf4j-nop"))