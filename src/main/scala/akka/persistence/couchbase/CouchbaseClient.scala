package akka.persistence.couchbase

import com.couchbase.client.java.{Bucket, Cluster, CouchbaseCluster}
import com.couchbase.client.java.auth.PasswordAuthenticator
import com.couchbase.client.java.env.CouchbaseEnvironment

trait CouchbasePersistencyClientContainer {
  def client = new CouchbasePersistencyClient {}
}

trait CouchbasePersistencyClient {
  private[couchbase] def createCluster(environment: CouchbaseEnvironment, nodes: java.util.List[String]): Cluster = {
    CouchbaseCluster.create(environment, nodes)
  }

  private[couchbase] def openBucket(cluster: Cluster, username: Option[String], bucketName: String, bucketPassword: Option[String]): Bucket = {
    username match {
      case None =>
        cluster.openBucket(bucketName, bucketPassword.orNull)
      case Some(user) =>
        cluster.authenticate(new PasswordAuthenticator(user, bucketPassword.orNull))
        cluster.openBucket(bucketName)
    }

  }
}