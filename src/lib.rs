/* Redis Glue is provides abstractions over single and cluster mode Redis interactions
 * Copyright 2021 Aravinth Manivannan <realaravinth@batsense.net>
 *
 * Licensed under the Apache License, Version 2.0 (the "License") or MIT
 */

//! Redis Client/Connection manager that can handle both single and clustered Redis Instances
use std::cell::RefCell;
use std::rc::Rc;

use redis::cluster::ClusterClient;
use redis::Client;
use redis::FromRedisValue;
use redis::RedisResult;
use redis::{aio::Connection, cluster::ClusterConnection};

pub use redis;

/// Client configuration
#[derive(Clone)]
pub enum RedisConfig {
    /// Redis server URL
    Single(String),
    /// List of URL of Redis nodes in cluster mode
    Cluster(Vec<String>),
}

impl RedisConfig {
    /// Create Redis connection
    pub fn connect(&self) -> RedisClient {
        match self {
            Self::Single(url) => {
                let client = Client::open(url.as_str()).unwrap();
                RedisClient::Single(client)
            }
            Self::Cluster(nodes) => {
                let cluster_client = ClusterClient::open(nodes.to_owned()).unwrap();
                RedisClient::Cluster(cluster_client)
            }
        }
    }
}

/// Redis connection - manages both single and clustered deployments
#[derive(Clone)]
pub enum RedisConnection {
    Single(Rc<RefCell<Connection>>),
    Cluster(Rc<RefCell<ClusterConnection>>),
}

impl RedisConnection {
    #[inline]
    /// Get client. Uses interior mutability, so lookout for panics
    pub fn get_client(&self) -> Self {
        match self {
            Self::Single(con) => Self::Single(Rc::clone(&con)),
            Self::Cluster(con) => Self::Cluster(Rc::clone(&con)),
        }
    }
    #[inline]
    /// execute a redis command against a [Self]
    pub async fn exec<T: FromRedisValue>(&self, cmd: &mut redis::Cmd) -> redis::RedisResult<T> {
        match self {
            RedisConnection::Single(con) => cmd.query_async(&mut *con.borrow_mut()).await,
            RedisConnection::Cluster(con) => cmd.query(&mut *con.borrow_mut()),
        }
    }

    pub async fn ping(&self) -> bool {
        if let Ok(redis::Value::Status(v)) = self.exec(&mut redis::cmd("PING")).await {
            v == "PONG"
        } else {
            false
        }
    }
}

#[derive(Clone)]
/// Client Configuration that can be used to get new connection shuld [RedisConnection] fail
pub enum RedisClient {
    Single(Client),
    Cluster(ClusterClient),
}

/// A Redis Client Object that encapsulates [RedisClient] and [RedisConnection].
/// Use this when you need a Redis Client
#[derive(Clone)]
pub struct Redis {
    _client: RedisClient,
    connection: RedisConnection,
}

impl Redis {
    /// create new [Redis]. Will try to connect to Redis instance specified in [RedisConfig]
    pub async fn new(redis: RedisConfig) -> RedisResult<Self> {
        let (_client, connection) = Self::connect(redis).await?;
        let master = Self {
            _client,
            connection,
        };
        Ok(master)
    }

    /// Get client to do interact with Redis server.
    ///
    /// Uses Interior mutability so look out for panics
    pub fn get_client(&self) -> RedisConnection {
        self.connection.get_client()
    }

    async fn connect(redis: RedisConfig) -> RedisResult<(RedisClient, RedisConnection)> {
        let redis = redis.connect();
        let client = match &redis {
            RedisClient::Single(c) => {
                let con = c.get_async_connection().await?;
                RedisConnection::Single(Rc::new(RefCell::new(con)))
            }
            RedisClient::Cluster(c) => {
                let con = c.get_connection()?;
                RedisConnection::Cluster(Rc::new(RefCell::new(con)))
            }
        };
        Ok((redis, client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn ping_works() {
        let r = Redis::new(RedisConfig::Single("redis://127.0.0.1".into()))
            .await
            .unwrap();
        assert!(r.get_client().ping().await);
    }

    #[actix_rt::test]
    async fn exec_works() {
        const VAR: (&str, &str) = ("testval", "4");
        let r = Redis::new(RedisConfig::Single("redis://127.0.0.1".into()))
            .await
            .unwrap();
        let _set: () = r
            .get_client()
            .exec(redis::cmd("SET").arg(&[VAR.0, VAR.1]))
            .await
            .unwrap();

        let get: String = r
            .get_client()
            .exec(redis::cmd("GET").arg(&[VAR.0]))
            .await
            .unwrap();

        assert_eq!(&get, VAR.1);
    }
}
