use redis::{RedisResult, ToRedisArgs};

#[derive(Clone)]
pub struct FakeMultiplexer {}

impl FakeMultiplexer {
    pub async fn get<K: ToRedisArgs>(&mut self, _key: K) -> RedisResult<Option<Vec<u8>>> {
        Ok(None)
    }

    pub async fn set<K: ToRedisArgs, V: ToRedisArgs>(
        &mut self,
        _key: K,
        _value: V,
    ) -> RedisResult<Option<Vec<u8>>> {
        Ok(None)
    }
}
