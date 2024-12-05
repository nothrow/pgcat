use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use log::debug;
use sqlparser::ast::Statement;

use crate::{
    errors::Error, messages::command_complete, plugins::{Plugin, PluginOutput}, query_router::QueryRouter
};

pub struct IgnoreSets<'a> {
    pub enabled: bool,
    pub ignored: &'a Vec<String>,
}

#[async_trait]
impl<'a> Plugin for IgnoreSets<'a> {
    async fn run(
        &mut self,
        _: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        if ast.is_empty() {
            debug!("Ignoring empty query");
            return Ok(PluginOutput::Allow);
        }

        let mut result = BytesMut::new();

        for q in ast {
            // Normalization
            let q = q.to_string().to_ascii_lowercase();

            for target in self.ignored.iter() {
                if q.contains(&format!("set {}", target)) {
                    debug!("Ignoring query: {}", q);
                    
                    result.put(command_complete("SET"));
                }
            }
        }

        if !result.is_empty() {
            result.put_u8(b'Z');
            result.put_i32(5);
            result.put_u8(b'I');

            return Ok(PluginOutput::Intercept(result));
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}
