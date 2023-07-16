use protobuf::Chars;

use crate::{
    redis_request::{command, Command, RequestType},
    ClienUsageError, ClientUsageResult,
};

fn command_name(command: &Command) -> Result<Option<&'static str>, ClienUsageError> {
    let request_enum = command
        .request_type
        .enum_value_or(RequestType::InvalidRequest);
    match request_enum {
        RequestType::CustomCommand => Ok(None),
        RequestType::GetString => Ok(Some("GET")),
        RequestType::SetString => Ok(Some("SET")),
        _ => Err(ClienUsageError::InternalError(format!(
            "Received invalid request type: {:?}",
            command.request_type
        ))),
    }
}

pub struct CommandArgs {
    command_name: Option<&'static str>,
    other_args: Vec<Chars>,
}

#[derive(Clone)]
pub struct ArgsIter<'a> {
    command_args: &'a CommandArgs,
    index: usize,
}

impl<'a> ExactSizeIterator for ArgsIter<'a> {
    fn len(&self) -> usize {
        self.command_args.other_args.len()
            + if self.command_args.command_name.is_some() {
                1
            } else {
                0
            }
            - self.index
    }
}

impl<'a> Iterator for ArgsIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.len() == 0 {
            return None;
        }
        self.index += 1;
        let index = self.index;
        if let Some(cmd_name) = self.command_args.command_name {
            if index == 1 {
                return Some(cmd_name.as_bytes());
            }
            return Some(self.command_args.other_args[index - 2].as_bytes());
        }

        return Some(self.command_args.other_args[index - 1].as_bytes());
    }
}

fn args_vec(command: Command) -> ClientUsageResult<Vec<Chars>> {
    match command.args {
        Some(command::Args::ArgsArray(args_vec)) => Ok(args_vec.args),
        Some(command::Args::ArgsVecPointer(pointer)) => {
            let string_vec = *unsafe { Box::from_raw(pointer as *mut Vec<String>) };
            let char_vec = string_vec.into_iter().map(|str| str.into()).collect();
            Ok(char_vec)
        }
        None => Err(ClienUsageError::InternalError(
            "Failed to get request arguemnts, no arguments are set".to_string(),
        )),
    }
}
impl CommandArgs {
    pub fn iter(&self) -> ArgsIter {
        ArgsIter {
            command_args: self,
            index: 0,
        }
    }
}

pub fn command_args(command: Command) -> ClientUsageResult<CommandArgs> {
    let command_name = command_name(&command)?;
    let other_args = args_vec(command)?;

    Ok(CommandArgs {
        command_name,
        other_args,
    })
}
