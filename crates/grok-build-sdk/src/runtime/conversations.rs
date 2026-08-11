// Copyright 2026 OriginGame contributors
// Licensed under the Apache License, Version 2.0.

//! The typed route to the three conversation tools.
//!
//! The agent reaches them as tools on the `conversation` mount; a Host reaches
//! the same validated contract here, which is how it exercises create/read/send
//! without a model in the loop. Both routes run the same validation and the
//! same fail-closed checks on the delegate's answer.

use crate::*;

impl Runtime {
    fn conversation_delegate(&self) -> Result<&Arc<dyn ConversationDelegate>, Error> {
        self.conversation
            .as_ref()
            .ok_or_else(|| ConversationError::NoDelegate.into())
    }

    /// Creates an ordinary durable conversation in a named project through the
    /// installed delegate and returns the created identity. Nothing about the
    /// caller's Session is coupled to it.
    pub async fn create_conversation(
        &self,
        caller: &SessionId,
        request: ConversationCreate,
    ) -> Result<ConversationIdentity, Error> {
        Ok(dispatch_create(self.conversation_delegate()?, caller, request).await?)
    }

    /// Returns the Host's bounded distillate of another conversation. The raw
    /// transcript never crosses this boundary.
    pub async fn read_conversation(
        &self,
        caller: &SessionId,
        request: ConversationRead,
    ) -> Result<ConversationDigest, Error> {
        Ok(dispatch_read(self.conversation_delegate()?, caller, request).await?)
    }

    /// Delivers one peer message through the Host's one send queue and returns
    /// its acceptance. Repeating a delivery key is the same delivery.
    pub async fn send_to_conversation(
        &self,
        caller: &SessionId,
        request: ConversationSend,
    ) -> Result<ConversationAcceptance, Error> {
        Ok(dispatch_send(self.conversation_delegate()?, caller, request).await?)
    }

    /// Runs one declared conversation tool by name, exactly as the agent-facing
    /// mount does.
    pub async fn invoke_conversation_tool(
        &self,
        caller: &SessionId,
        call: ConversationToolCall,
    ) -> Result<ConversationToolOutcome, Error> {
        Ok(dispatch_tool_call(self.conversation_delegate()?, caller, call).await?)
    }
}
