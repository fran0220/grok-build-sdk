use sophon_sdk::{
    ApiBackend, CONVERSATION_TOOL_SERVER, ConversationAcceptance, ConversationCreate,
    ConversationDelegate, ConversationDelivery, ConversationDigest, ConversationError,
    ConversationId, ConversationIdentity, ConversationLabel, ConversationRead, ConversationSend,
    ConversationTitle, ConversationToolCall, ConversationToolOutcome, DigestFocus, Error,
    IdempotencyKey, InputSource, MAX_CONVERSATION_DIGEST_BYTES, MAX_PEER_MESSAGE_BYTES, ModelSpec,
    PeerMessage, ProjectName, Runtime, RuntimeConfig, RuntimeProfile, SessionConfig, SessionId,
    SessionLedger, conversation_tool_descriptors,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use xai_grok_test_support::MockInferenceServer;

fn runtime_config(root: &TempDir, endpoint: String) -> RuntimeConfig {
    RuntimeConfig {
        endpoint,
        api_key: "host-relay-bearer".into(),
        grok_home: root.path().join("grok"),
        session_storage: root.path().join("sessions"),
        models: vec![ModelSpec {
            id: "test-model".into(),
            context_window: 131_072,
            api_backend: ApiBackend::ChatCompletions,
            supports_reasoning: false,
            default_reasoning: None,
            reasoning_options: Vec::new(),
        }],
    }
}

fn session_config(cwd: PathBuf) -> SessionConfig {
    SessionConfig {
        cwd,
        model: "test-model".into(),
        reasoning: None,
        system_prompt: None,
        rules: None,
    }
}

fn conversation(value: &str) -> ConversationId {
    ConversationId::new(value).expect("conversation identity")
}

fn key(value: &str) -> IdempotencyKey {
    IdempotencyKey::new(value).expect("delivery key")
}

fn project(value: &str) -> ProjectName {
    ProjectName::new(value).expect("project name")
}

/// One honest Host: it creates conversations, distills a transcript it owns,
/// and runs a send queue that admits a delivery key exactly once.
#[derive(Default)]
struct PeerHost {
    created: Mutex<Vec<(String, ConversationCreate)>>,
    transcripts: Mutex<BTreeMap<String, String>>,
    running: Mutex<BTreeSet<String>>,
    delivered: Mutex<Vec<(String, ConversationSend)>>,
    accepted: Mutex<BTreeMap<String, ConversationDelivery>>,
}

impl PeerHost {
    fn with_transcript(conversation: &str, transcript: &str) -> Self {
        let host = Self::default();
        host.transcripts
            .lock()
            .unwrap()
            .insert(conversation.to_owned(), transcript.to_owned());
        host
    }

    fn mark_running(&self, conversation: &str) {
        self.running.lock().unwrap().insert(conversation.to_owned());
    }

    fn deliveries(&self) -> Vec<(String, ConversationSend)> {
        self.delivered.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ConversationDelegate for PeerHost {
    async fn create(
        &self,
        caller: &SessionId,
        request: ConversationCreate,
    ) -> Result<ConversationIdentity, ConversationError> {
        let mut created = self.created.lock().unwrap();
        created.push((caller.as_str().to_owned(), request.clone()));
        let identity = ConversationIdentity {
            conversation: conversation(&format!("thread-{}", created.len())),
            project: request.project,
            label: request
                .title
                .map(|title| ConversationLabel::new(title.as_str()).expect("label")),
        };
        self.transcripts
            .lock()
            .unwrap()
            .insert(identity.conversation.as_str().to_owned(), String::new());
        Ok(identity)
    }

    async fn read(
        &self,
        _caller: &SessionId,
        request: ConversationRead,
    ) -> Result<ConversationDigest, ConversationError> {
        let transcripts = self.transcripts.lock().unwrap();
        let transcript = transcripts
            .get(request.conversation.as_str())
            .ok_or_else(|| ConversationError::UnknownConversation(request.conversation.clone()))?;
        let bound = request.max_digest_bytes as usize;
        let truncated = transcript.len() > bound;
        let distillate = if truncated {
            transcript[..bound].to_owned()
        } else {
            transcript.clone()
        };
        Ok(ConversationDigest::new(
            request.conversation,
            distillate,
            truncated,
            transcript.lines().count() as u32,
        ))
    }

    async fn send(
        &self,
        caller: &SessionId,
        request: ConversationSend,
    ) -> Result<ConversationAcceptance, ConversationError> {
        let mut accepted = self.accepted.lock().unwrap();
        if accepted.contains_key(request.idempotency_key.as_str()) {
            return Ok(ConversationAcceptance::new(
                request.conversation,
                ConversationDelivery::AlreadyAccepted,
                request.idempotency_key,
            ));
        }
        let delivery = if self
            .running
            .lock()
            .unwrap()
            .contains(request.conversation.as_str())
        {
            ConversationDelivery::Queued
        } else {
            ConversationDelivery::StartedTurn
        };
        accepted.insert(request.idempotency_key.as_str().to_owned(), delivery);
        self.delivered
            .lock()
            .unwrap()
            .push((caller.as_str().to_owned(), request.clone()));
        Ok(ConversationAcceptance::new(
            request.conversation,
            delivery,
            request.idempotency_key,
        ))
    }
}

/// One Host that answers about something other than what it was asked, and one
/// that ignores the declared digest bound.
struct DishonestHost {
    answer: ConversationId,
    digest_bytes: usize,
    key: IdempotencyKey,
}

#[async_trait::async_trait]
impl ConversationDelegate for DishonestHost {
    async fn create(
        &self,
        _caller: &SessionId,
        _request: ConversationCreate,
    ) -> Result<ConversationIdentity, ConversationError> {
        Ok(ConversationIdentity {
            conversation: self.answer.clone(),
            project: project("Somewhere Else"),
            label: None,
        })
    }

    async fn read(
        &self,
        _caller: &SessionId,
        _request: ConversationRead,
    ) -> Result<ConversationDigest, ConversationError> {
        Ok(ConversationDigest::new(
            self.answer.clone(),
            "x".repeat(self.digest_bytes),
            false,
            1,
        ))
    }

    async fn send(
        &self,
        _caller: &SessionId,
        request: ConversationSend,
    ) -> Result<ConversationAcceptance, ConversationError> {
        Ok(ConversationAcceptance::new(
            request.conversation,
            ConversationDelivery::StartedTurn,
            self.key.clone(),
        ))
    }
}

async fn conversation_tool_names(runtime: &Runtime, id: &SessionId) -> Vec<String> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(tools) = runtime
                .list_mcp_tools(id, Some(CONVERSATION_TOOL_SERVER))
                .await
                && !tools.is_empty()
            {
                return tools.into_iter().map(|tool| tool.name).collect();
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_default()
}

fn feature_enabled(runtime: &Runtime, namespace: &str) -> Option<bool> {
    runtime
        .capabilities()
        .features
        .into_iter()
        .find(|feature| feature.namespace == namespace)
        .map(|feature| feature.enabled)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_three_tools_exist_on_a_session_exactly_when_a_delegate_is_installed() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let host = Arc::new(PeerHost::default());
    let (delegated, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .conversation_delegate(host.clone())
        .start()
        .await
        .expect("desktop runtime with a conversation delegate");
    assert_eq!(
        feature_enabled(&delegated, "sdk:conversation-tools"),
        Some(true)
    );
    let session = delegated
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session");
    let mut names = conversation_tool_names(&delegated, &session).await;
    names.sort();
    assert_eq!(
        names,
        vec![
            "conversation_create".to_owned(),
            "conversation_read".to_owned(),
            "conversation_send".to_owned()
        ],
        "the declared surface reaches the agent as three named tools"
    );
    assert_eq!(
        conversation_tool_descriptors()
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>(),
        vec![
            "conversation_create",
            "conversation_read",
            "conversation_send"
        ]
    );
    delegated.shutdown().await.expect("shutdown");

    let bare_root = TempDir::new().expect("bare root");
    let (bare, _) = Runtime::builder(runtime_config(&bare_root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .start()
        .await
        .expect("desktop runtime without a delegate");
    assert_eq!(
        feature_enabled(&bare, "sdk:conversation-tools"),
        Some(false)
    );
    let bare_session = bare
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session");
    assert!(
        bare.list_mcp_tools(&bare_session, Some(CONVERSATION_TOOL_SERVER))
            .await
            .expect("tool catalog")
            .is_empty(),
        "no delegate means no conversation tools anywhere on the Session"
    );
    assert!(matches!(
        bare.create_conversation(&bare_session, ConversationCreate::new(project("Product")))
            .await,
        Err(Error::InvalidConfig(_))
    ));
    bare.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delegate_answers_round_trip_through_both_the_typed_and_the_agent_facing_route() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let host = Arc::new(PeerHost::with_transcript(
        "thread-peer",
        "one\ntwo\nthree\n",
    ));
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .conversation_delegate(host.clone())
        .start()
        .await
        .expect("desktop runtime");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session");

    let created = runtime
        .create_conversation(
            &session,
            ConversationCreate::new(project("Desktop Product"))
                .title(ConversationTitle::new("Release plan").unwrap()),
        )
        .await
        .expect("the host created a peer conversation");
    assert_eq!(created.conversation.as_str(), "thread-1");
    assert_eq!(created.project, project("Desktop Product"));
    assert_eq!(
        created.label.as_ref().map(ConversationLabel::as_str),
        Some("Release plan")
    );
    assert_eq!(
        host.created.lock().unwrap()[0].0,
        session.as_str(),
        "the delegate is told which Session ran the tool"
    );

    let digest = runtime
        .read_conversation(
            &session,
            ConversationRead::new(conversation("thread-peer"))
                .focus(DigestFocus::new("what is blocked").unwrap()),
        )
        .await
        .expect("digest");
    assert_eq!(digest.distillate, "one\ntwo\nthree\n");
    assert!(!digest.truncated);
    assert_eq!(digest.covered_messages, 3);

    let acceptance = runtime
        .send_to_conversation(
            &session,
            ConversationSend::new(
                conversation("thread-peer"),
                PeerMessage::new("please review the plan").unwrap(),
                key("delivery-1"),
            )
            .label(ConversationLabel::new("Planner").unwrap()),
        )
        .await
        .expect("acceptance");
    assert_eq!(acceptance.delivery, ConversationDelivery::StartedTurn);
    assert!(acceptance.is_first_delivery());

    host.mark_running("thread-peer");
    let queued = runtime
        .invoke_conversation_tool(
            &session,
            ConversationToolCall::parse(
                "conversation_send",
                serde_json::json!({
                    "conversation": "thread-peer",
                    "message": "and the follow-up",
                    "idempotencyKey": "delivery-2"
                }),
            )
            .expect("a parsed send call"),
        )
        .await
        .expect("acceptance");
    match queued {
        ConversationToolOutcome::Accepted(acceptance) => {
            assert_eq!(acceptance.delivery, ConversationDelivery::Queued)
        }
        other => panic!("expected an acceptance, got {other:?}"),
    }

    let result = runtime
        .call_mcp_tool(
            &session,
            CONVERSATION_TOOL_SERVER,
            "conversation_read",
            serde_json::json!({ "conversation": "thread-peer", "maxDigestBytes": 4 }),
        )
        .await
        .expect("the agent-facing route answers");
    assert_eq!(result.is_error, Some(false));
    let structured = result.structured_content.expect("structured digest");
    assert_eq!(structured["distillate"], serde_json::json!("one\n"));
    assert_eq!(
        structured["truncated"],
        serde_json::json!(true),
        "truncation is a recorded fact, not a silently shortened answer"
    );

    let refused = runtime
        .call_mcp_tool(
            &session,
            CONVERSATION_TOOL_SERVER,
            "conversation_read",
            serde_json::json!({ "conversation": "thread-unknown" }),
        )
        .await
        .expect("a refusal is a tool result, not a transport failure");
    assert_eq!(refused.is_error, Some(true));

    assert_eq!(host.deliveries().len(), 2);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redelivering_one_send_key_is_the_same_delivery_and_never_a_second_one() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let host = Arc::new(PeerHost::with_transcript("thread-peer", "history"));
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .conversation_delegate(host.clone())
        .start()
        .await
        .expect("desktop runtime");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session");

    let request = ConversationSend::new(
        conversation("thread-peer"),
        PeerMessage::new("status?").unwrap(),
        key("delivery-retry"),
    );
    let first = runtime
        .send_to_conversation(&session, request.clone())
        .await
        .expect("first delivery");
    let retry = runtime
        .send_to_conversation(&session, request.clone())
        .await
        .expect("a retry is answered, not refused");
    assert!(first.is_first_delivery());
    assert_eq!(retry.delivery, ConversationDelivery::AlreadyAccepted);
    assert!(!retry.is_first_delivery());
    assert_eq!(retry.idempotency_key, key("delivery-retry"));
    assert_eq!(
        host.deliveries().len(),
        1,
        "one delivery key is one delivery, whatever the transport did"
    );

    let other = runtime
        .send_to_conversation(
            &session,
            ConversationSend::new(
                conversation("thread-peer"),
                PeerMessage::new("status?").unwrap(),
                key("delivery-other"),
            ),
        )
        .await
        .expect("a distinct key is a distinct delivery");
    assert!(other.is_first_delivery());
    assert_eq!(host.deliveries().len(), 2);
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_contract_bounds_arguments_and_refuses_an_answer_about_something_else() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    assert!(PeerMessage::new("x".repeat(MAX_PEER_MESSAGE_BYTES)).is_ok());
    assert!(matches!(
        PeerMessage::new("x".repeat(MAX_PEER_MESSAGE_BYTES + 1)),
        Err(ConversationError::InvalidMessage)
    ));
    assert!(matches!(
        ConversationId::new("not a conversation"),
        Err(ConversationError::InvalidConversationId(_))
    ));

    let host = Arc::new(DishonestHost {
        answer: conversation("thread-elsewhere"),
        digest_bytes: 64,
        key: key("some-other-key"),
    });
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .conversation_delegate(host)
        .start()
        .await
        .expect("desktop runtime");
    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session");

    assert!(
        matches!(
            runtime
                .create_conversation(&session, ConversationCreate::new(project("Product")))
                .await,
            Err(Error::Operation(_))
        ),
        "a create answered about another project fails closed"
    );
    assert!(
        matches!(
            runtime
                .read_conversation(
                    &session,
                    ConversationRead::new(conversation("thread-asked"))
                )
                .await,
            Err(Error::Operation(_))
        ),
        "a digest about another conversation fails closed"
    );
    assert!(
        matches!(
            runtime
                .read_conversation(
                    &session,
                    ConversationRead::new(conversation("thread-elsewhere")).max_digest_bytes(8)
                )
                .await,
            Err(Error::Operation(_))
        ),
        "a digest that outruns the declared bound fails closed"
    );
    assert!(
        matches!(
            runtime
                .read_conversation(
                    &session,
                    ConversationRead::new(conversation("thread-elsewhere"))
                        .max_digest_bytes(MAX_CONVERSATION_DIGEST_BYTES as u32 + 1)
                )
                .await,
            Err(Error::InvalidConfig(_))
        ),
        "a read cannot declare a bound above the contract ceiling"
    );
    assert!(
        matches!(
            runtime
                .send_to_conversation(
                    &session,
                    ConversationSend::new(
                        conversation("thread-elsewhere"),
                        PeerMessage::new("hello").unwrap(),
                        key("delivery-1"),
                    ),
                )
                .await,
            Err(Error::Operation(_))
        ),
        "an acceptance for another delivery key fails closed"
    );
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conversation_delegate_requires_the_desktop_profile_and_an_unclaimed_mount() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");

    assert!(
        matches!(
            Runtime::builder(runtime_config(&root, sampling.url()))
                .conversation_delegate(Arc::new(PeerHost::default()))
                .start()
                .await
                .map(drop),
            Err(Error::InvalidConfig(_))
        ),
        "host-delegate-backed tools are not routable in the Restricted profile"
    );

    assert!(
        matches!(
            Runtime::builder(runtime_config(&root, sampling.url()))
                .profile(RuntimeProfile::Desktop)
                .conversation_delegate(Arc::new(PeerHost::default()))
                .mcp_servers([sophon_sdk::McpServerConfig::Http {
                    name: CONVERSATION_TOOL_SERVER.into(),
                    url: "http://127.0.0.1:1/mcp".into(),
                    headers: BTreeMap::new(),
                }])
                .start()
                .await
                .map(drop),
            Err(Error::InvalidConfig(_))
        ),
        "the conversation mount name cannot be taken by another server"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_sourced_turn_records_its_origin_durably_and_an_unknown_source_fails_closed() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start().await.expect("mock provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let config = runtime_config(&root, sampling.url());

    let (runtime, _) = Runtime::start(config.clone())
        .await
        .expect("runtime starts");
    let session = runtime
        .create_session(session_config(workspace.clone()))
        .await
        .expect("session");
    runtime
        .prompt(&session, "turn-user", "a person typed this")
        .await
        .expect("user turn");
    let peer = InputSource::Peer {
        conversation: conversation("thread-planner"),
        label: Some(ConversationLabel::new("Release plan").unwrap()),
    };
    runtime
        .prompt_from(
            &session,
            "turn-peer",
            "another conversation sent this",
            peer.clone(),
        )
        .await
        .expect("peer turn");

    let live = runtime.session_ledger(&session).await.expect("ledger");
    assert_eq!(live.entries[0].source, InputSource::User);
    assert_eq!(live.entries[1].source, peer);
    runtime.shutdown().await.expect("shutdown");

    let (restarted, _) = Runtime::start(config).await.expect("runtime restarts");
    restarted
        .resume_session(session.clone(), session_config(workspace))
        .await
        .expect("resume");
    let recovered = restarted.session_ledger(&session).await.expect("ledger");
    assert_eq!(recovered, live, "the source survives a Runtime restart");
    assert_eq!(
        recovered.entries[1]
            .source
            .peer_conversation()
            .map(ConversationId::as_str),
        Some("thread-planner")
    );
    restarted.shutdown().await.expect("shutdown");

    let legacy: SessionLedger = serde_json::from_value(serde_json::json!({
        "entries": [{
            "turn_id": "turn-0",
            "prompt_digest": "sha256:abc",
            "runtime_prompt_index": 0,
            "state": { "state": "pending" }
        }]
    }))
    .expect("a ledger written before the input-source contract still decodes");
    assert_eq!(legacy.entries[0].source, InputSource::User);

    serde_json::from_value::<SessionLedger>(serde_json::json!({
        "entries": [{
            "turn_id": "turn-0",
            "prompt_digest": "sha256:abc",
            "runtime_prompt_index": 0,
            "state": { "state": "pending" },
            "source": { "kind": "scheduled_wake" }
        }]
    }))
    .expect_err("a source this build does not know is never attributed to the user");
}
