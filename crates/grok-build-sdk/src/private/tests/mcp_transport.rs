use super::super::*;

struct InProcessProbe {
    called: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl crate::InProcessMcpHandler for InProcessProbe {
    async fn handle(
        &self,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, crate::HostError> {
        self.called.store(true, Ordering::Release);
        Ok(match message.get("id") {
            Some(id) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":{}}),
            None => serde_json::Value::Null,
        })
    }
}

#[tokio::test]
async fn direct_mcp_invoker_rejects_unregistered_stale_and_nonresident_bindings() {
    let called = Arc::new(AtomicBool::new(false));
    let handler: Arc<dyn crate::InProcessMcpHandler> = Arc::new(InProcessProbe {
        called: called.clone(),
    });
    let bindings = Arc::new(McpBindingRegistry::default());
    let invoker = DirectMcpInvoker {
        runtime_instance_id: 1,
        handlers: HashMap::from([("registration".into(), ("server".into(), handler))]),
        bindings: bindings.clone(),
        host_services: Default::default(),
    };

    let unregistered = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
        &invoker,
        "unknown-session",
        u64::MAX,
        "registration",
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect_err("unregistered bindings fail closed");
    assert!(unregistered.contains("stale or not resident"));

    let old_binding = bindings.bind("closed-session");
    let new_binding = bindings.bind("closed-session");
    let stale = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
        &invoker,
        "closed-session",
        old_binding,
        "registration",
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect_err("replacement invalidates the old actor binding");
    assert!(stale.contains("stale or not resident"));

    xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
        &invoker,
        "closed-session",
        new_binding,
        "registration",
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("the replacement binding is active");

    bindings.revoke_session("closed-session");
    let error = xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
        &invoker,
        "closed-session",
        new_binding,
        "registration",
        serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/list"}),
        std::time::Duration::from_secs(1),
    )
    .await
    .expect_err("nonresident sessions fail closed");
    assert!(error.contains("stale or not resident"));
    assert!(called.load(Ordering::Acquire));
}

struct OutboundProbe {
    peer: std::sync::Mutex<Option<crate::InProcessMcpPeer>>,
}

#[async_trait::async_trait]
impl crate::InProcessMcpHandler for OutboundProbe {
    async fn handle(
        &self,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, crate::HostError> {
        Ok(match message.get("id") {
            Some(id) => serde_json::json!({"jsonrpc":"2.0","id":id,"result":{}}),
            None => serde_json::Value::Null,
        })
    }

    async fn connected(
        &self,
        _context: &crate::InProcessMcpContext,
        peer: crate::InProcessMcpPeer,
    ) -> Result<(), crate::HostError> {
        *self.peer.lock().unwrap() = Some(peer);
        Ok(())
    }
}

#[tokio::test]
async fn in_process_outbound_peer_is_bounded_and_generation_bound() {
    let probe = Arc::new(OutboundProbe {
        peer: std::sync::Mutex::new(None),
    });
    let bindings = Arc::new(McpBindingRegistry::default());
    let invoker = DirectMcpInvoker {
        runtime_instance_id: 1,
        handlers: HashMap::from([(
            "registration".into(),
            (
                "server".into(),
                probe.clone() as Arc<dyn crate::InProcessMcpHandler>,
            ),
        )]),
        bindings: bindings.clone(),
        host_services: Default::default(),
    };
    let first_binding = bindings.bind("session");
    let (first_tx, mut first_rx) = tokio::sync::mpsc::channel(1);
    xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::connect(
        &invoker,
        "session",
        first_binding,
        "registration",
        first_tx,
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("first peer connects");
    let first_peer = probe.peer.lock().unwrap().clone().expect("first peer");
    first_peer
        .notify("notifications/tools/list_changed", serde_json::json!({}))
        .await
        .expect("active peer pushes");
    assert_eq!(
        first_rx.recv().await.unwrap()["method"],
        "notifications/tools/list_changed"
    );

    let second_binding = bindings.bind("session");
    assert!(
        first_peer
            .notify("notifications/tools/list_changed", serde_json::json!({}))
            .await
            .is_err(),
        "replacement invalidates a retained old peer"
    );
    assert!(first_rx.try_recv().is_err());

    let (second_tx, mut second_rx) = tokio::sync::mpsc::channel(1);
    xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::connect(
        &invoker,
        "session",
        second_binding,
        "registration",
        second_tx,
        std::time::Duration::from_secs(1),
    )
    .await
    .expect("replacement peer connects");
    let second_peer = probe.peer.lock().unwrap().clone().expect("second peer");
    second_peer
        .notify(
            "notifications/resources/list_changed",
            serde_json::json!({}),
        )
        .await
        .expect("replacement peer pushes");
    assert_eq!(
        second_rx.recv().await.unwrap()["method"],
        "notifications/resources/list_changed"
    );
}

#[tokio::test]
async fn in_process_outbound_backpressure_rechecks_generation_before_delivery() {
    let bindings = Arc::new(McpBindingRegistry::default());
    let binding_id = bindings.bind("session");
    let (outbound, mut receiver) = tokio::sync::mpsc::channel(1);
    let peer = crate::InProcessMcpPeer::new(Arc::new(DirectMcpOutbound {
        session_id: "session".into(),
        binding_id,
        bindings: bindings.clone(),
        outbound,
    }));
    peer.notify(
        "notifications/tools/list_changed",
        serde_json::json!({"n":1}),
    )
    .await
    .expect("first notification fills the bounded channel");

    let blocked = {
        let peer = peer.clone();
        tokio::spawn(async move {
            peer.notify(
                "notifications/tools/list_changed",
                serde_json::json!({"n":2}),
            )
            .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        !blocked.is_finished(),
        "a full outbound channel must apply backpressure"
    );

    bindings.revoke_session("session");
    assert_eq!(receiver.recv().await.unwrap()["params"]["n"], 1);
    blocked
        .await
        .expect("blocked sender task")
        .expect_err("a sender released after unload must fail closed");
    assert!(receiver.try_recv().is_err());
}

struct BlockingInProcessProbe {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::InProcessMcpHandler for BlockingInProcessProbe {
    async fn handle(
        &self,
        message: serde_json::Value,
    ) -> Result<serde_json::Value, crate::HostError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(serde_json::json!({
            "jsonrpc":"2.0",
            "id":message["id"],
            "result":{}
        }))
    }
}

#[tokio::test]
async fn direct_mcp_invoker_rejects_a_result_after_its_binding_is_revoked() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let bindings = Arc::new(McpBindingRegistry::default());
    let invoker = Arc::new(DirectMcpInvoker {
        runtime_instance_id: 1,
        handlers: HashMap::from([(
            "registration".into(),
            (
                "server".into(),
                Arc::new(BlockingInProcessProbe {
                    started: started.clone(),
                    release: release.clone(),
                }) as Arc<dyn crate::InProcessMcpHandler>,
            ),
        )]),
        bindings: bindings.clone(),
        host_services: Default::default(),
    });
    let binding_id = bindings.bind("session");
    let invocation = {
        let invoker = invoker.clone();
        tokio::spawn(async move {
            xai_grok_mcp::acp_transport::EmbeddedMcpInvoker::invoke(
                invoker.as_ref(),
                "session",
                binding_id,
                "registration",
                serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
                std::time::Duration::from_secs(2),
            )
            .await
        })
    };
    started.notified().await;
    bindings.revoke_session("session");
    release.notify_one();
    let error = invocation
        .await
        .expect("invocation task")
        .expect_err("a revoked binding cannot accept a late result");
    assert!(error.contains("stale or not resident"));
}
