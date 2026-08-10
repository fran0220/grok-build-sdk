use super::mcp_runtime::InProcessFixture;
use super::*;

fn write_skill(root: &std::path::Path, skill: &str, marker: &str) -> PathBuf {
    let directory = root.join(skill);
    std::fs::create_dir_all(&directory).expect("skill directory");
    std::fs::write(
        directory.join("SKILL.md"),
        format!(
            "---\nname: {skill}\ndescription: {marker} capability probe.\n---\n{marker}_BODY\n"
        ),
    )
    .expect("skill definition");
    root.to_path_buf()
}

/// One stdio MCP mount that advertises exactly one named tool.
fn stdio_mcp(root: &std::path::Path, mount: &str, tool: &str) -> McpServerConfig {
    let script = root.join(format!("{mount}-{tool}.sh"));
    std::fs::write(
        &script,
        r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
*'"method":"server/discover"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{"tools":{}},"ttlMs":0,"cacheScope":"private","_meta":{"io.modelcontextprotocol/serverInfo":{"name":"capability-fixture","version":"1"}}}}\n' "$id"
  ;;
*'"method":"tools/list"'*)
  printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"%s","description":"probe","inputSchema":{"type":"object"}}]}}\n' "$id" "$MCP_TOOL"
  ;;
  esac
done
"#,
    )
    .expect("mcp script");
    McpServerConfig::Stdio {
        name: mount.into(),
        command: "/bin/sh".into(),
        args: vec![script.to_string_lossy().into_owned()],
        env: BTreeMap::from([("MCP_TOOL".into(), tool.to_owned())]),
    }
}

async fn command_names(runtime: &Runtime, id: &SessionId) -> Vec<String> {
    runtime
        .list_agent_commands(id)
        .await
        .expect("live command catalog")
        .into_iter()
        .map(|command| command.name)
        .collect()
}

async fn await_mcp_tool(runtime: &Runtime, id: &SessionId, tool: &str) -> bool {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(tools) = runtime.list_mcp_tools(id, None).await
                && tools.iter().any(|entry| entry.name == tool)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// (a) Two Sessions on one Runtime carry different layers, and every Turn sees
/// only its own contributions plus the unmasked general ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_sessions_carry_independent_capability_layers() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let general_root = write_skill(&root.path().join("general"), "generalprobe", "GENERAL");
    let alpha_root = write_skill(&root.path().join("alpha"), "alphaprobe", "ALPHA");
    let beta_root = write_skill(&root.path().join("beta"), "betaprobe", "BETA");

    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .general_capabilities(
            CapabilityLayer::new()
                .skill(SkillContribution::new("general-skills", general_root))
                .mcp_service(stdio_mcp(root.path(), "general-mcp", "general_probe")),
        )
        .start()
        .await
        .expect("desktop runtime");

    let alpha = runtime
        .create_session_with_capabilities(
            session_config(workspace.clone()),
            CapabilityLayer::new()
                .skill(SkillContribution::new("project-skills", alpha_root))
                .mcp_service(stdio_mcp(root.path(), "project-mcp", "alpha_probe")),
        )
        .await
        .expect("alpha session");
    let beta = runtime
        .create_session_with_capabilities(
            session_config(workspace),
            CapabilityLayer::new()
                .skill(SkillContribution::new("project-skills", beta_root))
                .mcp_service(stdio_mcp(root.path(), "project-mcp", "beta_probe")),
        )
        .await
        .expect("beta session");

    let alpha_commands = command_names(&runtime, &alpha).await;
    let beta_commands = command_names(&runtime, &beta).await;
    assert!(alpha_commands.iter().any(|name| name == "alphaprobe"));
    assert!(!alpha_commands.iter().any(|name| name == "betaprobe"));
    assert!(beta_commands.iter().any(|name| name == "betaprobe"));
    assert!(!beta_commands.iter().any(|name| name == "alphaprobe"));
    assert!(alpha_commands.iter().any(|name| name == "generalprobe"));
    assert!(beta_commands.iter().any(|name| name == "generalprobe"));

    assert!(await_mcp_tool(&runtime, &alpha, "alpha_probe").await);
    assert!(await_mcp_tool(&runtime, &beta, "beta_probe").await);
    let alpha_tools: Vec<String> = runtime
        .list_mcp_tools(&alpha, None)
        .await
        .expect("alpha tools")
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert!(!alpha_tools.iter().any(|name| name == "beta_probe"));

    let alpha_resolution = runtime
        .session_capabilities(&alpha)
        .await
        .expect("alpha resolution");
    assert_eq!(
        alpha_resolution.origin(CapabilityKind::Skill, "project-skills"),
        Some(CapabilityOrigin::Session)
    );
    assert_eq!(
        alpha_resolution.origin(CapabilityKind::Skill, "general-skills"),
        Some(CapabilityOrigin::General)
    );
    assert!(alpha_resolution.masked.is_empty());
    assert_eq!(
        alpha_resolution.names(CapabilityKind::McpService),
        vec!["general-mcp", "project-mcp"]
    );
    runtime.shutdown().await.expect("shutdown");
}

/// (b) A Session capability masks the same-named general one; removing it from
/// the Session layer restores the general contribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_capability_masks_and_unmasks_the_general_layer() {
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let general_root = write_skill(&root.path().join("general"), "oraclegeneral", "GENERAL");
    let project_root = write_skill(&root.path().join("project"), "oracleproject", "PROJECT");

    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .general_capabilities(
            CapabilityLayer::new().skill(SkillContribution::new("oracle", general_root)),
        )
        .start()
        .await
        .expect("desktop runtime");

    let session = runtime
        .create_session_with_capabilities(
            session_config(workspace),
            CapabilityLayer::new().skill(SkillContribution::new("oracle", project_root)),
        )
        .await
        .expect("session");

    let masked = runtime
        .session_capabilities(&session)
        .await
        .expect("masked resolution");
    assert_eq!(
        masked.origin(CapabilityKind::Skill, "oracle"),
        Some(CapabilityOrigin::Session)
    );
    assert!(masked.is_masked(CapabilityKind::Skill, "oracle"));
    let commands = command_names(&runtime, &session).await;
    assert!(commands.iter().any(|name| name == "oracleproject"));
    assert!(!commands.iter().any(|name| name == "oraclegeneral"));

    let restored = runtime
        .set_session_capabilities(&session, CapabilityLayer::new())
        .await
        .expect("unmask");
    assert_eq!(
        restored.origin(CapabilityKind::Skill, "oracle"),
        Some(CapabilityOrigin::General)
    );
    assert!(restored.masked.is_empty());
    let restored_commands = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let commands = command_names(&runtime, &session).await;
            if commands.iter().any(|name| name == "oraclegeneral") {
                return commands;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("general skill is restored");
    assert!(!restored_commands.iter().any(|name| name == "oracleproject"));
    runtime.shutdown().await.expect("shutdown");
}

/// (c) A layer change between Turns applies to the next Turn, and (e) the
/// Runtime and Session incarnations are unchanged across it: no restart, no
/// second Runtime, no new Session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_change_between_turns_reaches_the_next_turn_without_a_restart() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let project_root = write_skill(&root.path().join("project"), "latecraft", "LATE");

    let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .in_process_mcp_servers([InProcessMcpServer::new(
            "sdk-fixture",
            "sdk-fixture-registration",
            Arc::new(InProcessFixture {
                contexts: contexts.clone(),
            }),
        )])
        .start()
        .await
        .expect("desktop runtime");
    let capabilities_before = runtime.capabilities();

    let session = runtime
        .create_session(session_config(workspace))
        .await
        .expect("session");
    runtime
        .prompt(&session, "turn-one", "first marker")
        .await
        .expect("first turn");
    runtime
        .call_mcp_tool(&session, "sdk-fixture", "echo", serde_json::json!({}))
        .await
        .expect("identity probe before the change");
    let before = contexts
        .lock()
        .expect("contexts")
        .last()
        .cloned()
        .expect("identity before");

    let commands_before = command_names(&runtime, &session).await;
    assert!(!commands_before.iter().any(|name| name == "latecraft"));

    let resolution = runtime
        .set_session_capabilities(
            &session,
            CapabilityLayer::new().skill(SkillContribution::new("project-skills", project_root)),
        )
        .await
        .expect("layer change");
    assert_eq!(
        resolution.origin(CapabilityKind::Skill, "project-skills"),
        Some(CapabilityOrigin::Session)
    );

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !command_names(&runtime, &session)
            .await
            .iter()
            .any(|name| name == "latecraft")
        {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the new skill reaches the session before the next turn");

    runtime
        .prompt(&session, "turn-two", "second marker")
        .await
        .expect("second turn");
    runtime
        .call_mcp_tool(&session, "sdk-fixture", "echo", serde_json::json!({}))
        .await
        .expect("identity probe after the change");
    let after = contexts
        .lock()
        .expect("contexts")
        .last()
        .cloned()
        .expect("identity after");

    assert_eq!(before.runtime_instance_id, after.runtime_instance_id);
    assert_eq!(before.session_instance_id, after.session_instance_id);
    assert_eq!(before.session_id, after.session_id);
    assert_eq!(capabilities_before, runtime.capabilities());

    let ledger = runtime
        .session_ledger(&session)
        .await
        .expect("session ledger");
    assert_eq!(ledger.entries.len(), 2);
    assert_eq!(ledger.entries[0].runtime_prompt_index, 0);
    assert_eq!(ledger.entries[1].runtime_prompt_index, 1);
    runtime.shutdown().await.expect("shutdown");
}

/// (d) Existing serialized configuration still decodes, and the layer itself
/// decodes from partial documents; invalid layers fail closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_layers_decode_additively_and_fail_closed() {
    let sampling = MockInferenceServer::start()
        .await
        .expect("sampling provider");
    let root = TempDir::new().expect("root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");

    let legacy: SessionConfig = serde_json::from_value(serde_json::json!({
        "cwd": workspace,
        "model": "test-model",
        "reasoning": null
    }))
    .expect("a session config serialized before capability layering still decodes");
    assert_eq!(legacy.model, "test-model");
    let legacy_layer: CapabilityLayer =
        serde_json::from_str("{}").expect("an absent layer decodes to the empty layer");
    assert!(legacy_layer.is_empty());

    let (runtime, _) = Runtime::builder(runtime_config(&root, sampling.url()))
        .profile(RuntimeProfile::Desktop)
        .start()
        .await
        .expect("desktop runtime");

    let duplicate = CapabilityLayer::new()
        .skill(SkillContribution::new("dup", root.path().join("one")))
        .skill(SkillContribution::new("dup", root.path().join("two")));
    assert!(matches!(
        runtime
            .create_session_with_capabilities(session_config(workspace.clone()), duplicate)
            .await,
        Err(Error::InvalidConfig(_))
    ));
    let unknown_model = CapabilityLayer::new()
        .agent_service(AgentServiceContribution::new("explore", "not-in-catalog"));
    assert!(matches!(
        runtime
            .create_session_with_capabilities(session_config(workspace.clone()), unknown_model)
            .await,
        Err(Error::InvalidConfig(_))
    ));
    runtime.shutdown().await.expect("shutdown");

    let restricted_root = TempDir::new().expect("restricted root");
    let (restricted, _) = Runtime::builder(runtime_config(&restricted_root, sampling.url()))
        .start()
        .await
        .expect("restricted runtime");
    assert!(
        matches!(
            restricted
                .create_session_with_capabilities(
                    session_config(workspace),
                    CapabilityLayer::new()
                        .skill(SkillContribution::new("any", restricted_root.path())),
                )
                .await,
            Err(Error::InvalidConfig(_))
        ),
        "capability layers require the Desktop profile"
    );
    restricted.shutdown().await.expect("restricted shutdown");
}
