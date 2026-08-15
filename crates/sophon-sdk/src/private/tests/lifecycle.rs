use super::super::*;

fn test_capabilities() -> RuntimeCapabilities {
    RuntimeCapabilities {
        profile: crate::RuntimeProfile::Restricted,
        host: crate::HostCapabilities::default(),
        features: Vec::new(),
    }
}

fn lifecycle_test_worker(
    mut commands: mpsc::UnboundedReceiver<Command>,
    shutdown_observed: std::sync::mpsc::Sender<()>,
    release_join: std::sync::mpsc::Receiver<()>,
    joined: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        match commands.blocking_recv().expect("lifecycle command") {
            Command::Shutdown(reply) => {
                let _ = reply.send(Ok(()));
                shutdown_observed.send(()).expect("shutdown observed");
            }
            _ => panic!("expected shutdown command"),
        }
        release_join.recv().expect("release worker join");
        joined.store(true, Ordering::Release);
    })
}

#[test]
fn worker_panic_before_readiness_reports_startup_failure_and_is_joined() {
    let (commands, _command_rx) = mpsc::unbounded_channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (completion_tx, mut completion) = watch::channel(None);
    let lifecycle = spawn_worker_lifecycle(commands, startup_tx, completion_tx, move |events| {
        Ok(std::thread::spawn(move || {
            let _startup = StartupReporter::new(events);
            panic!("panic before readiness");
        }))
    })
    .expect("lifecycle thread starts");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("embedding runtime")
        .block_on(async {
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                startup_rx,
            )
            .await
            .expect("startup reports worker panic")
            .expect("startup sender")
            .expect_err("startup fails");
            assert!(matches!(error, Error::Operation(message) if message == "runtime worker exited before readiness"));
            let error = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                wait_for_completion(&mut completion),
            )
            .await
            .expect("lifecycle reaches terminal completion")
            .expect_err("panic is retained");
            assert!(matches!(error, Error::Operation(message) if message == "runtime worker panicked"));
        });
    drop(lifecycle);
}

#[test]
fn executor_teardown_cancels_pending_startup_and_owned_worker_joins() {
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (completion_tx, mut completion) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let joined = Arc::new(AtomicBool::new(false));
    let worker_joined = joined.clone();
    let lifecycle = spawn_worker_lifecycle(commands, startup_tx, completion_tx, move |_events| {
        Ok(lifecycle_test_worker(
            command_rx,
            shutdown_tx,
            release_rx,
            worker_joined,
        ))
    })
    .expect("lifecycle thread starts");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("embedding runtime");
    let (polled_tx, polled_rx) = std::sync::mpsc::channel();
    runtime.block_on(async move {
        tokio::spawn(async move {
            let _lifecycle = lifecycle;
            polled_tx.send(()).expect("startup waiter polled");
            let _ = startup_rx.await;
        });
        tokio::task::yield_now().await;
    });
    polled_rx.recv().expect("startup is pending");

    drop(runtime);
    shutdown_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("executor teardown requests shutdown");
    release_tx.send(()).expect("release worker join");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("completion runtime")
        .block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                wait_for_completion(&mut completion),
            )
            .await
            .expect("worker reaches terminal completion")
            .expect("worker joins cleanly");
        });
    assert!(joined.load(Ordering::Acquire));
}

#[tokio::test]
async fn startup_readiness_racing_waiter_cancellation_retains_worker_ownership() {
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (completion_tx, mut completion) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let joined = Arc::new(AtomicBool::new(false));
    let worker_joined = joined.clone();
    let lifecycle = spawn_worker_lifecycle(commands, startup_tx, completion_tx, move |events| {
        let worker = lifecycle_test_worker(command_rx, shutdown_tx, release_rx, worker_joined);
        events
            .send(LifecycleEvent::Ready(Ok(test_capabilities())))
            .expect("worker becomes ready");
        Ok(worker)
    })
    .expect("lifecycle thread starts");
    startup_rx
        .await
        .expect("startup result")
        .expect("startup ready");
    drop(lifecycle);
    tokio::task::spawn_blocking(move || {
        shutdown_rx
            .recv()
            .expect("dropped startup owner shuts down");
        release_tx.send(()).expect("release join");
    })
    .await
    .expect("observer joins");
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        wait_for_completion(&mut completion),
    )
    .await
    .expect("worker reaches terminal completion")
    .expect("worker joins cleanly");
    assert!(joined.load(Ordering::Acquire));
}

#[test]
fn executor_teardown_during_shutdown_does_not_detach_pending_worker_join() {
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (completion_tx, mut completion) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let joined = Arc::new(AtomicBool::new(false));
    let worker_joined = joined.clone();
    let lifecycle = spawn_worker_lifecycle(commands, startup_tx, completion_tx, move |events| {
        let worker = lifecycle_test_worker(command_rx, shutdown_tx, release_rx, worker_joined);
        events
            .send(LifecycleEvent::Ready(Ok(test_capabilities())))
            .expect("worker becomes ready");
        Ok(worker)
    })
    .expect("lifecycle thread starts");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("embedding runtime");
    runtime
        .block_on(startup_rx)
        .expect("startup result")
        .expect("startup ready");
    let (polled_tx, polled_rx) = std::sync::mpsc::channel();
    let mut waiter_completion = completion.clone();
    runtime.block_on(async move {
        tokio::spawn(async move {
            lifecycle.shutdown();
            polled_tx.send(()).expect("shutdown waiter polled");
            let _ = wait_for_completion(&mut waiter_completion).await;
        });
        tokio::task::yield_now().await;
    });
    polled_rx.recv().expect("shutdown waiter is pending");
    shutdown_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("worker acknowledges shutdown");

    drop(runtime);
    release_tx.send(()).expect("release worker join");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("completion runtime")
        .block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                wait_for_completion(&mut completion),
            )
            .await
            .expect("owned lifecycle reaches terminal completion")
            .expect("worker joins cleanly");
        });
    assert!(joined.load(Ordering::Acquire));
}
