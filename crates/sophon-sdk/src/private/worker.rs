use super::*;

pub(super) enum LifecycleEvent {
    Ready(Result<RuntimeCapabilities, Error>),
    Shutdown,
}

pub(super) struct LifecycleOwner {
    events: std::sync::mpsc::Sender<LifecycleEvent>,
}

impl LifecycleOwner {
    pub(super) fn shutdown(&self) {
        let _ = self.events.send(LifecycleEvent::Shutdown);
    }
}

impl Drop for LifecycleOwner {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(super) struct StartupReporter {
    events: Option<std::sync::mpsc::Sender<LifecycleEvent>>,
}

impl StartupReporter {
    pub(super) fn new(events: std::sync::mpsc::Sender<LifecycleEvent>) -> Self {
        Self {
            events: Some(events),
        }
    }

    pub(super) fn report(mut self, result: Result<RuntimeCapabilities, Error>) {
        if let Some(events) = self.events.take() {
            let _ = events.send(LifecycleEvent::Ready(result));
        }
    }
}

impl Drop for StartupReporter {
    fn drop(&mut self) {
        if let Some(events) = self.events.take() {
            let _ = events.send(LifecycleEvent::Ready(Err(Error::Operation(
                "runtime worker exited before readiness".into(),
            ))));
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum LifecycleOutcome {
    Success,
    Shutdown,
    Operation(String),
}

impl LifecycleOutcome {
    fn from_result(result: Result<(), Error>) -> Self {
        match result {
            Ok(()) => Self::Success,
            Err(Error::Shutdown) => Self::Shutdown,
            Err(Error::Operation(message)) => Self::Operation(message),
            Err(error) => Self::Operation(error.to_string()),
        }
    }

    fn into_result(self) -> Result<(), Error> {
        match self {
            Self::Success => Ok(()),
            Self::Shutdown => Err(Error::Shutdown),
            Self::Operation(message) => Err(Error::Operation(message)),
        }
    }
}

fn join_worker(join: std::thread::JoinHandle<()>) -> Result<(), Error> {
    join.join()
        .map_err(|_| Error::Operation("runtime worker panicked".into()))
}

fn stop_worker(
    commands: &mpsc::UnboundedSender<Command>,
    join: std::thread::JoinHandle<()>,
    request_shutdown: bool,
) -> LifecycleOutcome {
    let shutdown_result = if request_shutdown {
        let (tx, rx) = oneshot::channel();
        if commands.send(Command::Shutdown(tx)).is_err() {
            Err(Error::Shutdown)
        } else {
            rx.blocking_recv()
                .map_err(|_| Error::Shutdown)
                .and_then(|result| result)
        }
    } else {
        Ok(())
    };
    LifecycleOutcome::from_result(shutdown_result.and(join_worker(join)))
}

fn own_worker_lifecycle(
    commands: mpsc::UnboundedSender<Command>,
    join: std::thread::JoinHandle<()>,
    startup: oneshot::Sender<Result<RuntimeCapabilities, Error>>,
    events: std::sync::mpsc::Receiver<LifecycleEvent>,
    completion: watch::Sender<Option<LifecycleOutcome>>,
) {
    let ready = match events.recv() {
        Ok(LifecycleEvent::Ready(result)) => Some(result),
        Ok(LifecycleEvent::Shutdown) | Err(_) => None,
    };
    let outcome = match ready {
        Some(Ok(capabilities)) => {
            if startup.send(Ok(capabilities)).is_err() {
                stop_worker(&commands, join, true)
            } else {
                // LifecycleOwner sends shutdown when startup is canceled after
                // readiness or when the last Runtime owner is dropped.
                let _ = events.recv();
                stop_worker(&commands, join, true)
            }
        }
        Some(Err(error)) => {
            let _ = startup.send(Err(error));
            stop_worker(&commands, join, false)
        }
        None => {
            // Startup was canceled while the worker still owned initialization.
            // Queue shutdown now; Core consumes it immediately if startup wins.
            stop_worker(&commands, join, true)
        }
    };
    completion.send_replace(Some(outcome));
}

pub(super) fn spawn_worker_lifecycle<F>(
    commands: mpsc::UnboundedSender<Command>,
    startup: oneshot::Sender<Result<RuntimeCapabilities, Error>>,
    completion: watch::Sender<Option<LifecycleOutcome>>,
    spawn_worker: F,
) -> Result<Arc<LifecycleOwner>, Error>
where
    F: FnOnce(
            std::sync::mpsc::Sender<LifecycleEvent>,
        ) -> Result<std::thread::JoinHandle<()>, Error>
        + Send
        + 'static,
{
    let (events, event_rx) = std::sync::mpsc::channel();
    let owner = Arc::new(LifecycleOwner {
        events: events.clone(),
    });
    std::thread::Builder::new()
        .name("sophon-sdk-lifecycle".into())
        .spawn(move || match spawn_worker(events) {
            Ok(join) => own_worker_lifecycle(commands, join, startup, event_rx, completion),
            Err(error) => {
                let _ = startup.send(Err(error));
            }
        })
        .map_err(op)?;
    Ok(owner)
}

pub(super) async fn wait_for_completion(
    completion: &mut watch::Receiver<Option<LifecycleOutcome>>,
) -> Result<(), Error> {
    loop {
        if let Some(outcome) = completion.borrow().clone() {
            return outcome.into_result();
        }
        completion.changed().await.map_err(|_| Error::Shutdown)?;
    }
}
