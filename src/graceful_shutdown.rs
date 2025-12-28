use tokio::sync::watch;

pub fn channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

pub fn request(tx: &watch::Sender<bool>) {
    let _ = tx.send(true);
}
