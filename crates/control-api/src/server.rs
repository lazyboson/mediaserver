use crate::controller::SessionController;
use crate::telcompat::TelCompat;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub async fn serve_on(
    controller: SessionController,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    serve_on_until(controller, listener, std::future::pending()).await
}

pub async fn serve_on_until(
    controller: SessionController,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    serve_shared_until(Arc::new(controller), listener, shutdown).await
}

pub async fn serve_shared_until(
    controller: Arc<SessionController>,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let draining = controller.drain_handle();
    let telcompat = TelCompat::new(Arc::clone(&controller)).into_service();

    Server::builder()
        .add_service(SessionController::service_for(controller))
        .add_service(telcompat)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            let _ = draining.send(true);
        })
        .await
}
