use crate::auth::AuthPolicy;
use crate::controller::SessionController;
use crate::proto::media_control_server::MediaControlServer;
use crate::stream::MediaStreamService;
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
    serve_authenticated_until(controller, AuthPolicy::open(), listener, shutdown).await
}

pub async fn serve_authenticated_until(
    controller: Arc<SessionController>,
    auth: AuthPolicy,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let draining = controller.drain_handle();
    let control = MediaControlServer::with_interceptor(Arc::clone(&controller), auth.clone());
    let stream = MediaStreamService::new(Arc::clone(&controller), auth).into_service();
    let telcompat = TelCompat::new(controller).into_service();

    Server::builder()
        .add_service(control)
        .add_service(stream)
        .add_service(telcompat)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            draining.send_replace(true);
        })
        .await
}
