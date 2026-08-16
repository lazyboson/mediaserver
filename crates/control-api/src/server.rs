use crate::controller::SessionController;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub async fn serve_on(
    controller: SessionController,
    listener: TcpListener,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(controller.into_service())
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
}

pub async fn serve_on_until(
    controller: SessionController,
    listener: TcpListener,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let draining = controller.drain_handle();
    Server::builder()
        .add_service(controller.into_service())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            let _ = draining.send(true);
        })
        .await
}
