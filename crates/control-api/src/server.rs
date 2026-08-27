use crate::auth::AuthPolicy;
use crate::controller::SessionController;
use crate::proto;
use crate::proto::media_control_server::{MediaControl, MediaControlServer};
use crate::stream::MediaStreamService;
use crate::telcompat::TelCompat;
use session_core::reserved_metadata_refusal;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct WireFacing(Arc<SessionController>);

impl WireFacing {
    pub fn around(controller: Arc<SessionController>) -> WireFacing {
        WireFacing(controller)
    }
}

fn refuse_reserved_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> Result<(), Status> {
    match reserved_metadata_refusal(metadata) {
        Some(reason) => Err(Status::invalid_argument(reason)),
        None => Ok(()),
    }
}

#[tonic::async_trait]
impl MediaControl for WireFacing {
    type WatchEventsStream = <SessionController as MediaControl>::WatchEventsStream;

    async fn create_session(
        &self,
        request: Request<proto::CreateSessionRequest>,
    ) -> Result<Response<proto::Session>, Status> {
        self.0.create_session(request).await
    }

    async fn destroy_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.0.destroy_session(request).await
    }

    async fn describe_session(
        &self,
        request: Request<proto::SessionRef>,
    ) -> Result<Response<proto::Session>, Status> {
        self.0.describe_session(request).await
    }

    async fn attach(
        &self,
        request: Request<proto::AttachRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        refuse_reserved_metadata(&request.get_ref().metadata)?;
        self.0.attach(request).await
    }

    async fn detach(
        &self,
        request: Request<proto::AttachmentRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.0.detach(request).await
    }

    async fn update_attachment(
        &self,
        request: Request<proto::UpdateAttachmentRequest>,
    ) -> Result<Response<proto::Attachment>, Status> {
        refuse_reserved_metadata(&request.get_ref().metadata)?;
        self.0.update_attachment(request).await
    }

    async fn send_to_attachment(
        &self,
        request: Request<proto::SendToAttachmentRequest>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.0.send_to_attachment(request).await
    }

    async fn start_playback(
        &self,
        request: Request<proto::StartPlaybackRequest>,
    ) -> Result<Response<proto::Playback>, Status> {
        self.0.start_playback(request).await
    }

    async fn stop_playback(
        &self,
        request: Request<proto::PlaybackRef>,
    ) -> Result<Response<proto::Ack>, Status> {
        self.0.stop_playback(request).await
    }

    async fn watch_events(
        &self,
        request: Request<proto::WatchRequest>,
    ) -> Result<Response<Self::WatchEventsStream>, Status> {
        self.0.watch_events(request).await
    }
}

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
    let control = MediaControlServer::with_interceptor(
        WireFacing::around(Arc::clone(&controller)),
        auth.clone(),
    );
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
