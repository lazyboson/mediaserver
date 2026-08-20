use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

pub const AUTHORIZATION_HEADER: &str = "authorization";
pub const BEARER_PREFIX: &str = "Bearer ";

#[derive(Clone)]
pub struct AuthPolicy {
    secret: Option<Arc<str>>,
}

impl AuthPolicy {
    pub fn open() -> AuthPolicy {
        AuthPolicy { secret: None }
    }

    pub fn shared_secret(secret: impl Into<String>) -> AuthPolicy {
        AuthPolicy {
            secret: Some(Arc::from(secret.into())),
        }
    }

    pub fn requires_authentication(&self) -> bool {
        self.secret.is_some()
    }

    pub fn check_bearer(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let Some(secret) = &self.secret else {
            return Ok(());
        };
        let presented = metadata
            .get(AUTHORIZATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix(BEARER_PREFIX))
            .ok_or_else(|| {
                Status::unauthenticated("this control plane requires an authorization bearer token")
            })?;
        if constant_time_eq(presented.as_bytes(), secret.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("the bearer token is not valid"))
        }
    }

    pub fn check_hello_token(&self, presented: &str) -> Result<(), Status> {
        let Some(secret) = &self.secret else {
            return Ok(());
        };
        if constant_time_eq(presented.as_bytes(), secret.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated(
                "the consumer hello carried no valid token",
            ))
        }
    }
}

impl Interceptor for AuthPolicy {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        self.check_bearer(request.metadata())?;
        Ok(request)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (a, b) in left.iter().zip(right) {
        difference |= a ^ b;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_bearer(token: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_HEADER,
            format!("{BEARER_PREFIX}{token}").parse().unwrap(),
        );
        metadata
    }

    #[test]
    fn an_open_policy_admits_everyone() {
        let policy = AuthPolicy::open();
        assert!(!policy.requires_authentication());
        assert!(policy.check_bearer(&MetadataMap::new()).is_ok());
        assert!(policy.check_hello_token("").is_ok());
    }

    #[test]
    fn a_secret_policy_rejects_missing_and_wrong_credentials() {
        let policy = AuthPolicy::shared_secret("s3cret");
        assert!(policy.requires_authentication());

        let missing = policy.check_bearer(&MetadataMap::new()).unwrap_err();
        assert_eq!(missing.code(), tonic::Code::Unauthenticated);

        let wrong = policy.check_bearer(&with_bearer("nope")).unwrap_err();
        assert_eq!(wrong.code(), tonic::Code::Unauthenticated);

        assert!(policy.check_bearer(&with_bearer("s3cret")).is_ok());

        assert!(policy.check_hello_token("s3cret").is_ok());
        let hello = policy.check_hello_token("nope").unwrap_err();
        assert_eq!(hello.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn a_token_without_the_bearer_prefix_is_not_accepted() {
        let policy = AuthPolicy::shared_secret("s3cret");
        let mut metadata = MetadataMap::new();
        metadata.insert(AUTHORIZATION_HEADER, "s3cret".parse().unwrap());
        assert!(policy.check_bearer(&metadata).is_err());
    }
}
