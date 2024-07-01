use hmac::Hmac;
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use crate::memdx::auth_mechanism::AuthMechanism;
use crate::memdx::client::Result;
use crate::memdx::dispatcher::Dispatcher;
use crate::memdx::error::Error;
use crate::memdx::op_auth_saslplain::OpSASLPlainEncoder;
use crate::memdx::pendingop::StandardPendingOp;
use crate::memdx::request::{SASLAuthRequest, SASLStepRequest};
use crate::memdx::response::SASLStepResponse;
use crate::scram;

pub trait OpSASLScramEncoder: OpSASLPlainEncoder {
    async fn sasl_step<D>(
        &self,
        dispatcher: &mut D,
        request: SASLStepRequest,
    ) -> Result<StandardPendingOp<SASLStepResponse>>
    where
        D: Dispatcher;
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SASLAuthScramOptions {
    pub username: String,
    pub password: String,
    // pub hash: SASLAuthScramHash,
}

impl SASLAuthScramOptions {
    pub fn new(username: String, password: String) -> Self {
        Self {
            username,
            password,
            // hash,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OpsSASLAuthScram {}

// TODO: this is ugly, but I can't work out how to be generic over the digest algorithm.
impl OpsSASLAuthScram {
    pub async fn sasl_auth_scram_512<E, D>(
        &self,
        encoder: &E,
        dispatcher: &mut D,
        opts: SASLAuthScramOptions,
    ) -> Result<()>
    where
        E: OpSASLScramEncoder,
        D: Dispatcher,
    {
        let mut client =
            scram::Client::<Hmac<Sha512>, Sha512>::new(opts.username, opts.password, None);

        // Perform the initial SASL step
        let payload = client.step1()?;

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha512,
        };

        let mut op = encoder.sasl_auth(dispatcher, req).await?;

        let resp = op.recv().await?;

        if !resp.needs_more_steps {
            return Ok(());
        }

        let payload = client.step2(resp.payload.as_slice())?;

        let req = SASLStepRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha512,
        };

        let mut op = encoder.sasl_step(dispatcher, req).await?;

        let resp = op.recv().await?;

        if resp.needs_more_steps {
            return Err(Error::Protocol(
                "Server did not accept auth when the client expected".to_string(),
            ));
        }

        Ok(())
    }

    pub async fn sasl_auth_scram_256<E, D>(
        &self,
        encoder: &E,
        dispatcher: &mut D,
        opts: SASLAuthScramOptions,
    ) -> Result<()>
    where
        E: OpSASLScramEncoder,
        D: Dispatcher,
    {
        let mut client =
            scram::Client::<Hmac<Sha256>, Sha256>::new(opts.username, opts.password, None);

        // Perform the initial SASL step
        let payload = client.step1()?;

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha256,
        };

        let mut op = encoder.sasl_auth(dispatcher, req).await?;

        let resp = op.recv().await?;

        if !resp.needs_more_steps {
            return Ok(());
        }

        let payload = client.step2(resp.payload.as_slice())?;

        let req = SASLStepRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha256,
        };

        let mut op = encoder.sasl_step(dispatcher, req).await?;

        let resp = op.recv().await?;

        if resp.needs_more_steps {
            return Err(Error::Protocol(
                "Server did not accept auth when the client expected".to_string(),
            ));
        }

        Ok(())
    }

    pub async fn sasl_auth_scram_1<E, D>(
        &self,
        encoder: &E,
        dispatcher: &mut D,
        opts: SASLAuthScramOptions,
    ) -> Result<()>
    where
        E: OpSASLScramEncoder,
        D: Dispatcher,
    {
        let mut client = scram::Client::<Hmac<Sha1>, Sha1>::new(opts.username, opts.password, None);

        // Perform the initial SASL step
        let payload = client.step1()?;

        let req = SASLAuthRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha1,
        };

        let mut op = encoder.sasl_auth(dispatcher, req).await?;

        let resp = op.recv().await?;

        if !resp.needs_more_steps {
            return Ok(());
        }

        let payload = client.step2(resp.payload.as_slice())?;

        let req = SASLStepRequest {
            payload,
            auth_mechanism: AuthMechanism::ScramSha1,
        };

        let mut op = encoder.sasl_step(dispatcher, req).await?;

        let resp = op.recv().await?;

        if resp.needs_more_steps {
            return Err(Error::Protocol(
                "Server did not accept auth when the client expected".to_string(),
            ));
        }

        Ok(())
    }
}
