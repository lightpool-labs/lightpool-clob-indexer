// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::SinkExt;
use lightpool_sdk::lightpool_types::SignedTransaction;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::error::{AppError, AppResult};

const MEMPOOL_SEND_QUEUE_CAPACITY: usize = 1024;

struct SubmitRequest {
    tx_bytes: Bytes,
    respond_to: oneshot::Sender<AppResult<()>>,
}

#[derive(Clone, Debug)]
pub struct MempoolClient {
    sender: mpsc::Sender<SubmitRequest>,
}

impl MempoolClient {
    pub fn new(addr: &str) -> AppResult<Self> {
        let addr = addr
            .parse::<SocketAddr>()
            .map_err(|error| AppError::Internal(format!("invalid mempool address: {error}")))?;

        let (sender, receiver) = mpsc::channel(MEMPOOL_SEND_QUEUE_CAPACITY);
        tokio::spawn(run_mempool_sender(addr, receiver));

        Ok(Self { sender })
    }

    pub async fn submit_transaction(&self, tx: &SignedTransaction) -> AppResult<()> {
        let tx_bytes = bincode::serialize(tx)
            .map_err(|error| AppError::Internal(format!("serialize transaction: {error}")))?;

        let (respond_to, response_rx) = oneshot::channel();
        self.sender
            .send(SubmitRequest {
                tx_bytes: Bytes::from(tx_bytes),
                respond_to,
            })
            .await
            .map_err(|_| AppError::ServiceUnavailable("mempool client unavailable".into()))?;

        response_rx
            .await
            .map_err(|_| AppError::Internal("mempool send task dropped".into()))?
    }
}

async fn run_mempool_sender(addr: SocketAddr, mut receiver: mpsc::Receiver<SubmitRequest>) {
    let mut transport: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;

    while let Some(request) = receiver.recv().await {
        let result = send_on_connection(&addr, &mut transport, request.tx_bytes).await;
        if request.respond_to.send(result).is_err() {
            tracing::debug!("mempool submit caller dropped before send completed");
        }
    }

    tracing::info!(%addr, "mempool sender stopped");
}

async fn send_on_connection(
    addr: &SocketAddr,
    transport: &mut Option<Framed<TcpStream, LengthDelimitedCodec>>,
    tx_bytes: Bytes,
) -> AppResult<()> {
    for attempt in 0..2 {
        if transport.is_none() {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    tracing::info!(%addr, "mempool connection established");
                    *transport = Some(Framed::new(stream, LengthDelimitedCodec::new()));
                }
                Err(error) => {
                    return Err(AppError::Internal(format!(
                        "mempool connect failed: {error}"
                    )));
                }
            }
        }

        match transport.as_mut().unwrap().send(tx_bytes.clone()).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == 0 => {
                tracing::warn!(%addr, error = %error, "mempool send failed, reconnecting");
                *transport = None;
            }
            Err(error) => {
                return Err(AppError::Internal(format!("mempool send failed: {error}")));
            }
        }
    }

    Err(AppError::Internal("mempool send failed after reconnect".into()))
}
