use ethers::{
    abi::{Abi, Address},
    etherscan,
    types::Chain,
};
use eyre::Result;
use foundry_utils::RuntimeOrHandle;
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    stream::{Fuse, Stream, StreamExt},
    task::{Context, Poll},
    Future,
};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    pin::Pin,
    sync::mpsc::{channel as oneshot_channel, Sender as OneshotSender},
};

/// Trace identifiers figure out what ABIs and labels belong to all the addresses of the trace.
pub trait TraceIdentifier {
    /// Attempts to identify an address in one or more call traces.
    ///
    /// The tuple is of the format `(contract, label, abi)`, where `contract` is intended to be of
    /// the format `"<artifact>:<contract>"`, e.g. `"Foo.json:Foo"`.
    fn identify_address(
        &self,
        address: &Address,
        code: Option<&Vec<u8>>,
    ) -> (Option<String>, Option<String>, Option<&Abi>);
}

/// The local trace identifier keeps track of addresses that are instances of local contracts.
pub struct LocalTraceIdentifier {
    local_contracts: BTreeMap<Vec<u8>, (String, Abi)>,
}

impl LocalTraceIdentifier {
    pub fn new(known_contracts: &BTreeMap<String, (Abi, Vec<u8>)>) -> Self {
        Self {
            local_contracts: known_contracts
                .iter()
                .map(|(name, (abi, runtime_code))| {
                    (runtime_code.clone(), (name.clone(), abi.clone()))
                })
                .collect(),
        }
    }
}

impl TraceIdentifier for LocalTraceIdentifier {
    fn identify_address(
        &self,
        _: &Address,
        code: Option<&Vec<u8>>,
    ) -> (Option<String>, Option<String>, Option<&Abi>) {
        if let Some(code) = code {
            if let Some((_, (name, abi))) = self
                .local_contracts
                .iter()
                .find(|(known_code, _)| diff_score(known_code, code) < 0.1)
            {
                (Some(name.clone()), Some(name.clone()), Some(abi))
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        }
    }
}

/// Very simple fuzzy matching of contract bytecode.
///
/// Will fail for small contracts that are essentially all immutable variables.
fn diff_score(a: &[u8], b: &[u8]) -> f64 {
    let cutoff_len = usize::min(a.len(), b.len());
    if cutoff_len == 0 {
        return 1.0
    }

    let a = &a[..cutoff_len];
    let b = &b[..cutoff_len];
    let mut diff_chars = 0;
    for i in 0..cutoff_len {
        if a[i] != b[i] {
            diff_chars += 1;
        }
    }
    diff_chars as f64 / cutoff_len as f64
}

type EtherscanRequest = Pin<Box<dyn Future<Output = Option<(String, Abi)>> + Send>>;
type HandlerRequest = (Address, OneshotSender<Option<(String, Abi)>>);

struct EtherscanHandler {
    /// The Etherscan client
    client: etherscan::Client,
    /// Cached information about addresses
    cache: BTreeMap<Address, Option<(String, Abi)>>,
    /// Incoming requests
    incoming: Fuse<Receiver<HandlerRequest>>,
    /// Requests currently in progress
    pending_requests: BTreeMap<Address, EtherscanRequest>,
    /// Requests we haven't responded to yet
    waiting: BTreeMap<Address, Vec<OneshotSender<Option<(String, Abi)>>>>,
}

impl EtherscanHandler {
    pub fn new(client: etherscan::Client, incoming: Receiver<HandlerRequest>) -> Self {
        Self {
            client,
            incoming: incoming.fuse(),
            cache: BTreeMap::new(),
            pending_requests: BTreeMap::new(),
            waiting: BTreeMap::new(),
        }
    }
}

impl Future for EtherscanHandler {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pin = self.get_mut();

        // Pull incoming requests
        while let Poll::Ready(Some((addr, sender))) = Pin::new(&mut pin.incoming).poll_next(cx) {
            match pin.cache.entry(addr) {
                // If we have this address in the cache we just respond
                Entry::Occupied(entry) => {
                    let _ = sender.send(entry.get().clone());
                }
                // Otherwise we send a request to Etherscan
                Entry::Vacant(_) => {
                    let client = pin.client.clone();
                    pin.pending_requests.entry(addr).or_insert(Box::pin(async move {
                        client
                            .contract_source_code(addr)
                            .await
                            .ok()
                            .map(|mut metadata| {
                                if let Some(item) = metadata.items.pop() {
                                    Some((
                                        item.contract_name,
                                        serde_json::from_str(&item.abi).ok()?,
                                    ))
                                } else {
                                    None
                                }
                            })
                            .flatten()
                    }));
                    pin.waiting.entry(addr).or_default().push(sender);
                }
            }
        }

        // TODO: Poll pending requests
        // TODO: Check if handler is finished
        Poll::Pending
    }
}

pub struct EtherscanIdentifier {
    backend: Sender<HandlerRequest>,
}

impl EtherscanIdentifier {
    pub fn new(chain: Chain) -> Result<Self> {
        let (backend, backend_rx) = channel(1);
        let handler = EtherscanHandler::new(etherscan::Client::new_from_env(chain)?, backend_rx);

        let rt = RuntimeOrHandle::new();
        std::thread::spawn(move || match rt {
            RuntimeOrHandle::Runtime(runtime) => runtime.block_on(handler),
            RuntimeOrHandle::Handle(handle) => handle.block_on(handler),
        });

        Ok(Self { backend })
    }
}

impl TraceIdentifier for EtherscanIdentifier {
    fn identify_address(
        &self,
        addr: &Address,
        _: Option<&Vec<u8>>,
    ) -> (Option<String>, Option<String>, Option<&Abi>) {
        let (sender, rx) = oneshot_channel();
        if let Ok(_) = self.backend.clone().try_send((addr.clone(), sender)) {
            if let Ok(Some((label, abi))) = rx.recv() {
                return (None, Some(label), Some(&abi))
            }
        }
        (None, None, None)
    }
}
