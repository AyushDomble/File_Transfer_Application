# Host guide

The host offers a shared directory, approves connections and upload groups, and
displays activity for connected clients.

## Before receiving

Choose a writable destination with enough free space. Review extension rules and
maximum file size, then start hosting. Give the intended client your address,
certificate fingerprint through a trusted channel, and current PIN. Approve only
the connection you expect; possession of a discovered address does not identify
the sender.

## Receiving and monitoring

Each upload selection appears as one group approval request. Inspect the peer,
filenames, sizes, and ZIP choice before accepting. Approving a connection does not
automatically approve every subsequent upload group. Rejecting a group leaves the
connection usable.

Live sharing separates peers and shows their transfers. Expand a list to inspect
individual files; collapse it to save space. Transfer actions primarily live on
the client. Refresh updates the host view and clears eligible dismissed/terminal
display entries without deleting received files or cancelling live transfers.

The host accepts multiple clients, with five active files per peer by default.
Remote deletion is off by default. Enabling it grants approved clients permission
to delete eligible shared files; it is not ownership-based access control.

Use [Device chat](CHAT.md) to select and message one client privately.

## Host operating rules

Approve the expected device, not merely the first pending request. Verify the
selection separately from the connection. Do not add private identity/state files
to the shared directory's visible content. Keep deletion disabled unless all approved
clients are intentionally granted that privilege. Stop hosting before maintaining
private state or moving the shared directory.

## Verify

```powershell
python -m pytest tests/test_host.py tests/test_batches.py tests/test_refresh_browser.py -q
```

Host tests exercise pairing, peers, listing, permitted deletion, extension/size
policy, overwrite conflicts and retained offline peers. Group tests exercise
approval decisions. Browser refresh checks concern displayed state, not deletion
of received files or a restart of the host service.
