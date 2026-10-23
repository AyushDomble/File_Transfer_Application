# Client guide

The client connects to one host at a time. Before connection, the interface focuses
on nearby discovery and address selection. Pairing then requests identity/PIN
information and host approval. Transfer controls become available after connection.

## Send files

Choose whether to send the selection as one ZIP, then select files or drag them
onto the upload area. A selection is a group of up to 100 files. Local staging and
preparation happen before the host's group decision; disk space is required even
before peer transfer begins. Wait for approval and monitor individual queue rows.

## Receive files

Expand the host's shared-file list and request a download. The request becomes a
queued transfer. For browser-based saving, use the completed download's Save action;
native dialogs can select destinations where available. Downloads are verified
before completion is reported.

## Manage work

Pause retains a resumable checkpoint. Cancel stops the chosen transfer rather than
disconnecting other work. Retry is for failed transfers; reconnect/recover handles
a lost session. Disconnect closes the link but retains recoverable transfer state.

If a group is rejected, acknowledge the notification and select the files again
when ready. Do not repeatedly resend without resolving the host's reason for rejection.

## Client workflow rules

Do not modify a selected source while it may be retried. Keep the app running during
staging and preparation. Treat group rejection, connection loss and local staging
failure as different outcomes: reselect, reconnect, or retry the local preparation
as appropriate. Saving a completed browser download is separate from peer transfer.

## Verify

```powershell
python -m pytest tests/test_api_transfers.py tests/test_e2e_browser.py tests/test_queue_browser.py -q
```

API tests cover asynchronous upload/download resources and lifecycle actions.
The end-to-end browser case exercises an authenticated local application and real
TLS transfer, group approval, pause/resume and integrity. Its fixture supplies an
established peer connection; do not claim that it independently drives every native
PIN/fingerprint dialog. Queue DOM tests have their own mocked-route scope.
