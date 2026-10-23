# Appearance and accessibility

The interface provides light, dark and system theme choices. Search, connection,
transfer and completion animations communicate activity; reduced-motion preferences
are respected where implemented. Animations do not replace state labels or error
messages.

Discovery cards can be selected with the keyboard. The chat panel supports keyboard
movement, minimizing and position reset. Narrow-window layouts adapt controls and
lists, while expandable sections keep long transfer histories manageable.

## Interaction expectations

- Connection-only controls are shown before pairing; sharing tools appear after
  successful establishment of the session.
- File names and peer messages are rendered safely as text.
- State updates preserve actionable controls rather than replacing a button under
  an active click.
- Rejection notices can be acknowledged without accumulating permanent cards.
- Size controls support numeric entry as well as a slider.

Automated browser checks cover themes, viewport layouts, chat movement, safe text,
queue actions and reduced motion. These checks are not a formal accessibility
conformance certification. Before deployment, manually test keyboard-only use,
zoom, screen-reader announcements, contrast and native dialogs on target devices.

## Interaction acceptance rules

| Area | Required manual observation |
| --- | --- |
| Keyboard | Every essential action is reachable and focus remains visible |
| Status | Text explains state; color and animation are not the only signal |
| Resize | Chat and controls remain within the viewport after layout settles |
| Reduced motion | Essential work remains understandable without animation |
| Theme | Text, borders, disabled and error states remain distinguishable |
| Long lists | Scrolling/collapsing preserves access to actions |

## Verify

```powershell
python -m pytest tests/test_frontend_ui.py tests/test_chat_browser.py tests/test_rules_ui.py tests/test_queue_browser.py -q
```

Browser checks cover responsive layouts, theme choices, safe content, keyboard chat
movement and viewport bounds. A resize assertion must wait for both horizontal and
vertical layout constraints, not just one edge. These checks are not a screen-reader
audit or native WebView2 conformance test. Browser-under-load metrics are documented
in [Performance](../03-Validate-and-operate/PERFORMANCE.md), separately from visual correctness.
